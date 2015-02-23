/* -*- C++ -*-
 * mapredo
 * Copyright (C) 2015 Kjell Irgens <hextremist@gmail.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 */

#ifndef _HEXTREME_MAPREDO_FILE_MERGER_H
#define _HEXTREME_MAPREDO_FILE_MERGER_H

#include <string>
#include <map>
#include <list>

#include "collector.h"
#include "tmpfile_reader.h"
#include "settings.h"
#include "valuelist.h"
#include "mapreducer.h"
#include "tmpfile_collector.h"

namespace mapredo
{
    class base;
}

/** Does merging of temporary data from the sorting phase */
class file_merger : public mapredo::collector
{
public:
    file_merger (mapredo::base& reducer,
		 std::list<std::string>&& tmpfiles,
		 const std::string& tmpdir,
		 const size_t index,
		 const size_t buffer_per_file,
		 const size_t max_open_files);
    virtual ~file_merger();

    /**
     * Go through all sorted temporary files and generate a single sorted
     * stream.
     */
    void merge();

    /**
     * Merge to a single file and return the file name.
     */
    std::string merge_to_file();

    /**
     * Merge to at most max_open_files file and return the file names.
     */
    std::list<std::string> merge_to_files();

    /**
     * Function called by reducer to report output.
     */
    void collect (const char* line, const size_t length);

    /**
     * @returns a thread exception if it occured or nullptr otherwise
     */
    std::exception_ptr& exception_ptr() {return _texception;}

    file_merger (file_merger&& other);
    file_merger (const file_merger&) = delete;

private:
    enum merge_mode
    {
	TO_MAX_FILES,
	TO_SINGLE_FILE,
	TO_OUTPUT
    };
    
    void merge_max_files (const merge_mode mode);
    void compressed_sort();
    void regular_sort();
    template<typename T> void do_merge (const merge_mode mode);

    mapredo::base& _reducer;
    size_t _size_buffer;
    size_t _max_open_files;
    size_t _num_merged_files = 0;
    std::string _file_prefix;
    int _tmpfile_id = 0;
    std::list<std::string> _tmpfiles;
    std::unique_ptr<compression> _compressor;
    std::unique_ptr<char[]> _cinbuffer;
    std::unique_ptr<char[]> _coutbuffer;
    size_t _cinbufpos;
    size_t _coutbufpos;
    std::exception_ptr _texception = nullptr;
};

template <typename T> void
file_merger::do_merge (const merge_mode mode)
{
    typename data_reader<T>::queue queue;
    size_t files;

    if (mode == TO_MAX_FILES)
    {
	files = std::min (_tmpfiles.size() - _num_merged_files,
			  _max_open_files);
    }
    else files = std::min (_tmpfiles.size(), _max_open_files);
    
    for (size_t i = 0; i < files; i++)
    {
	const std::string& filename = _tmpfiles.front();
	auto* proc = new tmpfile_reader<T>
	    (filename, 0x100000, !settings::instance().keep_tmpfiles());
	const T* key = proc->next_key();

	if (key) queue.push(proc);
	else if (!settings::instance().keep_tmpfiles())
	{
	    remove (filename.c_str());
	}

	_tmpfiles.pop_front();
    }
    if (settings::instance().verbose())
    {
	std::ostringstream stream;
	stream << "Processing " << queue.size() << " tmpfiles, "
	       << _tmpfiles.size() << " left\n";
	std::cerr << stream.str();
    }

    if (queue.empty())
    {
	throw std::runtime_error ("Queue should not be empty here");
    }

    _num_merged_files++;

    if (_tmpfiles.empty() && mode == TO_OUTPUT) // last_merge, run reducer
    {
	mapredo::valuelist<T> list (queue);

	while (!queue.empty())
	{
	    T key (queue.top()->get_key_copy());
	    list.set_key (key);
	    static_cast<mapredo::mapreducer<T>&>(_reducer).reduce
		(key, list, *this);
	}
    }
    else if (_reducer.reducer_can_combine()
	     || (_tmpfiles.empty() && mode == TO_SINGLE_FILE))
    {
	tmpfile_collector collector (_file_prefix, _tmpfile_id);
	mapredo::valuelist<T> list (queue);

	while (!queue.empty())
	{
	    T key (queue.top()->get_key_copy());
	    list.set_key (key);
	    static_cast<mapredo::mapreducer<T>&>(_reducer).reduce
		(key, list, collector);
	}
	collector.flush();
	_tmpfiles.push_back (collector.filename());
    }
    else // no reduction
    {
	std::ofstream outfile;
	std::ostringstream filename;
	const bool compressed (settings::instance().compressed());

	filename << _file_prefix << _tmpfile_id++;
	if (compressed)
	{
	    filename << ".snappy";
	    _cinbuffer.reset (new char[0x10000]);
	    _coutbuffer.reset (new char[0x15000]);
	    _cinbufpos = 0;
	}
	outfile.open (filename.str(), std::ofstream::binary);
	if (!outfile)
	{
	    char err[80];
#ifdef _WIN32
	    strerror_s (err, sizeof(err), errno);
#endif	
	    throw std::invalid_argument
		("Unable to open " + filename.str() + " for writing: "
#ifndef _WIN32
		 + strerror_r (errno, err, sizeof(err))
#else
		 + err
#endif
		);
	}
	_tmpfiles.push_back (filename.str());

	auto* proc = queue.top();
	queue.pop();
	T key (proc->get_key_copy());
	const T* next_key;
	size_t length;

	for(;;)
	{
	    while ((next_key = proc->next_key())
		   && (*proc == key || queue.empty()))
	    {
		auto line = proc->get_next_line (length);
		if (compressed)
		{
		    if (_cinbufpos + length > 0x10000)
		    {
			_coutbufpos = 0x15000;
			_compressor->compress (_cinbuffer.get(),
					       _cinbufpos,
					       _coutbuffer.get(),
					       _coutbufpos);
			outfile.write (_coutbuffer.get(), _coutbufpos);
			_cinbufpos = 0;
		    }
		    memcpy (_cinbuffer.get() + _cinbufpos, line, length);
		    _cinbufpos += length;
		}
		else outfile.write (line, length);
	    }

	    if (!next_key) // file emptied
	    {
		delete proc;
		if (queue.empty()) break;
		proc = queue.top();
		queue.pop();
		key = proc->get_key_copy();
		continue;
	    }

	    auto* nproc = queue.top();
	    int cmp = nproc->compare (key);
    
	    if (cmp == 0)
	    {
		queue.pop();
		queue.push (proc);
		proc = nproc;
	    }
	    else
	    {
		if (cmp > 0)
		{
		    queue.pop();
		    queue.push (proc);
		    proc = nproc;
		}
		key = proc->get_key_copy();
	    }
	}

	if (compressed && _cinbufpos > 0)
	{
	    _coutbufpos = 0x15000;
	    _compressor->compress (_cinbuffer.get(),
				   _cinbufpos,
				   _coutbuffer.get(),
				   _coutbufpos);
	    outfile.write (_coutbuffer.get(), _coutbufpos);
	}

	outfile.close();
    }
}

#endif
