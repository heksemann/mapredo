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
#include <list>

#include "rcollector.h"
#include "tmpfile_reader.h"
#include "settings.h"
#include "valuelist.h"
#include "mapreducer.h"
#include "tmpfile_collector.h"
#include "data_reader_queue.h"

namespace mapredo
{
    class base;
}

/**
 * Does merging of temporary data from different files in the sorting phase
 */
class file_merger : public mapredo::rcollector
{
public:
    file_merger (mapredo::base& reducer,
		 std::list<std::string>&& tmpfiles,
		 const std::string& tmpdir,
		 const size_t index,
		 const size_t max_open_files);
    virtual ~file_merger();

    /**
     * Go through all sorted temporary files and generate a single sorted
     * stream.
     */
    void merge();

    /**
     * Merge to a single file and return the file name.  This may also
     * output final data to an alternate sink.
     * @param alt_output if not nullptr, attempt to write to this first
     */
    std::string merge_to_file (prefered_output* alt_output);

    /**
     * Merge to at most max_open_files file and return the file names.
     */
    std::list<std::string> merge_to_files();

    /** Function called by reducer to report output. */
    void collect (const char* line, const size_t length);

    /** Reserve memory buffer for reducer */
    virtual char* reserve (const size_t bytes) final;

    /** Collect data from reserved memory */
    virtual void collect_reserved (const size_t length = 0) final;

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

    template <class T> class key_holder {
    public:
	template<class U = T,
		 typename std::enable_if<std::is_fundamental<U>::value>
                 ::type* = nullptr>
	U get_key (data_reader<T>& reader) {
	    auto key = reader.next_key();
	    if (key) return *key;
	    throw std::runtime_error
		("Attempted to key_handler::get_key() on an empty file");
	}

	template<class U = T,
                 typename std::enable_if<std::is_same<U,char*>::value,
                                         bool>::type* = nullptr>
	char* get_key (data_reader<T>& reader) {
	    auto key = reader.next_key();
	    if (key)
	    {
		_key_copy = *key;
		return const_cast<char*>(_key_copy.c_str());
	    }
	    throw std::runtime_error
		("Attempted to key_handler::get_key() on an empty file");
	}
    private:
	std::string _key_copy;
    };
    
    void merge_max_files (const merge_mode mode,
			  prefered_output* alt_output = nullptr);
    void compressed_sort();
    void regular_sort();
    void flush() {
	if (_buffer_pos > 0)
	{
	    fwrite (_buffer, _buffer_pos, 1, stdout);
	    _buffer_pos = 0;
	}
    }
    template<typename T> void do_merge (const merge_mode mode,
					prefered_output* alt_output,
					const bool reverse);

    mapredo::base& _reducer;
    static const size_t _buffer_size = 0x10000;
    size_t _max_open_files;
    size_t _num_merged_files = 0;
    std::string _file_prefix;
    int _tmpfile_id = 0;
    std::list<std::string> _tmpfiles;
    std::unique_ptr<compression> _compressor;
    char _buffer[_buffer_size];
    std::unique_ptr<char[]> _coutbuffer;
    size_t _buffer_pos;
    size_t _coutbufpos;
    size_t _reserved_bytes = 0;
    std::exception_ptr _texception = nullptr;
};

template <typename T> void
file_merger::do_merge (const merge_mode mode, prefered_output* alt_output,
		       const bool reverse)
{
    data_reader_queue<T> queue (reverse);
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
	    static_cast<mapredo::mapreducer<T>&>(_reducer).reduce
		(list.get_key(), list, *this);
	}
	flush();
    }
    else if (_reducer.reducer_can_combine()
	     || (_tmpfiles.empty() && mode == TO_SINGLE_FILE))
    {
	tmpfile_collector collector
	    (_file_prefix, _tmpfile_id,
	     (_tmpfiles.empty() && mode == TO_SINGLE_FILE)
	     ? alt_output
	     : nullptr);
	mapredo::valuelist<T> list (queue);

	while (!queue.empty())
	{
	    static_cast<mapredo::mapreducer<T>&>(_reducer).reduce
		(list.get_key(), list, collector);
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
	    _coutbuffer.reset (new char[0x15000]);
	    _buffer_pos = 0;
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

	const T* next_key;
	size_t length;
	key_holder<T> keyh;
	auto* proc = queue.top();
	T key (keyh.get_key(*proc));
	queue.pop();

	for(;;)
	{
	    while ((next_key = proc->next_key())
		   && (*proc == key || queue.empty()))
	    {
		auto line = proc->get_next_line (length);
		if (compressed)
		{
		    if (_buffer_pos + length > _buffer_size)
		    {
			_coutbufpos = 0x15000;
			_compressor->compress (_buffer,
					       _buffer_pos,
					       _coutbuffer.get(),
					       _coutbufpos);
			outfile.write (_coutbuffer.get(), _coutbufpos);
			_buffer_pos = 0;
		    }
		    memcpy (_buffer + _buffer_pos, line, length);
		    _buffer_pos += length;
		}
		else outfile.write (line, length);
	    }

	    if (!next_key) // file emptied
	    {
		delete proc;
		if (queue.empty()) break;
		proc = queue.top();
		key = keyh.get_key (*proc);
		queue.pop();
		continue;
	    }

	    auto* nproc = queue.top();

	    if (*nproc == key)
	    {
		queue.pop();
		queue.push (proc);
		proc = nproc;
	    }
	    else
	    {
		int cmp = nproc->compare (*next_key);
		if (cmp < 0)
		{
		    queue.pop();
		    queue.push (proc);
		    proc = nproc;
		}
		key = keyh.get_key (*proc);
	    }
	}

	if (compressed && _buffer_pos > 0)
	{
	    _coutbufpos = 0x15000;
	    _compressor->compress (_buffer,
				   _buffer_pos,
				   _coutbuffer.get(),
				   _coutbufpos);
	    outfile.write (_coutbuffer.get(), _coutbufpos);
	}

	outfile.close();
    }
}

#endif
