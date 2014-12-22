/*** -*- C++ -*- *********************************************************/

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
    void merge_max_files (const bool to_single_file = false);
    void compressed_sort();
    void regular_sort();
    template<typename T> void do_merge (const bool to_single_file);

    mapredo::base& _reducer;
    size_t _size_buffer;
    size_t _max_open_files;
    std::string _file_prefix;
    int _tmpfile_id = 0;
    std::list<std::string> _tmpfiles;
    std::unique_ptr<compression> _compressor;
    std::unique_ptr<char[]> _cinbuffer;
    std::unique_ptr<char[]> _coutbuffer;
    size_t _cinbufpos;
    size_t _coutbufpos;
    std::exception_ptr _texception;
};

template <typename T> void
file_merger::do_merge (const bool to_single_file)
{
    typedef std::pair<T, tmpfile_reader<T>*> tmpfile_entry;
    typedef std::multimap<T, tmpfile_reader<T>*> tmpfile_reader_queue;

    tmpfile_reader_queue queue;
    
    for (size_t i = _max_open_files; i > 0 && !_tmpfiles.empty(); i--)
    {
	std::string& filename = _tmpfiles.front();
	auto* proc = new tmpfile_reader<T> (filename, 0x100000);
	const T* key = proc->next_key();

	if (key)
	{
	    queue.insert (std::pair<T,tmpfile_reader<T>*>(*key, proc));
	}
	else if (!settings::instance().keep_tmpfiles())
	{
	    remove (filename.c_str());
	}

	_tmpfiles.pop_front();
    }
    if (settings::instance().verbose())
    {
	std::cerr << "Processing " << queue.size() << " tmpfiles, "
		  << _tmpfiles.size() << " left \n";
    }

    if (queue.empty()) return;

    if (_tmpfiles.empty() && !to_single_file) // last_merge, run reducer
    {
	if (settings::instance().verbose()) std::cerr << "Last merge\n";
	mapredo::valuelist<T> list (queue);
	const T* key = nullptr;

	while (!queue.empty()
	       && (key = (*queue.begin()).second->next_key()))
	{
	    static_cast<mapredo::mapreducer<T>&>(_reducer).reduce
		(*key, list, *this);
	}
    }
    else if (_reducer.reducer_can_combine())
    {
	tmpfile_collector collector (_file_prefix, _tmpfile_id);
	mapredo::valuelist<T> list (queue);
	const T* key = nullptr;

	while (!queue.empty()
	       && (key = (*queue.begin()).second->next_key()))
	{
	    static_cast<mapredo::mapreducer<T>&>(_reducer).reduce
		(*key, list, collector);
	}
	collector.flush();
	_tmpfiles.push_back (collector.filename());
    }
    else
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
	outfile.open (filename.str());
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

	while (queue.size() > 1)
	{
	    auto* proc = (*queue.begin()).second;
	    T key (*proc->next_key());
	    const T* next_key;
	    size_t length;

	    for(;;)
	    {
		while ((next_key = proc->next_key()) && key == *next_key)
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

		if (!next_key)
		{
		    std::string filename (proc->filename());
		    delete proc;
		    if (!settings::instance().keep_tmpfiles())
		    {
			remove (filename.c_str());
		    }
		    queue.erase (queue.begin());
		    if (queue.empty()) break;
		    proc = (*queue.begin()).second;
		    continue;
		}

		if (queue.size() > 1)
		{
		    typename tmpfile_reader_queue::const_iterator niter;

		    niter = queue.begin();
		    niter++;

		    if (niter->first == key)
		    {
			queue.erase (queue.begin());
			queue.insert (tmpfile_entry(*next_key, proc));
			proc = (*queue.begin()).second;
		    }
		    else if (*next_key > niter->first)
		    {
			queue.erase (queue.begin());
			queue.insert (tmpfile_entry(*next_key, proc));
			key = niter->first;
			proc = (*queue.begin()).second;
		    }
		    else key = *next_key;
		}
		else key = *next_key;
	    }
	}

	outfile.close();
    }
}

#endif
