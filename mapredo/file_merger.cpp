
#include <sstream>
#include <stdexcept>
#include <thread>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <cstdio>
#include <memory>
#include <cerrno>

#include "file_merger.h"
#include "valuelist.h"

file_merger::file_merger (mapredo::base& reducer,
			  std::list<std::string>&& tmpfiles,
			  const std::string& tmpdir,
			  const size_t index,
			  const size_t buffer_per_file,
			  const size_t max_open_files) :
    _reducer (reducer),
    _size_buffer (buffer_per_file),
    _max_open_files (max_open_files),
    _tmpfiles (tmpfiles)
{
    std::ostringstream filename;

    filename << tmpdir << "/merge_" << std::this_thread::get_id()
	     << '.' << index << '.';
    _file_prefix = filename.str();

    if (max_open_files < 3)
    {
	throw std::runtime_error ("Can not operate on less than three files"
				  " per bucket");
    }

    if (settings::instance().compressed()) 
    {
        _compressor.reset (new compression());
    }
}

file_merger::file_merger (file_merger&& other) :
    _reducer (other._reducer),
    _size_buffer (other._size_buffer),
    _max_open_files (other._max_open_files),
    _file_prefix (std::move(other._file_prefix)),
    _tmpfiles (std::move(other._tmpfiles))
{}

file_merger::~file_merger()
{}

void
file_merger::merge()
{
    while (!_tmpfiles.empty())
    {
	merge_max_files();
	if (_texception) return;
    }
}

std::string
file_merger::merge_to_file()
{
    try
    {
	while (_tmpfiles.size() > 1)
	{
	    merge_max_files (true);
	}

	return _tmpfiles.front();
    }
    catch (...)
    {
	_texception = std::current_exception();
	return ("");
    }
}

void
file_merger::merge_max_files (const bool to_single_file)
{
    switch (_reducer.type())
    {
    case mapredo::base::keytype::STRING:
    {
	do_merge<std::string> (to_single_file);
	break;
    }
    case mapredo::base::keytype::DOUBLE:
    {
	do_merge<double> (to_single_file);
	break;
    }
    case mapredo::base::keytype::INT64:
    {
	do_merge<int64_t> (to_single_file);
	break;
    }
    case mapredo::base::keytype::UNKNOWN:
    {
	throw std::runtime_error ("Program error, keytype not set"
				  " in mapredo::base");
    }
    }
}

void
file_merger::collect (const char* line, const size_t length)
{
    std::cout << line << "\n";
}
