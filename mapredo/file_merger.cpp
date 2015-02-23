/*
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
	merge_max_files (TO_OUTPUT);
	if (_texception) return;
    }
}

std::string
file_merger::merge_to_file()
{
    try
    {
	do
	{
	    merge_max_files (TO_SINGLE_FILE);
	}
	while (_tmpfiles.size() > 1);

	return _tmpfiles.front();
    }
    catch (...)
    {
	_texception = std::current_exception();
	return ("");
    }
}

std::list<std::string>
file_merger::merge_to_files()
{
    try
    {
	while (_tmpfiles.size() > _num_merged_files
	       || _tmpfiles.size() > _max_open_files)
	{
	    if (_tmpfiles.size() == _num_merged_files)
	    {
		// We have to re-merge files because we still have too many
		_num_merged_files = 0;
	    }
	    merge_max_files (TO_MAX_FILES);
	}

	return _tmpfiles;
    }
    catch (...)
    {
	_texception = std::current_exception();
	return (std::list<std::string>());
    }
}

void
file_merger::merge_max_files (const file_merger::merge_mode mode)
{
    switch (_reducer.type())
    {
    case mapredo::base::keytype::STRING:
    {
	do_merge<char*> (mode);
	break;
    }
    case mapredo::base::keytype::DOUBLE:
    {
	do_merge<double> (mode);
	break;
    }
    case mapredo::base::keytype::INT64:
    {
	do_merge<int64_t> (mode);
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
    fwrite (line, length, 1, stdout);
    fwrite ("\n", 1, 1, stdout);
}
