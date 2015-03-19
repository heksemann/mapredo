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
			  const std::string& tmpdir,
			  const size_t index,
			  const size_t max_open_files,
			  std::list<std::string>&& tmpfiles,
			  merge_cache::buffer_list&& cache_buffers) :
    _reducer (reducer),
    _max_open_files (max_open_files),
    _tmpfiles (tmpfiles),
    _cache_buffers (cache_buffers)
{
    std::ostringstream filename;

    filename << tmpdir << "/merge_" << std::this_thread::get_id()
	     << ".w" << index << '.';
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
    _max_open_files (other._max_open_files),
    _file_prefix (std::move(other._file_prefix)),
    _tmpfiles (std::move(other._tmpfiles)),
    _cache_buffers (std::move(other._cache_buffers))
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
file_merger::merge_to_file (prefered_output* alt_output)
{
    try
    {
	do
	{
	    merge_max_files (TO_SINGLE_FILE, alt_output);
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
file_merger::merge_max_files (const file_merger::merge_mode mode,
			      prefered_output* alt_output)
{
    switch (_reducer.type())
    {
    case mapredo::base::keytype::STRING:
	do_merge<char*> (mode, alt_output);
	break;

    case mapredo::base::keytype::DOUBLE:
	do_merge<double> (mode, alt_output);
	break;

    case mapredo::base::keytype::INT64:
	do_merge<int64_t> (mode, alt_output);
	break;

    case mapredo::base::keytype::UNKNOWN:
	throw std::runtime_error ("Program error, keytype not set"
				  " in mapredo::base");
    }
}

void
file_merger::collect (const char* line, const size_t length)
{
    if (_buffer_pos + length >= _buffer_size) flush();
    memcpy (_buffer + _buffer_pos, line, length);
    _buffer_pos += length;
    _buffer[_buffer_pos++] = '\n';
}

char*
file_merger::reserve (const size_t bytes)
{
    _reserved_bytes = bytes;
    if (_buffer_pos + bytes >= _buffer_size) flush();
    return (_buffer + _buffer_pos);
}

void
file_merger::collect_reserved (const size_t length)
{
    if (_reserved_bytes == 0)
    {
	throw std::runtime_error
	    ("No memory reserved via reserve() in"
	     " file_merger::collect_reserved()");
    }

    if (length == 0) _buffer_pos += _reserved_bytes;
    else _buffer_pos += length;
    _buffer[_buffer_pos++] = '\n';

    _reserved_bytes = 0;
}
