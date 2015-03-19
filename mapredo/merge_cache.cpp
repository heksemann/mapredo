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

#include <list>

#include "merge_cache.h"
#include "file_merger.h"

merge_cache::merge_cache (mapredo::base& reducer,
			  const std::string& tmpdir,
			  const std::string& file_prefix,
			  const size_t cache_size,
			  const size_t index) :
    _reducer(reducer),
    _tmpdir (tmpdir),
    _file_prefix(file_prefix),
    _buffer(new char[cache_size]),
    _buffer_size(cache_size),
    _index(index)
{}

void
merge_cache::add (const uint16_t hash_index,
		  const char* buffer,
		  const size_t size)
{
    auto list = _buffer_lists[hash_index];

    if (_buffer_pos + size <= _buffer_size)
    {
	list.emplace_back (buffer,size);
	_buffer_pos += size;
    }
    else
    {
	file_merger merger (_reducer, _tmpdir, _index, 3);
	for (auto& list: _buffer_lists)
	{
	    auto& flist = _tmpfiles[list.first];
	    flist.push (merger.merge_to_file (std::list<std::string>(),
					      list.second, nullptr);
	}
    }
}
