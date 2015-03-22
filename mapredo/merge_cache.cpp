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
#include "base.h"
#include "sorter_buffer.h"

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
merge_cache::add (const uint16_t hash_index, const sorter_buffer& sorted)
{
    size_t size = sorted.buffer_size();

    if (_buffer_pos + size > _buffer_size)
    {
	for (auto& list: _buffer_lists)
	{
	    if (list.second.size())
	    {
		std::cerr << "Lager merger\n";
		file_merger merger (_reducer, _tmpdir, _index, 3,
				    std::list<std::string>(),
				    std::move(list.second));
		std::cerr << "Lagd merger\n";
	    }
	}
    }

    auto& list = _buffer_lists[hash_index];
    char* dest = _buffer.get() + _buffer_pos;

    auto end = sorted.lookup().cbegin() + sorted.lookup_used();
    
    for (auto iter = sorted.lookup().cbegin(); iter != end; iter++)
    {
	memcpy (dest, iter->keyvalue(), iter->size());
	dest += iter->size();
    }
    list.emplace_back (_buffer.get() + _buffer_pos, size);
    _buffer_pos += size;
}

void
merge_cache::append_cache_buffers (const uint16_t index, buffer_list& list)
{
    auto iter = _buffer_lists.find (index);

    if (iter != _buffer_lists.end())
    {
	std::cerr << "Append " << index << ": " << iter->second.size() << "\n";
	list.splice (list.end(), iter->second);
    }
}

void
merge_cache::append_tmpfiles (const uint16_t index,
			      std::list<std::string>& list)
{
    auto iter = _tmpfiles.find (index);

    if (iter != _tmpfiles.end()) list.splice (list.end(), iter->second);
}
