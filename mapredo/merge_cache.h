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

#ifndef _HEXTREME_MAPREDO_MERGE_CACHE_H
#define _HEXTREME_MAPREDO_MERGE_CACHE_H

#include <memory>
#include <unordered_map>
#include <forward_list>

#include "ram_reader.h"

namespace mapredo
{
}

/**
 * Keeps sorted data in memory for use in merge sorting.  This allows
 * us to efficiently utilize more memory without increasing the size
 * of the sorter buffers.  Performance will suffer if the sorter
 * buffers are too large to fit in L2 cache.
 */
template <class T>
class merge_cache
{
public:
    /*
     * @param file_prefix prefix for created files
     * @param total_size total number of bytes in the cache
     * @param buckets number of buffers to spread the total size into
     */
    merge_cache (const std::string& file_prefix,
		 
		 const size_t cache_size,
		 const uint16_t buckets) :
	_file_prefix(file_prefix),
	_buffer(new char[cache_size]),
	_buffer_size (cache_size) {}

    /**
     * Add more data to the cache.  If the buffer is full, data will be merged
     * and flushed to disk.
     * @param hash_index which bucket to copy the data into
     * @param buffer data to copy
     * @param size bytes of data
     */
    void add (const uint16_t hash_index,
	      const char* buffer,
	      const size_t size) {
	if (_buffer_size > 0)
	{
	    
	}
    }

    /**
     * Take the list of RAM readers for a specific hash index.  This
     * empties the list in this object.
     */
    std::forward_list<ram_reader<T>> grab_readers (const uint16_t hash_index) {
	
    }

private:
    std::string _file_prefix;
    std::unique_ptr<char[]> _buffer;
    const size_t _buffer_size;
    size_t _buffer_pos = 0;
    std::unordered_map<int, std::forward_list<ram_reader<T>>> _buckets;
    std::vector<std::string> _tmpfiles;
};

#endif
