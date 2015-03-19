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

#include "base.h"

/**
 * Keeps sorted data in memory for use in merge sorting.  This allows
 * us to efficiently utilize more memory without increasing the size
 * of the sorter buffers.  Performance will suffer if the sorter
 * buffers are too large to fit in L2 cache.
 */
class merge_cache
{
public:
    using buffer_list = std::vector<std::pair<char*,uint32_t>>;
    
    /*
     * @param reducer map-reducer plugin object
     * @param file_prefix prefix for created files
     * @param total_size total number of bytes in the cache
     * @param index worker index
     */
    merge_cache (mapredo::base& reducer,
		 const std::string& tmpdir,
		 const std::string& file_prefix,
		 const size_t cache_size,
		 const size_t index);

    /**
     * Add more data to the cache.  If the buffer is full, data will be merged
     * and flushed to disk.
     * @param hash_index which bucket to copy the data into
     * @param buffer data to copy
     * @param size bytes of data
     */
    void add (const uint16_t hash_index,
	      const char* buffer,
	      const size_t size);

    /**
     * Steal the list of buffers for a hash index from this object.
     */
    buffer_list grab_buffers (const uint16_t hash_index);

    /**
     * Steal the list of temporary files for a hash index from this object.
     */
    std::list<std::string> grab_files (const uint16_t hash_index);

private:
    mapredo::base& _reducer;
    std::string _tmpdir;
    std::string _file_prefix;
    std::unique_ptr<char[]> _buffer;
    const size_t _buffer_size;
    const size_t _index;
    size_t _buffer_pos = 0;
    std::unordered_map<int, buffer_list> _buffer_lists;
    std::unordered_map<int, std::list<std::string>> _tmpfiles;
};

#endif
