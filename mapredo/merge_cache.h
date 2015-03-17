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

#include <unordered_map>

/**
 * Keeps sorted data in memory for use in merge sorting.  This allows
 * us to efficiently utilize more memory without increasing the size of
 * the sorter buffers.  The sorter buffer cannot be too large, due to
 * size of L3 cache (and random accessing of data in the sort function).
 */
class merge_cache
{
public:
    /*
     * @param total_size total number of bytes in the cache
     * @param buckets number of buffers to spread the total size into
     */
    merge_cache (const size_t total_size, const uint16_t buckets);

    /**
     * Add more data to the cache.  If the bucket is full, it will be
     * flushed to disk.
     * @param hash_index which bucket to copy the data into
     * @param buffer data to copy
     * @param size bytes of data
     */
    add (const uint16_t hash_index, const char* buffer, const size_t size);

    flush_all();

private:
    std::string _file_prefix;
    std::unordered_map<int> _buckets;
    std::vector<std::string> _tmpfiles;
};

#endif
