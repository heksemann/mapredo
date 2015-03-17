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

#ifndef _HEXTREME_MAPREDO_SORTER_H
#define _HEXTREME_MAPREDO_SORTER_H

#include <string>
#include <vector>
#include <list>
#include <future>

#include "sorter_buffer.h"
#include "base.h"

class compression;

/**
 * Used to sort lines on key
 */
class sorter
{
public:
    /**
     * @param tmpdir where to save temporary files
     * @param index number used in filenames
     * @param max_bytes_buffer number of bytes in each buffer to sort
     * @param type type of key to sort on
     * @param reverse sort in descending order if true
     */
    sorter (const std::string& tmpdir,
	    const uint16_t hash_index,
	    const uint16_t worker_index,
	    const size_t max_bytes_buffer,
	    const mapredo::base::keytype type,
	    const bool reverse);
    sorter (sorter&& other) noexcept;
    ~sorter();

    /**
     * Add a key and value to the buffer, flush if necessary
     * @param keyvalue tab separated key and value
     * @param keylen length of key
     * @param size total length of keyvalue
     */
    void add (const char* keyvalue, const size_t keylen, const size_t size);

    /** Reserve space in buffer */
    char* reserve (const size_t bytes);

    /** Include reserved data in buffer */
    void add_reserved (const size_t keylen, const size_t size);

    /**
     * Take the list of temporary files. This empties the list in this object
     */
    std::list<std::string> grab_tmpfiles();

    /**
     * Sort and flush current buffer to disk
     */
    void flush();

    /** @returns hash index number as given to the constructor */
    uint16_t hash_index() const {return _index;}

    sorter (const sorter&) = delete;

private:
    void make_room (const size_t size);

    sorter_buffer _buffer;
    const std::string _tmpdir;
    const size_t _bytes_per_buffer;
    uint16_t _index;
    std::string _file_prefix;
    int _tmpfile_id = 0;
    std::list<std::string> _tmpfiles;
    std::unique_ptr<compression> _compressor;
    bool _merging_off = false;
    bool _flushing_in_progress = false;
    std::future<std::string> _flush_result;
    const mapredo::base::keytype _type;
    const bool _reverse;
};

#endif
