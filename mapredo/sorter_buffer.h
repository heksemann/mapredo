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

#ifndef _HEXTREME_MAPREDO_SORTER_BUFFER_H
#define _HEXTREME_MAPREDO_SORTER_BUFFER_H

#include <cstring>
#include <vector>
#include <iostream>

#include "lookup.h"

/**
 * Represents a buffer that will be sorted, with a lookup table
 */
class sorter_buffer
{
public:
    /**
     * @param bytes_available the number of bytes to use for the
     *                        buffer and the lookup table in total
     * @param ratio the ratio between the buffer and the lookup table
     */
    sorter_buffer (const size_t bytes_available, const double ratio);
    sorter_buffer (sorter_buffer&& other) noexcept;
    ~sorter_buffer();

    /** @return pointer to buffer */
    char*& buffer() {return _buffer;}
    /** @return buffer size in bytes */
    size_t buffer_size() const {return _buffer_size;}
    /** @return number of bytes used in buffer */
    size_t& buffer_used()  {return _buffer_used;}

    /** @return a reference to a vector of pointers to keyvalues */
    std::vector<struct lookup>& lookup() {return _lookup;}
    /** @return size of lookup vector in elements */
    size_t lookup_size() const {return _lookup_size;}
    /** @return number of elements used in lookup vector */
    size_t lookup_used() const {return _lookup_used;}

    /**
     * @param bytes the number of bytes to attempt to fit in the buffer/lookup
     * @returns true if there is no room for the number of bytes
     */
    bool would_overflow (const size_t bytes) const {
	return (_lookup_used == _lookup_size
		|| _buffer_used + bytes >= _buffer_size);
    }
    /** @returns true if the buffer does not contain any data */
    bool empty() const {return _buffer_used == 0;}
    /** Remove all data from the buffer and lookup table */
    void clear() {_buffer_used = _lookup_used = 0;}

    /**
     * Add a key and value to the buffer
     * @param keyvalue tab separated key and value
     * @param keylen length of key
     * @param size total length of keyvalue
     */
    void add (const char* keyvalue, const size_t keylen, size_t totalsize) {
	memcpy (&_buffer[_buffer_used], keyvalue, totalsize);
	_lookup[_lookup_used].set_ptr (&_buffer[_buffer_used], keylen,
				       ++totalsize);
	_buffer_used += totalsize;
	_buffer[_buffer_used - 1] = '\n';
	++_lookup_used;
    }

    void add_reserved (const size_t keylen, size_t totalsize) {
	_lookup[_lookup_used].set_ptr (&_buffer[_buffer_used], keylen,
				       ++totalsize);
	_buffer_used += totalsize;
	_buffer[_buffer_used - 1] = '\n';
	++_lookup_used;
    }

    /** @return true if buffer size vs lookup vector size is tuned */
    bool tuned() const {return _tuned;}
    /**
     * Tune size of buffer vs lookup vector
     * @param ratio ratio of buffer size vs lookup size in memory use
     */
    void tune (const double ratio);
    /** Get the current ratio between buffer and lookup vector sizes */
    double ratio() const {return _ratio;}
    /** Get the ideal ratio between buffer and lookup vector */
    double ideal_ratio() const;
    
    sorter_buffer (const sorter_buffer&) = delete;

private:
    size_t _bytes_available;

    char* _buffer = nullptr;
    size_t _buffer_size;
    size_t _buffer_used = 0;
    std::vector<struct lookup> _lookup;
    size_t _lookup_size;
    size_t _lookup_used = 0;

    double _ratio;
    bool _tuned = false;
};

#endif
