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

#ifndef _HEXTREME_MAPREDO_INPUT_BUFFER_H
#define _HEXTREME_MAPREDO_INPUT_BUFFER_H

#include <memory>

class input_buffer
{
public:
    input_buffer (const size_t bytes) :
	_buf(new char[bytes+1]), _capacity (bytes) {}

    /** Get pointer to correctly sized buffer */
    char* get() {return _buf.get();}

    /** Get buffer capacity in bytes */
    size_t capacity() const {return _capacity;}

    /**
     * Increase buffer capacity.  The actual capacity reserved may be
     * bigger than the requested number of bytes.
     * @param bytes number of bytes to fit in buffer
     */
    void increase_capacity (size_t bytes) {
	abort(); // not yet implemented
    }

    /** Start of buffer */
    size_t& start() {return _start;}

    /** End of buffer */
    size_t& end() {return _end;}

private:
    std::unique_ptr<char[]> _buf;
    size_t _capacity;
    size_t _start = 0;
    size_t _end = 0;
};

#endif
