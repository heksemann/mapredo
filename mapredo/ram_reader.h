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

#ifndef _HEXTREME_MAPREDO_RAM_READER_H
#define _HEXTREME_MAPREDO_RAM_READER_H

#include "data_reader.h"

/**
 * Used when reading temporary files while merge sorting
 */
template <class T>
class ram_reader final : public data_reader<T>
{
public:
    /**
     * @param buffer memory buffer to be provided to the file merger
     * @param size buffer size
     */
    ram_reader (char* const buffer, const size_t size)  {
        this->buffer = buffer;
	this->_end_pos = size;
	this->fill_next_line();
    }
    ~ram_reader() {}

    ram_reader (const ram_reader&) = delete;
    ram_reader& operator=(const ram_reader&) = delete;
};

#endif
