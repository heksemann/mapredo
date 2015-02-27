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

#ifndef _HEXTREME_MAPREDO_PREFERED_OUTPUT_H
#define _HEXTREME_MAPREDO_PREFERED_OUTPUT_H

/** Used to do a lockfree best effort to write to an alternative output sink. */
class prefered_output
{
public:
    prefered_output() {}
    virtual ~prefered_output() {}

    /**
     * Try to write to the alternative output destination.
     * @param buffer data to write
     * @param size number of bytes in buffer
     * @returns true if data was written, false otherwise
     */
    virtual bool try_write (const char* buffer, const size_t size) = 0;
};
#endif
