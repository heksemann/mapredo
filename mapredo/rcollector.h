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

#ifndef _HEXTREME_MAPREDO_RCOLLECTOR_H
#define _HEXTREME_MAPREDO_RCOLLECTOR_H

#include "collector.h"

namespace mapredo
{
    /**
     * Collect class for reducers
     */
    class rcollector : public collector 
    {
    public:
        /**
	 * Reserve memory buffer to store collected data to.  The
	 * buffer is later provided to collect_reserved().  This saves
	 * copying data as the case is by collect() or collect_keyval().
	 * @param bytes the number of bytes to reserve for data
	 * @returns buffer to write output to
	 */
        virtual char* reserve (const size_t bytes) = 0;

        /**
	 * Collect from the reserved memory buffer returned from reserve().
	 * @param bytes the number of bytes to provide from the buffer.
	 *              If left out, the reserved number of bytes is
	 *              collected.  The value may not be larger than
	 *              the reserved bytes.
	 */
	 virtual void collect_reserved (const size_t length = 0) = 0;
    };
}

#endif
