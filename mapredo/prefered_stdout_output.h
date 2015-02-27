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

#ifndef _HEXTREME_MAPREDO_ALTERNATE_STDOUT_OUTPUT_H
#define _HEXTREME_MAPREDO_ALTERNATE_STDOUT_OUTPUT_H

#include "prefered_output.h"

/** Use to do lockfree best effort write to standard output. */
class prefered_stdout_output final : public prefered_output
{
public:
    prefered_stdout_output () : _busy (false) {}

    bool try_write (const char* buffer, const size_t size) {
	bool expected = false;
	if (_busy.compare_exchange_strong(expected, true))
	{
	    fwrite (buffer, size, 1, stdout);
	    _busy = false;
	    return true;
	}
	return false;
    }

private:
    std::atomic<bool> _busy;
};
#endif
