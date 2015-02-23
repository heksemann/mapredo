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

#ifndef _HEXTREME_MAPREDO_COLLECTOR_H
#define _HEXTREME_MAPREDO_COLLECTOR_H

#include <sstream>

namespace mapredo
{
    /**
     * Collect class for mapreducers
     */
    class collector
    {
    public:
	virtual void collect (const char* line, const size_t length) = 0;
	template<typename T1, typename T2>
	void collect_keyval (const T1& key, const T2& value) {
	    std::ostringstream stream;
	    stream << key << '\t' << value;
	    const std::string& val (stream.str());
	    collect (val.c_str(), val.size());
	}
	virtual ~collector() {}

    protected:
	collector() = default;
    };
}

#endif
