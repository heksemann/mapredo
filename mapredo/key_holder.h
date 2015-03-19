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

#ifndef _HEXTREME_MAPREDO_KEY_HOLDER_H
#define _HEXTREME_MAPREDO_KEY_HOLDER_H

#include "data_reader.h"

/**
 * A class that holds a copy of a key for all accepted template types for
 * mapreducers.
 */
template <class T>
class key_holder {
public:
    template<class U = T,
	     typename std::enable_if<std::is_fundamental<U>::value>
	     ::type* = nullptr>
    U get_key (data_reader<T>& reader) {
	auto key = reader.next_key();
	if (key) return *key;
	throw std::runtime_error
	    ("Attempted to key_handler::get_key() on an empty file");
    }

    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,
				     bool>::type* = nullptr>
    char* get_key (data_reader<T>& reader) {
	auto key = reader.next_key();
	if (key)
	{
	    _key_copy = *key;
	    return const_cast<char*>(_key_copy.c_str());
	}
	throw std::runtime_error
	    ("Attempted to key_handler::get_key() on an empty file");
    }
private:
    std::string _key_copy;
};

#endif
