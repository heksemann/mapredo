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

#ifndef _HEXTREME_MAPREDO_LOOKUP_H
#define _HEXTREME_MAPREDO_LOOKUP_H

/**
 * Class used in an array to enable sorting of data.
 */
struct lookup
{
    /** @return a pointer to the key and value of the entry */
    const char* keyvalue() const {return _keyvalue;}
    /** @return the length of the key */
    uint16_t keylen() const {return _keylen;}
    /** @return the length of the entire line */
    uint32_t size() const {return _totallen;}

    /** Set the key and value of the entry */
    void set_ptr (const char* keyvalue,
		  const uint16_t keylen,
		  const uint16_t totallen) {
	_keyvalue = keyvalue;
	_keylen = keylen;
	_totallen = totallen;
    }
    /**
     * Operator used when sorting the array
     * @param the array element to compare this with
     */
    bool operator< (const lookup& right) const {
	uint16_t len = std::min(_keylen, right._keylen);

#if 0
	uint16_t i = 0;

	while (len - i >= sizeof(size_t)
	       && (*reinterpret_cast<const size_t*>(_keyvalue + i)
		   == *reinterpret_cast<const size_t*>(right._keyvalue + i)))
	{
	    i += sizeof(size_t);
	}
#endif

	for (uint16_t i = 0; i < len; i++)
	{
	    int l = _keyvalue[i];
	    int r = right._keyvalue[i];

	    if (l != r) return l < r;
	}
	return _keylen < right._keylen;
    }
    
private:
    const char* _keyvalue;
    uint16_t _keylen;
    uint32_t _totallen;
};

#endif
