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

#ifndef _HEXTREME_MAPREDO_DATA_READER_H
#define _HEXTREME_MAPREDO_DATA_READER_H

#include <type_traits>
#include <iostream>
#include <stdexcept>

/** Base class for classes used when reading data in merge sort phase */
template <class T> class data_reader
{
public:
    virtual ~data_reader() {
	if (_buffer) delete[] _buffer;
    }

    /**
     * Peek at the next key from the data source.  This function may
     * be called multiple times and it will return the same pointer
     * until get_next_value or get_next_line() is called.
     * @returns pointer to next key if there is more data, nullptr otherwise
     */
    const T* next_key() {
	if (_keylen > 0) return &_key;
	if (_keylen == 0)
	{
	    fill_next_line();
	    if (_keylen > 0) return &_key;
	}
	return nullptr;
    }

    /**
     * Get the next value.  Remember to call next_key() before calling
     * this function.
     * @returns null terminated pointer to next value field or nullptr.
     */
    char* get_next_value();

    /**
     * Get the next line (key-value pair) from the file.  The line is
     * not nul-terminated.  Remember to call next_key() before calling
     * this function.
     * @param size set to the size of the keyvalue field.
     * @returns pointer to next keyvalue field or nullptr.
     */
    const char* get_next_line (size_t& length);

    /**
     * Get a copy of the next key.  This function is not defined
     * unless T (and the key) is char*.
     * @returns std::string copy of the char* key
     */
    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,bool>::type*
	     = nullptr>
    std::string get_key_copy() {
	auto key = next_key();
	if (key) return std::string(_key);
	throw std::runtime_error
	    ("data_reader::get_key_copy() with no data left");
    }

    /** Comparison with a key, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_fundamental<U>::value>::type*
	     = nullptr>
    bool operator==(const T key) {
	return next_key() && _key == key;
    }

    /** Comparison with a key, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,
				     bool>::type* = nullptr>
    bool operator==(const char* const key) {
	if (!next_key()) return false;
	return strcmp (_key, key) == 0;
    }

    /** Comparison with other object, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_fundamental<U>::value>::type*
	     = nullptr>
    int compare (const T& other_key) {
	return _key - other_key;
    }
    
    /** Comparison with other object, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,
				     bool>::type* = nullptr>
    int compare (const char* const other_key) {
	return strcmp (_key, other_key);
    }

    data_reader (const data_reader&) = delete;
    data_reader& operator=(const data_reader&) = delete;
    
protected:
    data_reader() {
	static_assert (std::is_same<T,char*>::value
                       || std::is_same<T,int64_t>::value
		       || std::is_same<T,double>::value,
                       "Only char*, int64_t and double keys are supported");
    }

    /** Prepare next line from buffer */
    void fill_next_line();

    /** Override this if the data source can provide more data. */
    virtual bool read_more() {return false;}

    char* _buffer = nullptr;
    size_t _start_pos = 0;
    size_t _end_pos = 0;

private:
    template<class U = T,
	     typename std::enable_if<std::is_floating_point<U>::value>::type*
	     = nullptr>
    void set_key (const char* const buf) {
	_key = atof (buf);
    }

    template<class U = T,
	     typename std::enable_if<std::is_integral<U>::value>::type*
	     = nullptr>
    void set_key (const char* const buf) {
	_key = atol (buf);
    }

    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,
				     bool>::type* = nullptr>
    void set_key (char* const buf) {
	_key = buf;
    }

    T _key;
    int _keylen = 0;
    int _totallen = 0;
};

template <class T> void
data_reader<T>::fill_next_line()
{
    _keylen = _totallen = -1;
    if (_start_pos == _end_pos && !read_more()) return;

    size_t i;

    set_key (_buffer + _start_pos);

    for (i = _start_pos; i < _end_pos; i++)
    {
	if (_buffer[i] == '\t')
	{
	    _keylen = i - _start_pos;
	    _buffer[i] = '\0';
	    break;
	}
	else if (_buffer[i] == '\n') break;
    }
    for (; i < _end_pos; i++)
    {
	if (_buffer[i] == '\n')
	{
	    _buffer[i] = '\0';
	    _totallen = i - _start_pos;
	    if (_keylen < 0)
	    {
		_keylen = _totallen;
	    }
	    return;
	}
    }

    if (!read_more())
    {
	throw std::runtime_error
	    ("Temporary data does not contain newlines or tabs");
    }

    set_key (_buffer);

    for (i = 0; i < _end_pos; i++)
    {
	if (_keylen < 0 && _buffer[i] == '\t')
	{
	    _keylen = i;
	    _buffer[i] = '\0';
	}
	else if (_buffer[i] == '\n')
	{
	    _buffer[i] = '\0';
	    _totallen = i;
	    if (_keylen < 0) _keylen = _totallen;
	    return;
	}
    }

    if (i == _end_pos)
    {
	throw std::runtime_error
	    ("Temporary file does not contain trailing newline");
    }
}

template <class T> char*
data_reader<T>::get_next_value()
{
    if (_keylen <= 0)
    {
	throw std::runtime_error ("Programming error, keylen is zero in "
				  + std::string(__FUNCTION__) + "()");
    }

    char *value = _buffer + _start_pos + _keylen;

    if (_keylen == _totallen) _start_pos += _keylen + 1;
    else
    {
	value++;
	_start_pos += _totallen + 1;
    }
    _keylen = 0;

    return value;
}

template <class T> const char*
data_reader<T>::get_next_line (size_t& length)
{
    if (_keylen <= 0)
    {
	throw std::runtime_error ("Programming error, keylen is zero in "
				  + std::string(__FUNCTION__) + "()");
    }

    char* line = _buffer + _start_pos;

    line[_keylen] = '\t';
    line[_totallen] = '\n';
    length = ++_totallen;
    _start_pos += _totallen;
    _keylen = 0;

    return line;
}

#endif
