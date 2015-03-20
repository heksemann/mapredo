/*
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

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <fstream>
#include <iostream>
#include <memory>
#include <cerrno>

#include "sorter.h"
#include "tmpfile_reader.h"
#include "file_merger.h"
#include "settings.h"
#include "merge_cache.h"
#include "compression.h"

sorter::sorter (const std::string& tmpdir,
		const uint16_t hash_index,
		const uint16_t worker_index,
		const size_t bytes_buffer,
		const mapredo::base::keytype type,
		merge_cache* const cache,
		const bool reverse) :
    _buffer (bytes_buffer, 3.0),
    _tmpdir (tmpdir),
    _bytes_per_buffer (bytes_buffer),
    _index (hash_index),
    _type (type),
    _cache (cache),
    _reverse (reverse)
{
    std::ostringstream filename;

    filename << tmpdir << "/sort_" << std::this_thread::get_id()
	     << ".h" << hash_index << ".w" << worker_index << ".n";
    _file_prefix = filename.str();

    if (settings::instance().compressed()) 
    {
	_compressor.reset (new compression());
    }
}

sorter::sorter (sorter&& other) noexcept :
    _buffer (std::move(other._buffer)),
    _tmpdir (std::move(other._tmpdir)),
    _bytes_per_buffer (other._bytes_per_buffer),
    _index (other._index),
    _file_prefix (std::move(other._file_prefix)),
    _compressor (std::move(other._compressor)),
    _type (other._type),
    _reverse (other._reverse)
{}

sorter::~sorter()
{
    for (auto& filename: _tmpfiles) std::remove (filename.c_str());
}

void
sorter::add (const char* keyvalue, const size_t keylen, const size_t size)
{
    make_room (size);
    _buffer.add (keyvalue, keylen, size);
}

char*
sorter::reserve (const size_t bytes)
{
    make_room (bytes);
    return (_buffer.buffer() + _buffer.buffer_used());
}

void
sorter::add_reserved (const size_t keylen, const size_t size)
{
  _buffer.add_reserved (keylen, size);
}

std::list<std::string>
sorter::grab_tmpfiles()
{
    return std::move(_tmpfiles);
}

static bool sorter_r (const lookup& left, const lookup& right)
{
    register uint16_t len = std::min(left.keylen(), right.keylen());
    register uint16_t i;
    register const char* const lkeyvalue = left.keyvalue();
    register const char* const rkeyvalue = right.keyvalue();

    for (i = 0; i < len && lkeyvalue[i] == rkeyvalue[i]; i++) ;

    if (i < len) return lkeyvalue[i] > rkeyvalue[i];
    return left.keylen() > right.keylen();
}

void
sorter::flush()
{
    if (_buffer.lookup_used() == 0) return;

    switch (_type)
    {
    case mapredo::base::STRING:
	if (!_reverse)
	{
	    std::sort (_buffer.lookup().begin(),
		       _buffer.lookup().begin() + _buffer.lookup_used());	
	}
	else
	{
	    std::sort (_buffer.lookup().begin(),
		       _buffer.lookup().begin() + _buffer.lookup_used(),
		       &sorter_r);
	}
	break;
    case mapredo::base::INT64:
	if (!_reverse)
	{
	    std::sort (_buffer.lookup().begin(),
		       _buffer.lookup().begin() + _buffer.lookup_used(),
		       [](const lookup& left, const lookup& right)
		       {
			 return (atoll(left.keyvalue())
				 < atol(right.keyvalue()));
		       });
	}
	else
	{
	    std::sort (_buffer.lookup().begin(),
		       _buffer.lookup().begin() + _buffer.lookup_used(),
		       [](const lookup& left, const lookup& right)
		       {
			 return (atoll(left.keyvalue())
				 > atol(right.keyvalue()));
		       });
	}
	break;
    case mapredo::base::DOUBLE:
	if (!_reverse)
	{
	    std::sort (_buffer.lookup().begin(),
		       _buffer.lookup().begin() + _buffer.lookup_used(),
		       [](const lookup& left, const lookup& right)
		       {
			 return (atof(left.keyvalue())
				 < atof(right.keyvalue()));
		       });
	}
	else
	{
	    std::sort (_buffer.lookup().begin(),
		       _buffer.lookup().begin() + _buffer.lookup_used(),
		       [](const lookup& left, const lookup& right)
		       {
			 return (atof(left.keyvalue())
				 > atof(right.keyvalue()));
		       });
	}
	break;
    case mapredo::base::UNKNOWN:
	throw std::runtime_error ("Program error, keytype not set"
                                  " in mapredo::base");
	break;
    }

    if (_cache)
    {
	_cache->add (_index, _buffer);
	return;
    }

    std::ofstream tmpfile;
    std::ostringstream filename;

    filename << _file_prefix << _tmpfile_id++;
    if (_compressor.get()) filename << ".snappy";

    tmpfile.open (filename.str(), std::ofstream::binary);
    if (!tmpfile)
    {
	char err[80];
#ifdef _WIN32
	strerror_s (err, sizeof(err), errno);
#endif
	throw std::invalid_argument
	    ("Unable to open " + filename.str() + " for writing: "
#ifndef _WIN32
	     + strerror_r (errno, err, sizeof(err))
#else
	     + err
#endif
	    );
    }

    auto end = _buffer.lookup().cbegin() + _buffer.lookup_used();

    if (_compressor.get())
    {
	const size_t inbuffer_size = 0x10000;
	const size_t outbuffer_size = 0x15000;
	std::unique_ptr<char[]> inbuffer (new char[inbuffer_size]);
	std::unique_ptr<char[]> outbuffer (new char[outbuffer_size]);
	size_t inbufpos = 0;
	size_t outbufpos = outbuffer_size;

	for (auto iter = _buffer.lookup().begin(); iter != end; iter++)
	{
	    if (inbufpos + iter->size() > inbuffer_size)
	    {
		_compressor->compress (inbuffer.get(), inbufpos,
				       outbuffer.get(), outbufpos);
		tmpfile.write (outbuffer.get(), outbufpos);
		inbufpos = 0;
		outbufpos = outbuffer_size;
	    }
	    memcpy (inbuffer.get() + inbufpos, iter->keyvalue(), iter->size());
	    inbufpos += iter->size();
	}
	_compressor->compress (inbuffer.get(), inbufpos,
				outbuffer.get(), outbufpos);
	tmpfile.write (outbuffer.get(), outbufpos);
    }
    else
    {
	for (auto iter = _buffer.lookup().begin(); iter != end; iter++)
	{
	    tmpfile.write (iter->keyvalue(), iter->size());
	}
    }

    tmpfile.close();
    _buffer.clear();
    _tmpfiles.push_back (std::move(filename.str()));
}

void
sorter::make_room (const size_t size)
{
    if (_buffer.would_overflow(size))
    {
#if 0
	std::cerr << _current->lookup_used() * sizeof(lookup)
		  << " >= " << _current->lookup_size() * sizeof(lookup)
		  << " || "
		  << _current->buffer_used() + size << " > "
		  << _current->buffer_size()
		  << " ideal ratio " << _current->ideal_ratio()
		  << "\n";
#endif

	if (!_buffer.tuned())
	{
	    double ratio = _buffer.ideal_ratio();

	    flush();
	    _buffer.tune (ratio);
	}
	else flush();
    }
}
