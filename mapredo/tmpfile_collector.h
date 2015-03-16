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

#ifndef _HEXTREME_MAPREDO_TMPFILE_COLLECTOR_H
#define _HEXTREME_MAPREDO_TMPFILE_COLLECTOR_H

#include "settings.h"
#include "prefered_output.h"
#include "rcollector.h"

/**
 * A collector class that writes to a temporary file
 */
class tmpfile_collector : public mapredo::rcollector
{
public:
    tmpfile_collector (const std::string& file_prefix,
		       int& tmpfile_id,
		       prefered_output* alt_output) :
	_compressed (settings::instance().compressed()),
	_prefered_output (alt_output)
    {
	std::ofstream outfile;

	_filename_stream << file_prefix << tmpfile_id++;
	if (_compressed)
	{
	    _filename_stream << ".snappy";
            _coutbuffer.reset (new char[0x15000]);
	}
	_outfile.open (_filename_stream.str(), std::ofstream::binary);
        if (!_outfile)
        {
            char err[80];
#ifdef _WIN32
	    strerror_s (err, sizeof(err), errno);
#endif
            throw std::invalid_argument
                ("Unable to open " + _filename_stream.str() + " for writing: "
#ifndef _WIN32
                 + strerror_r (errno, err, sizeof(err))
#else
		 + err
#endif
		);
        }

    }
    ~tmpfile_collector() {_outfile.close();}

    /** Collect data from reducer */
    virtual void collect (const char* line, const size_t length) final
    {
	if (_buffer_pos + length >= _buffer_size) flush_internal();
	memcpy (_buffer + _buffer_pos, line, length);
	_buffer_pos += length;
	_buffer[_buffer_pos++] = '\n';
    }

    /** Reserve memory buffer for reducer */
    virtual char* reserve (const size_t bytes) final {
	_reserved_bytes = bytes;
	if (_buffer_pos + bytes >= _buffer_size)
	{
	    flush_internal();
	    if (bytes >= _buffer_size)
	    {
		std::ostringstream stream;
		stream << "Cannot reserve " << bytes
		       << " bytes in tmpfile_collector::reserve(), "
		       << (_buffer_size - 1) << " was attempted";
		throw std::runtime_error (stream.str());
	    }
	    return _buffer;
	}
	else return (_buffer + _buffer_pos);
    }

    virtual void collect_reserved (const size_t length = 0) {
	if (_reserved_bytes == 0)
	{
	    throw std::runtime_error
                ("No memory reserved via reserve() in"
		 " tmpfile_collector::collect_reserved()");
	}

	if (length == 0) _buffer_pos += _reserved_bytes;
	else _buffer_pos += length;
	_buffer[_buffer_pos++] = '\n';

	_reserved_bytes = 0;
    }

    void flush() {if (_buffer_pos > 0) flush_internal();}

    /** @returns the name of the temporary file */
    std::string filename() {return _filename_stream.str();}

private:
    void flush_internal() {
	if (!_prefered_output
	    || !_prefered_output->try_write(_buffer, _buffer_pos))
	{
	    if (_compressed)
	    {
		_coutbufpos = 0x15000;
		_compressor->compress (_buffer,
				       _buffer_pos,
				       _coutbuffer.get(),
				       _coutbufpos);
		_outfile.write (_coutbuffer.get(), _coutbufpos);
	    }
	    else _outfile.write (_buffer, _buffer_pos);
	}
	_buffer_pos = 0;
    }

    static const size_t _buffer_size = 0x10000;

    std::ostringstream _filename_stream;
    const bool _compressed;
    std::ofstream _outfile;
    std::unique_ptr<compression> _compressor;
    char _buffer[_buffer_size];
    std::unique_ptr<char[]> _coutbuffer;
    size_t _buffer_pos = 0;
    size_t _coutbufpos;
    size_t _reserved_bytes = 0;
    prefered_output* _prefered_output;
};

#endif
