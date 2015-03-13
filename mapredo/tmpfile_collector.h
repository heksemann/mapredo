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
            _cinbuffer.reset (new char[0x10000]);
            _coutbuffer.reset (new char[0x15000]);
            _cinbufpos = 0;
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
	if (_compressed)
	{
	    if (_cinbufpos + length >= 0x10000)
	    {
		if (_prefered_output
		    && _prefered_output->try_write(_cinbuffer.get(),
						   _cinbufpos))
		{
		    _cinbufpos = 0;
		}
		else
		{
		    _coutbufpos = 0x15000;
		    _compressor->compress (_cinbuffer.get(),
					   _cinbufpos,
					   _coutbuffer.get(),
					   _coutbufpos);
		    _outfile.write (_coutbuffer.get(), _coutbufpos);
		    _cinbufpos = 0;
		}
	    }
	    memcpy (_cinbuffer.get() + _cinbufpos, line, length);
	    _cinbufpos += length;
	    _cinbuffer.get()[_cinbufpos++] = '\n';
	}
	else
	{
	    _outfile.write (line, length);
	    _outfile.write ("\n", 1);
	}
    }

    /** Reserve memory buffer for reducer */
    virtual char* reserve (const size_t bytes) final
    {
        abort();
    }

    virtual void collect_reserved (const size_t length = 0) {
        abort();
    }

    void flush() {
	if (_compressed && _cinbufpos > 0)
	{
	    _coutbufpos = 0x15000;
	    _compressor->compress (_cinbuffer.get(),
				   _cinbufpos,
				   _coutbuffer.get(),
				   _coutbufpos);
	    _outfile.write (_coutbuffer.get(), _coutbufpos);
	}
    }

    /** @returns the name of the temporary file */
    std::string filename() {return _filename_stream.str();}

private:
    std::ostringstream _filename_stream;
    const bool _compressed;
    std::ofstream _outfile;
    std::unique_ptr<compression> _compressor;
    std::unique_ptr<char[]> _cinbuffer;
    std::unique_ptr<char[]> _coutbuffer;
    size_t _cinbufpos;
    size_t _coutbufpos;
    prefered_output* _prefered_output;
};

#endif
