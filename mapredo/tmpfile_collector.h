/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_TMPFILE_COLLECTOR_H
#define _HEXTREME_MAPREDO_TMPFILE_COLLECTOR_H

#include "settings.h"

class tmpfile_collector : public mapredo::collector
{
public:
    tmpfile_collector (const std::string& file_prefix, int& tmpfile_id)
	: _compressed (settings::instance().compressed())
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
	_outfile.open (_filename_stream.str());
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

    void collect (const char* line, const size_t length)
    {
	if (_compressed)
	{
	    if (_cinbufpos + length >= 0x10000)
	    {
		_coutbufpos = 0x15000;
		_compressor->compress (_cinbuffer.get(),
				       _cinbufpos,
				       _coutbuffer.get(),
				       _coutbufpos);
		_outfile.write (_coutbuffer.get(), _coutbufpos);
		_cinbufpos = 0;
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
};

#endif
