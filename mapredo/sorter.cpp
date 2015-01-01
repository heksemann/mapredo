
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
#include "compression.h"

sorter::sorter (const std::string& tmpdir,
		const size_t index,
		const size_t bytes_buffer,
		const mapredo::base::keytype type,
		const bool reverse) :
    _tmpdir (tmpdir),
    _bytes_per_buffer (bytes_buffer/2),
    _type (type),
    _reverse (reverse)
{
    // proeve med int offsets inni buffer?

    _buffers.push_back (sorter_buffer(_bytes_per_buffer, 3.0));
    _current = &_buffers.front();
    
    std::ostringstream filename;

    filename << tmpdir << "/sort_" << std::this_thread::get_id()
	     << '.' << index << '.';
    _file_prefix = filename.str();

    if (settings::instance().compressed()) 
    {
	_compressor.reset (new compression());
    }
}

sorter::sorter (sorter&& other) :
    _buffers (std::move(other._buffers)),
    _current (other._current),
    _tmpdir (std::move(other._tmpdir)),
    _bytes_per_buffer (other._bytes_per_buffer),
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
sorter::add (const char* keyvalue, const size_t size)
{
    if (_current->would_overflow(size))
    {
#if 0
	std::cerr << _current->lookup_used() * sizeof(lookup)
		  << " >= " << _current->lookup_size() * sizeof(lookup)
		  << " || "
		  << _current->buffer_used() + size << " > "
		  << _current->buffer_size()
		  << " sizeof(lookup) " << sizeof(lookup)
		  << "\n";
#endif

	if (_buffers.size() < 2)
	{
	    double ratio = _current->ideal_ratio();

	    _buffers.push_back (sorter_buffer(_bytes_per_buffer, ratio));

	    std::string filename;
	    filename = flush_buffer (_current);
	    if (!filename.empty())
	    {
		_tmpfiles.push_back ((std::string&&)filename);
	    }
	    _current->tune (ratio);
	    _current = &_buffers.back();
	}
	else flush();
    }

    _current->add (keyvalue, size);
}

std::list<std::string>
sorter::grab_tmpfiles()
{
    return std::move(_tmpfiles);
}

void
sorter::flush()
{
    wait_flushed();
    _flushing_in_progress = true;
    _flush_result = std::async (std::launch::async,
				&sorter::flush_buffer_safe,
				this, _current);
    _current = ((_current == &_buffers.front())
		? &_buffers.back() : &_buffers.front());
}

void
sorter::wait_flushed()
{
    if (_flushing_in_progress)
    {
	std::string filename (_flush_result.get());
	if (_texception) std::rethrow_exception (_texception);
	if (filename.size()) _tmpfiles.push_back ((std::string&&)filename);
	_flushing_in_progress = false;
    }
}

std::string
sorter::flush_buffer_safe (sorter_buffer* const buffer)
{
    try
    {
	return flush_buffer (buffer);
    }
    catch (...)
    {
	_texception = std::current_exception();
	return ("");
    }
}

static bool sorter_r (const lookup& left, const lookup& right)
{
    for (int i = 0;; i++)
    {
	register int l = left.keyvalue()[i];
	register int r = right.keyvalue()[i];

	if ((l == '\t' || l == '\n' || l == '\0')
	    && r != '\t' && r != '\n' && r != '\0')
	{
	    return false;
	}
	if (r == '\t' || r == '\n' || r == '\0') return true;
	if (l < r) return false;
	if (l > r) return true;
    }
    return true;
}

static bool sorter_n (const lookup& left, const lookup& right)
{
    return atoll (left.keyvalue()) < atol (right.keyvalue());
}

static bool sorter_rn (const lookup& left, const lookup& right)
{
    return atoll (left.keyvalue()) > atol (right.keyvalue());
}

static bool sorter_d (const lookup& left, const lookup& right)
{
    return atof (left.keyvalue()) < atof (right.keyvalue());
}

static bool sorter_rd (const lookup& left, const lookup& right)
{
    return atof (left.keyvalue()) > atof (right.keyvalue());
}

std::string
sorter::flush_buffer (sorter_buffer* const buffer)
{
    if (buffer->lookup_used() == 0) return "";
    //std::cerr << "Before sorting " << buffer->lookup_used()  << "\n";
    switch (_type)
    {
    case mapredo::base::STRING:
	if (!_reverse)
	{
	    std::sort (buffer->lookup().begin(),
		       buffer->lookup().begin() + buffer->lookup_used());	
	}
	else
	{
	    std::sort (buffer->lookup().begin(),
		       buffer->lookup().begin() + buffer->lookup_used(),
		       &sorter_r);
	}
	break;
    case mapredo::base::INT64:
	if (!_reverse)
	{
	    std::sort (buffer->lookup().begin(),
		       buffer->lookup().begin() + buffer->lookup_used(),
		       &sorter_n);
	}
	else
	{
	    std::sort (buffer->lookup().begin(),
		       buffer->lookup().begin() + buffer->lookup_used(),
		       &sorter_rn);
	}
	break;
    case mapredo::base::DOUBLE:
	if (!_reverse)
	{
	    std::sort (buffer->lookup().begin(),
		       buffer->lookup().begin() + buffer->lookup_used(),
		       &sorter_d);
	}
	else
	{
	    std::sort (buffer->lookup().begin(),
		       buffer->lookup().begin() + buffer->lookup_used(),
		       &sorter_rd);
	}
	break;
    case mapredo::base::UNKNOWN:
	throw std::runtime_error ("Program error, keytype not set"
                                  " in mapredo::base");
	break;
    }
    //std::cerr << "After sorting\n";

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

    auto end = buffer->lookup().cbegin() + buffer->lookup_used();

    if (_compressor.get())
    {
	const size_t inbuffer_size = 0x10000;
	const size_t outbuffer_size = 0x15000;
	std::unique_ptr<char[]> inbuffer (new char[inbuffer_size]);
	std::unique_ptr<char[]> outbuffer (new char[outbuffer_size]);
	size_t inbufpos = 0;
	size_t outbufpos = outbuffer_size;

	for (auto iter = buffer->lookup().begin(); iter != end; iter++)
	{
	    const char* t (iter->keyvalue());
	    int i = 0;

	    while (t[i] != '\0' && t[i++] != '\n') ;
	    if (inbufpos + i > inbuffer_size)
	    {
		_compressor->compress (inbuffer.get(), inbufpos,
				       outbuffer.get(), outbufpos);
		tmpfile.write (outbuffer.get(), outbufpos);
		inbufpos = 0;
		outbufpos = outbuffer_size;
	    }
	    memcpy (inbuffer.get() + inbufpos, t, i);
	    inbufpos += i;
	}
	_compressor->compress (inbuffer.get(), inbufpos,
				outbuffer.get(), outbufpos);
	tmpfile.write (outbuffer.get(), outbufpos);
    }
    else
    {
	for (auto iter = buffer->lookup().begin(); iter != end; iter++)
	{
	    const char* t (iter->keyvalue());
	    int i = 0;

	    while (t[i] != '\0' && t[i++] != '\n') ;
	    tmpfile.write (t, i);
	}	
    }

    tmpfile.close();
    buffer->clear();

    return (filename.str());
}
