/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_COMPRESSION_H
#define _HEXTREME_MAPREDO_COMPRESSION_H

#include <fstream>
#include <vector>
#include <memory>
#include <cstring>
#include <iostream>
#include <sstream>

#include <snappy.h>

#include "lookup.h"

class compression
{
public:
    compression () = default;
    ~compression() {}

    /**
     * Request compression of data.  We prepend a 32 bit little endian
     * compressed size since this is not stored in the format and we need it.
     * @param inbuffer the buffer to compress data from.
     * @param inbuffer_size the number of used bytes in inbuffer.
     *        On return, this is adjusted to the number of bytes read.
     * @param outbuffer the buffer to compress data to.
     * @param outbuffer_size the number of bytes available in outbuffer.
     *        On return, this is adjusted to the number of bytes written.
     */
    void compress (const char* const inbuffer, size_t& inbuffer_size,
		   char *const outbuffer, size_t& outbuffer_size) {
	if (inbuffer_size > 0x10000) inbuffer_size = 0x10000;

	if (snappy::MaxCompressedLength(inbuffer_size) + 4 > outbuffer_size)
	{
	    std::ostringstream stream;

	    stream << "Snappy compression buffer too small (" << outbuffer_size
		   << ") for input data (" << inbuffer_size << "), needs "
		   << snappy::MaxCompressedLength(inbuffer_size) << " bytes";
	    throw std::runtime_error (stream.str());
	}
	
	snappy::RawCompress (inbuffer, inbuffer_size,
			     outbuffer + 4, &outbuffer_size);
	//std::cerr << inbuffer_size << " => " << outbuffer_size << "\n";
	outbuffer[0] = outbuffer_size & 0xff;
	outbuffer[1] = (outbuffer_size >> 8) & 0xff;
	outbuffer[2] = (outbuffer_size >> 16) & 0xff;
	outbuffer[3] = outbuffer_size >> 24;
	outbuffer_size += 4;
    }

    /**
     * Request decompression of data.
     * @param inbuffer the buffer to uncompress data from.

     * @param inbuffer_size the number of used bytes in inbuffer.
     *        On return, this is adjusted to the number of bytes read.
     * @param outbuffer the buffer to compress data to.
     * @param outbuffer_size the number of bytes available in outbuffer.
     *        On return, this is adjusted to the number of bytes written.
     * @returns true if some data is uncompressed, false if not enough
     *          data is available in inbuffer.  If false is returned,
     *          inbuffer_size and outbuffer_size is left unchanged.
     */
    bool inflate (const char* const inbuffer, size_t& inbuffer_size,
		  char* const outbuffer, size_t& outbuffer_size) {
	if (inbuffer_size < 5) return false;

	size_t uncomp_len;
	size_t comp_len = (uint8_t)inbuffer[0]
	    | (uint8_t)inbuffer[1] << 8
	    | (uint8_t)inbuffer[2] << 16
	    | (uint8_t)inbuffer[3] << 24;

	if (comp_len > inbuffer_size - 4) return false;
	inbuffer_size = comp_len + 4;

	if (!snappy::GetUncompressedLength (inbuffer + 4, comp_len,
					    &uncomp_len))
	{
	    throw std::runtime_error
		("Can not parse corrupted Snappy uncompressed size");
	}
	if (uncomp_len >= outbuffer_size)
	{
	    std::ostringstream stream;

	    stream << "Snappy output buffer too small (" << outbuffer_size
		   << ") for input data (" << inbuffer_size << "), needs "
		   << uncomp_len << " bytes";

	    throw std::runtime_error (stream.str());
	}

	if (snappy::RawUncompress(inbuffer + 4, comp_len, outbuffer))
	{
	    outbuffer_size = uncomp_len;
	    return true;
	}

	throw std::runtime_error
	    ("Can not uncompress corrupted Snappy data");
    }

    static int max_compressed_size (const int data_size) {
	return snappy::MaxCompressedLength (data_size);
    }
};

#endif
