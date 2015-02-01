
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif

#include "consumer.h"
#include "collector.h"
#ifndef _WIN32
#include "directory.h"
#include "plugin_loader.h"
#else
#include "directory_win32.h"
#include "plugin_loader_win32.h"
#endif
#include "compression.h"

consumer::consumer (const std::string& tmpdir,
		    const std::string& subdir,
		    const size_t buckets,
		    const size_t bytes_buffer,
		    const mapredo::base::keytype type,
		    const bool reverse) :
    _tmpdir (subdir.empty() ? tmpdir : (tmpdir + "/" + subdir)),
    _is_subdir (subdir.size()),
    _buckets (buckets)
{
#ifndef _WIN32
    if (access(tmpdir.c_str(), R_OK|W_OK|X_OK) != 0)
#else
    if (_access_s(tmpdir.c_str(), 0x06) != 0)
#endif
    {
	throw std::runtime_error (tmpdir + " needs to be a writable directory");
    }

    if (_is_subdir)
    {
	if (!directory::exists(_tmpdir)) directory::create (_tmpdir);
    }    

    for (size_t i = 0; i < buckets; i++)
    {
	_sorters.emplace_back (_tmpdir, i, bytes_buffer/buckets, type, reverse);
    }
}

consumer::~consumer()
{}

void
consumer::process (buffer_trader& trader)
{
    auto* buffer = trader.consumer_get();

    if (!buffer) return;
	
    do
    {
	
    }
    while ((buffer = trader.consumer_swap()));
}

static unsigned int hash (const char* str, size_t siz)
{
    unsigned int result = 0x55555555;
    size_t i;

    for (i = 0; i < siz && str[i] != '\t'; i++)
    {
	result ^= str[i];
	result = ((result << 5) | (result >> 27));
    }
    return result;
}


void
consumer::collect (const char* inbuffer, const size_t insize)
{
    if (_buckets > 1)
    {
	_sorters[hash(inbuffer, insize) % _buckets].add (inbuffer, insize);
    }
    else _sorters[0].add (inbuffer, insize);
}
