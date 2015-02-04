
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif

#include "consumer.h"
#include "mapreducer.h"

consumer::consumer (mapredo::base& mapreducer,
		    const std::string& tmpdir,
		    const bool is_subdir,
		    const size_t buckets,
		    const size_t bytes_buffer,
		    const bool reverse) :
    _mapreducer (mapreducer),
    _tmpdir (tmpdir),
    _is_subdir (is_subdir),
    _buckets (buckets)
{
    std::cerr << "CONSUMER STARTING\n";
    for (size_t i = 0; i < buckets; i++)
    {
	_sorters.emplace_back (_tmpdir, i, bytes_buffer,
			       mapreducer.type(), reverse);
    }
}

consumer::~consumer()
{}

void
consumer::work (buffer_trader& trader)
{
    auto* buffer = trader.consumer_get();

    if (!buffer) return;

    size_t pos;
    char* buf = buffer->get();

    do
    {
	const size_t end = buffer->end();
	size_t start = buffer->start();

	while (start < end)
	{
	    for (pos = start; buf[pos] != '\n'; pos++) ;

	    if (pos == start || buf[pos-1] != '\r')
	    {
		buf[pos] = '\0';
		_mapreducer.map (buf + start, pos - start, *this);
	    }
	    else
	    {
		buf[pos-1] = '\0';
		_mapreducer.map (buf + start, pos - start - 1, *this);
	    }
	    start = pos + 1;
	}
    }
    while ((buffer = trader.consumer_swap(buffer)));
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
