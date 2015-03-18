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

#include "consumer.h"
#include "mapreducer.h"

consumer::consumer (mapredo::base& mapreducer,
		    const std::string& tmpdir,
		    const bool is_subdir,
		    const uint16_t buckets,
		    const uint16_t worker_id,
		    const size_t bytes_buffer,
		    const bool reverse,
		    merge_cache& cache) :
    _mapreducer (mapreducer),
    _tmpdir (tmpdir),
    _is_subdir (is_subdir),
    _buckets (buckets),
    _worker_id (worker_id)
{
    for (size_t i = 0; i < buckets; i++)
    {
	_sorters.emplace_back (_tmpdir, i, worker_id, bytes_buffer,
			       mapreducer.type(), cache, reverse);
    }
}

consumer::~consumer()
{
    join_thread();
}

void
consumer::start_thread (buffer_trader& trader)
{
    _thread = std::thread (&consumer::work, this, std::ref(trader));
}

void
consumer::join_thread()
{
    if (_thread.joinable()) _thread.join();
}

void
consumer::append_tmpfiles (const size_t index, std::list<std::string>& files)
{
    auto iter (_tmpfiles.find (index));

    if (iter != _tmpfiles.end()) files.splice (files.end(), iter->second);
}

void
consumer::work (buffer_trader& trader)
{
    try
    {
	auto* buffer = trader.consumer_get (_worker_id);

	if (!buffer) return;
	std::stringstream stream;

	size_t pos;

	do
	{
	    char* buf = buffer->get();
	    size_t start = buffer->start();
	    const size_t end = buffer->end();

	    while (start < end)
	    {
		for (pos = start; pos < end && buf[pos] != '\n'; pos++) ;

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
	while ((buffer = trader.consumer_swap(buffer, _worker_id)));

	for (auto& sorter: _sorters)
	{
	    sorter.flush();
	    _tmpfiles[sorter.hash_index()] = sorter.grab_tmpfiles();
	}
	_sorters.clear();
    }
    catch (...)
    {
	_texception = std::current_exception();
	trader.consumer_fail (_worker_id);
    }
}

static unsigned int hash (const char* str, size_t siz, size_t& keylen,
			  const char delim = '\t')
{
    unsigned int result = 0x55555555;

    for (keylen = 0; keylen < siz && str[keylen] != delim; keylen++)
    {
	result ^= str[keylen];
	result = ((result << 5) | (result >> 27));
    }
    return result;
}

void
consumer::collect (const char* inbuffer, const size_t insize)
{
    size_t keylen = 0;
    unsigned int bucket = hash(inbuffer, insize, keylen) % _buckets;

    _sorters[bucket].add (inbuffer, keylen, insize);
}

char*
consumer::reserve (const char* const key, const size_t bytes)
{
    _reserved_bucket = hash(key, 0, _reserved_keylen, '\0') % _buckets;
    _reserved_valuelen = bytes;

    char* buf = _sorters[_reserved_bucket].reserve
	(_reserved_keylen + 1 + bytes);
    memcpy (buf, key, _reserved_keylen);
    buf[_reserved_keylen] = '\t';

    return buf + _reserved_keylen + 1;
}

void
consumer::collect_reserved (const size_t length)
{
    if (length == 0)
    {
	_sorters[_reserved_bucket].add_reserved
	    (_reserved_keylen, _reserved_keylen + 1 + _reserved_valuelen);
    }
    else
    {
	_sorters[_reserved_bucket].add_reserved
	    (_reserved_keylen, _reserved_keylen + 1 + length);
    }
}
