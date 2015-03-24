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

#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif
#include <cstdio>
#include <cctype>
#include <iostream>
#include <future>
#include <stdexcept>

#include "engine.h"
#ifndef _WIN32
#include "directory.h"
#include "plugin_loader.h"
#else
#include "directory_win32.h"
#include "plugin_loader_win32.h"
#endif
#include "settings.h"
#include "compression.h"
#include "prefered_stdout_output.h"

engine::engine (const std::string& plugin,
		const std::string& tmpdir,
		const std::string& subdir,
		const uint16_t parallel,
		const size_t bytes_buffer,
		const size_t merger_cache_size,
		const int max_open_files) :
    _plugin_loader (plugin),
    _tmpdir (subdir.empty() ? tmpdir : (tmpdir + "/" + subdir)),
    _is_subdir (subdir.size()),
    _parallel (parallel),
    _bytes_buffer (bytes_buffer),
    _merger_cache_size (merger_cache_size/parallel),
    _max_files (max_open_files),
    _buffer_trader (0x100000, parallel)
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

    if (_merger_cache_size < bytes_buffer * 2 && _merger_cache_size != 0)
    {
	throw std::runtime_error
	    ("The merger cache buffer is too small, use either size 0"
	     " or a larger size");
    }
}

engine::~engine()
{
    _buffer_trader.producer_finish();
    for (auto& consumer: _consumers) consumer.join_thread();
}

input_buffer*
engine::prepare_input()
{
    try
    {
	if (_next_buffer)
	{
	    throw std::runtime_error (std::string("engine::") + __FUNCTION__
				      + " shall only be called once");
	}
	_next_buffer = _buffer_trader.producer_get();

	for (uint16_t i = 0; i < _parallel; i++)
	{
	    mapredo::base& mapper (_plugin_loader.get());

	    if (_merger_cache_size > 0)
	    {
		_merge_caches.emplace_back (mapper, _tmpdir, "FIXME",
					    _merger_cache_size, i);
		_consumers.emplace_back (mapper, _tmpdir, _is_subdir,
					 _parallel, i, _bytes_buffer,
					 settings::instance().reverse_sort(),
					 &_merge_caches.back());
	    }
	    else
	    {
		_consumers.emplace_back (mapper, _tmpdir, _is_subdir,
					 _parallel, i, _bytes_buffer,
					 settings::instance().reverse_sort(),
					 nullptr);
	    }
	    _consumers.back().start_thread (_buffer_trader);
	}

	return _buffer_trader.producer_get();
    }
    catch (...)
    {
	_buffer_trader.producer_finish();
	throw;
    }
}

static void
transfer_end (input_buffer* current, input_buffer* next)
{
    const size_t start (current->start());
    char* buf (current->get());

    for (size_t i = current->end(); i > start; i--)
    {
	if (buf[i-1] == '\n')
	{
	    next->end() = current->end() - i;
	    if (next->end() > 0)
	    {
		memcpy (next->get(), buf + i, next->end());
		current->end() = i;
	    }
	    return;
	}
    }

    throw std::runtime_error ("No newlines found in input buffer: \""
			      + std::string(buf, current->end() - start)
			      + "\"");
}

static void wait_consumers (std::list<consumer>& consumers)
{
    for (auto& consumer: consumers)
    {
	consumer.join_thread();
	if (consumer.exception_ptr())
	{
	    std::rethrow_exception (consumer.exception_ptr());
	}
    }
}

input_buffer*
engine::provide_input_data (input_buffer* data)
{
    try
    {
	if (!_next_buffer)
	{
	    throw std::runtime_error
		("engine::prepare_sorting() must be called before"
		 " engine::provide_input_data()");
	}

	transfer_end (data, _next_buffer);
	auto* current = _next_buffer;
	_next_buffer = _buffer_trader.producer_swap (data);
	if (!_next_buffer) wait_consumers (_consumers);
	return current;
    }
    catch (...)
    {
	_buffer_trader.producer_finish();
	throw;
    }
}

void
engine::complete_input (input_buffer* data)
{
    try
    {
	if (data->start() != data->end()) _buffer_trader.producer_swap (data);
	_buffer_trader.producer_finish();
    }
    catch (...)
    {
	_buffer_trader.producer_finish();
	throw;
    }
    wait_consumers (_consumers);
}

void
engine::reduce()
{
    for (size_t i = 0; i < _parallel; i++)
    {
	std::list<std::string> tmpfiles;
	merge_cache::buffer_list buffers;

	for (auto& consumer: _consumers)
	{
	    consumer.append_tmpfiles (i, tmpfiles);
	}
	for (auto& cache: _merge_caches)
	{
	    cache.append_tmpfiles (i, tmpfiles);
	    cache.append_cache_buffers (i, buffers);
	}

	if ((tmpfiles.size() + buffers.size() == 1)
	    && settings::instance().sort_output())
	{
	    if (tmpfiles.size())
	    {
		_files_final_merge.push_back (tmpfiles.front());
	    }
	    if (buffers.size())
	    {
		_buffers_final_merge.push_back (buffers.front());
	    }
	}
	else if (tmpfiles.size() + buffers.size())
	{
	    _mergers.emplace_back (_plugin_loader.get(),
				   _tmpdir, _unique_id++,
				   _max_files/_parallel,
				   std::move(tmpfiles),
				   std::move(buffers));
	}
    }
    _consumers.clear();

    auto& mapreducer (_plugin_loader.get());
    if (settings::instance().sort_output()) merge_sorted (mapreducer);
    else merge_grouped (mapreducer);
}

void
engine::reduce_existing_files()
{
    if (!_is_subdir)
    {
	throw std::logic_error
	    ("Subdir need to be set for reduce only processing");
    }

    try
    {
	std::vector<std::list<std::string>> lists;
	directory dir (_tmpdir);

	lists.resize (_parallel);

	for (const auto& file: dir)
	{
	    const char *period = strchr (file, '.');

	    if (!period || period[1] != 'h' || !isdigit(period[2]))
	    {
		throw std::runtime_error (std::string("Invalid tmpfile name ")
					  + file);
	    }
	    lists[atoi(period+2)%_parallel].push_back (_tmpdir + "/" + file);
	}

	for (auto& tmpfiles: lists)
	{
	    if (tmpfiles.size() == 1 && settings::instance().sort_output())
	    {
		_files_final_merge.push_back (tmpfiles.front());
	    }
	    else if (tmpfiles.size())
	    {
		_mergers.push_back
		    (file_merger
		     (_plugin_loader.get(),
		      _tmpdir, _unique_id++, _max_files/_parallel,
		      static_cast<std::list<std::string>&&>(tmpfiles),
		      merge_cache::buffer_list()));
	    }
	}

	auto& mapreducer (_plugin_loader.get());

	if (settings::instance().sort_output()) merge_sorted (mapreducer);
	else merge_grouped (mapreducer);
    }
    catch (...)
    {
	if (!settings::instance().keep_tmpfiles())
	{
	    directory::remove(_tmpdir, true);
	}
	throw;
    }

    if (!settings::instance().keep_tmpfiles())
    {
	directory::remove(_tmpdir);
    }
}

void
engine::merge_grouped (mapredo::base& mapreducer)
{
    auto iter = _mergers.begin();

    if (_files_final_merge.empty() && _buffers_final_merge.empty())
    {
	if (_mergers.empty()) return;
	if (_mergers.size() == 1)
	{
	    iter->merge();
	    return;
	}
    }

    prefered_stdout_output prefered_sink;
    std::vector<std::future<std::string>> results;
    results.resize (_mergers.size());
    auto riter = results.begin();

    for (auto& merger: _mergers)
    {
	*riter++ = std::async (std::launch::async,
			       &file_merger::merge_to_file,
			       &merger, &prefered_sink);
    }

    for (iter = _mergers.begin(), riter = results.begin();
	 iter != _mergers.end(); iter++, riter++)
    {
	std::string res (riter->get());
	if (iter->exception_ptr())
	{
	    std::rethrow_exception(iter->exception_ptr());
	}
	_files_final_merge.push_back (res);
    }
    _mergers.clear();

    output_final_files();
    _files_final_merge.clear();
}

void
engine::merge_sorted (mapredo::base& mapreducer)
{
    std::vector<std::future<std::list<std::string>>> results;
    results.resize (_mergers.size());
    auto iter = _mergers.begin();
    auto riter = results.begin();

    if (_files_final_merge.empty())
    {
	if (_mergers.empty()) return;
	if (_mergers.size() == 1)
	{
	    iter->merge();
	    return;
	}
    }

    for (; iter != _mergers.end(); iter++, riter++)
    {
	auto& merger (*iter);

	*riter = std::async (std::launch::async,
			     &file_merger::merge_to_files,
			     &merger);
    }

    for (iter = _mergers.begin(), riter = results.begin();
	 iter != _mergers.end(); iter++, riter++)
    {
	if (iter->exception_ptr())
	{
	    std::rethrow_exception(iter->exception_ptr());
	}
	for (auto& entry: riter->get())
	{
	    _files_final_merge.push_back (entry);
	}
    }
    _mergers.clear();

    file_merger merger
	(mapreducer,
	 _tmpdir, _unique_id, _max_files,
	 static_cast<std::list<std::string>&&>(_files_final_merge),
	 merge_cache::buffer_list());
    merger.merge();

    _files_final_merge.clear();
}

void
engine::output_final_files()
{
    static const size_t bufsize = 0x10000;
    std::unique_ptr<char[]> buf (new char[bufsize]); 
    std::unique_ptr<char[]> cbuf;
    std::unique_ptr<compression> compressor;

    if (settings::instance().compressed())
    {
	cbuf.reset (new char[0x15000]);
	compressor.reset (new compression());
    }

    for (auto& file: _files_final_merge)
    {
	size_t bytes;
	FILE *fp = fopen (file.c_str(), "rb");

	if (!fp)
	{
	    char err[80];

	    throw std::runtime_error ("Can not open " + file + ": "
				      + strerror_r(errno, err, sizeof(err)));
	}
	if (!settings::instance().keep_tmpfiles()) unlink (file.c_str());

	if (settings::instance().compressed())
	{
	    size_t start = 0;
	    size_t end = 0;
	    size_t insize;
	    size_t outsize = bufsize;

	    while ((bytes = fread(cbuf.get() + end,
				  1, 0x15000 - end, fp)) > 0)
	    {
		end += bytes;
		insize = end;

		while (insize > 0
		       && compressor->inflate (cbuf.get() + start, insize,
					       buf.get(), outsize))
		{
		    fwrite (buf.get(), outsize, 1, stdout);
		    start += insize;
		    insize = end - start;
		    outsize = bufsize;
		}

		if (insize == 0) start = end = 0;
		else
		{
		    end -= start;
		    memmove (cbuf.get(), cbuf.get() + start, end);
		    start = 0;
		}
#if 0
		else throw std::runtime_error
			 ("Can not read compressed data from temporary file"
			  " in final output phase");
#endif
	    }
	}
	else
	{
	    size_t bytes;
	    
	    while ((bytes = fread(buf.get(), 1, bufsize, fp)) > 0)
	    {
		fwrite (buf.get(), bytes, 1, stdout);
	    }
	}
	fclose (fp);
    }
}
