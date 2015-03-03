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

#include <thread>
#include <iostream>
#include <sstream>

#include "buffer_trader.h"

static const char* snames[] =
{
    "INITIAL", "WORKING", "FILLED", "FINISHED"
};

buffer_trader::buffer_trader (const size_t buffer_size,
			      const size_t num_consumers) :
    _buffer_size (buffer_size),
    _num_consumers (num_consumers),
    _states (new std::atomic<state>[num_consumers]),
    _sems (new bsem[num_consumers]),
    _producer_scan (false)
{
    for (size_t i = 0; i < num_consumers; i++) _states[i] = INITIAL;
    _buffers.resize (num_consumers);
}

buffer_trader::~buffer_trader()
{}

input_buffer*
buffer_trader::producer_get()
{
    if (_all_buffers.size() < _num_consumers * 2)
    {
	_all_buffers.emplace_back (_buffer_size);
	return &_all_buffers.back();
    }

    throw std::runtime_error
	("buffer_trader::producer_get() called too many times");
}

input_buffer*
buffer_trader::producer_swap (input_buffer* buffer)
{
    input_buffer* empty = nullptr;

    if (_initiated < _num_consumers * 2)
    {
	_all_buffers.emplace_back (_buffer_size);
	empty = &_all_buffers.back();

	for(;;)
	{
	    state status = _states[_current_task];

	    if (status == INITIAL)
	    {
		_buffers[_current_task] = buffer;
		_states[_current_task] = FILLED;
		_sems[_current_task].post();
		_initiated++;
		_current_task = (_current_task + 1) % _num_consumers;
		return empty;
	    }
	    else if (status == FINISHED) return nullptr;
	    else if (status != FILLED && status != WORKING)
	    {
		std::ostringstream stream;
		stream << "Invalid state " << snames[status]
		       << " in buffer_trader::producer_swap()";
		throw std::runtime_error (stream.str());
	    }
	    _current_task = (_current_task + 1) % _num_consumers;
	}
    }

    size_t current = _current_task;
    _producer_scan = true;

    for (;;)
    {
	state status = _states[current];

	switch (status)
	{
	case INITIAL:
	{
	    std::ostringstream stream;
	    stream << "Invalid state " << snames[status]
		   << " in buffer_trader::consumer_get()";
	    throw std::runtime_error (stream.str());
	}
	case WORKING:
	{
	    bool expected = true;
	    if (!_producer_scan.compare_exchange_strong(expected, false))
	    {
		_producer_sem.wait();
	    }
	    input_buffer* nbuffer = _buffers[current];
	    _buffers[current] = buffer;
	    _states[current] = FILLED;
	    _sems[current].post();
	    _current_task = (current + 1) % _num_consumers;
	    return nbuffer;
	}
	case FINISHED:
	    return nullptr;
	default:
	    current = (current + 1) % _num_consumers;
	    if (current == _current_task)
	    {
		_producer_sem.wait();
		_producer_scan = true;
	    }
	    break;
	}
    }
}

input_buffer*
buffer_trader::consumer_get (const size_t id)
{
    _sems[id].wait();

    state status = _states[id];

    switch (status)
    {
    case FINISHED:
	return nullptr;
    case FILLED:
    {
	bool expected = true;
	if (_producer_scan.compare_exchange_strong(expected, false))
	{
	    _producer_sem.post();
	}
	input_buffer* buffer = _buffers[id];
	_states[id] = INITIAL;
	return buffer;
    }
    default:
    {
	std::ostringstream stream;
	stream << "Invalid state " << snames[status]
	       << " in buffer_trader::consumer_get()";
	throw std::runtime_error (stream.str());
    }
    }
}

input_buffer*
buffer_trader::consumer_swap (input_buffer* buffer, const size_t id)
{
    buffer->start() = 0;
    buffer->end() = 0;

    _sems[id].wait();

    state status = _states[id];

    switch (status)
    {
    case FINISHED:
	return nullptr;
    case FILLED:
    {
	bool expected = true;
	if (_producer_scan.compare_exchange_strong(expected, false))
	{
	    _producer_sem.post();
	}
	input_buffer* nbuffer = _buffers[id];
	_buffers[id] = buffer;
	_states[id] = WORKING;
	return nbuffer;
    }
    default:
    {
	std::ostringstream stream;
	stream << "Invalid state " << snames[status]
	       << " in buffer_trader::consumer_get()";
	throw std::runtime_error (stream.str());
    }
    }
}

void
buffer_trader::producer_finish()
{
    bool done;

    do
    {
	done = true;

	for (size_t i = 0; i < _num_consumers; i++)
	{
	    state status = _states[i];

	    if (status != FILLED)
	    {
		if (status != FINISHED)
		{
		    _states[i] = FINISHED;
		    _sems[i].post();
		}
	    }
	    else done = false;
	}
	std::this_thread::yield();
    }
    while (!done);
}

void
buffer_trader::consumer_fail (const size_t id)
{
    state status;

    while ((status = _states[id]) != FILLED && status != FINISHED)
    {
	std::this_thread::yield();
    }

    if (status != FINISHED) _states[id] = FINISHED;

    bool expected = true;
    if (_producer_scan.compare_exchange_strong(expected, false))
    {
	_producer_sem.post();
    }
}
