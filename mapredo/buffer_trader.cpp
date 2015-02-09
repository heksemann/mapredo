
#include <iostream>
#include "buffer_trader.h"

buffer_trader::buffer_trader (const size_t buffer_size,
			      const size_t num_buffers,
			      const size_t num_consumers) :
    _buffer_size (buffer_size),
    _max_buffers (num_buffers),
    _num_consumers (num_consumers)
{
    if (num_buffers < num_consumers + 1)
    {
	throw std::runtime_error (std::string("Too few buffers in ")
				  + __FUNCTION__);
    }
}

buffer_trader::~buffer_trader()
{}

input_buffer*
buffer_trader::producer_get()
{
    std::lock_guard<std::mutex> lock(_mutex);

    if (_all_buffers.size() < _max_buffers - 1)
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
    std::unique_lock<std::mutex> lock(_mutex);

    if (_finished) return nullptr;

    bool was_empty = _filled_buffers.empty();

    _filled_buffers.push (buffer);
    if (was_empty) _consumer_cond.notify_one();
    
    if (_all_buffers.size() < _max_buffers)
    {
	_all_buffers.emplace_back (_buffer_size);
	return &_all_buffers.back();
    }

    while (_empty_buffers.empty() && !_finished)
    {
	_producer_cond.wait (lock);
    }

    if (_finished) return nullptr;

    buffer = _empty_buffers.top();
    _empty_buffers.pop();

    return buffer;
}

input_buffer*
buffer_trader::consumer_get()
{
    std::unique_lock<std::mutex> lock(_mutex);

    while (!_finished && _filled_buffers.empty())
    {
	_consumer_cond.wait (lock);
    }

    if (_filled_buffers.size() && (!_finished || _success))
    {
	auto* buffer = _filled_buffers.top();
	_filled_buffers.pop();

	return buffer;
    }

    return nullptr;
}

input_buffer*
buffer_trader::consumer_swap (input_buffer* buffer)
{
    std::unique_lock<std::mutex> lock(_mutex);

    if (_finished)
    {
	if (_filled_buffers.empty() || !_success) return nullptr;
	else
	{
	    buffer = _filled_buffers.top();
	    _filled_buffers.pop();

	    return buffer;
	}
    }
    
    bool was_empty = _empty_buffers.empty();

    buffer->start() = 0;
    buffer->end() = 0;
    _empty_buffers.push (buffer);
    if (was_empty) _producer_cond.notify_one();
    
    while (!_finished && _filled_buffers.empty())
    {
	_consumer_cond.wait (lock);
    }
    if (_filled_buffers.size()  && (!_finished || _success))
    {
	buffer = _filled_buffers.top();
	_filled_buffers.pop();

	return buffer;
    }

    return nullptr;
}

void
buffer_trader::finish (const bool success)
{
    std::unique_lock<std::mutex> lock(_mutex);

    if (!_finished)
    {
	_finished = true;
	_success = success;
	_consumer_cond.notify_all();
	_producer_cond.notify_all();
    }
}
