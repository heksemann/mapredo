
#include "buffer_trader.h"

buffer_trader::buffer_trader (const size_t buffer_size,
			      const size_t num_buffers,
			      const size_t num_threads) :
    _buffer_size (buffer_size), _max_buffers (num_buffers)
{
    if (num_buffers < num_threads + 1)
    {
	throw std::runtime_error ("Too few buffers");
    }
}

#include <iostream>
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
	(std::string(__FUNCTION__) + " called too many times");
}

#include <iostream>
input_buffer*
buffer_trader::producer_swap (input_buffer* buffer)
{
    std::unique_lock<std::mutex> lock(_mutex);

    _filled_buffers.push (buffer);
    
    if (_all_buffers.size() < _max_buffers)
    {
	_all_buffers.emplace_back (_buffer_size);
	return &_all_buffers.back();
    }

    while (_empty_buffers.empty()) _cond.wait (lock);

    buffer = _empty_buffers.top();
    _empty_buffers.pop();

    return buffer;
}

input_buffer*
buffer_trader::consumer_get()
{
    std::unique_lock<std::mutex> lock(_mutex);

    while (_filled_buffers.empty()) _cond.wait (lock);

    auto* buffer = _filled_buffers.top();
    _filled_buffers.pop();

    return buffer;
}

input_buffer*
buffer_trader::consumer_swap (input_buffer* buffer)
{
    std::unique_lock<std::mutex> lock(_mutex);

    _empty_buffers.push (buffer);
    
    while (_filled_buffers.empty()) _cond.wait (lock);

    buffer = _filled_buffers.top();
    _filled_buffers.pop();

    return buffer;
}
