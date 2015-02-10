
#include <stdexcept>

#include "sorter_buffer.h"
#include "settings.h"

sorter_buffer::sorter_buffer(const size_t bytes_available, const double ratio)
    : _bytes_available(bytes_available), _ratio(ratio)
{
    double total_ratio = ratio + 1.0;

    _buffer_size = static_cast<size_t> (bytes_available / total_ratio * ratio);
    _lookup_size = static_cast<size_t> (bytes_available
					/ total_ratio / sizeof(struct lookup));
    //std::cerr << "b " << _size_buffer << " l " << _size_lookup << "\n";

    if (_lookup_size < 1)
    {
	throw std::runtime_error
	    ("Buffer size " + std::to_string(bytes_available)
	     + " for merge sort is too small");
    }

    _lookup.resize (_lookup_size);

    _buffer = new char[_buffer_size];
    //memset (_buffer, 0, _buffer_size); // for valgrind testing
}

sorter_buffer::sorter_buffer (sorter_buffer&& other) :
    _bytes_available (other._bytes_available),
    _buffer (other._buffer),
    _buffer_size (other._buffer_size),
    _buffer_used (other._buffer_used),
    _lookup (std::move(other._lookup)),
    _lookup_size (other._lookup_size),
    _lookup_used (other._lookup_used),
    _ratio (other._ratio)
{
    other._buffer = 0;
}

sorter_buffer::~sorter_buffer()
{
    if (_buffer) delete[] _buffer;
}

void
sorter_buffer::tune (const double ratio)
{
    if (_lookup_used < _lookup_size / 10 * 7
	|| _buffer_used < _buffer_size / 10 * 7)
    {
	_ratio = ratio;

	double total_ratio = _ratio + 1.0;
	size_t old_lookup_size = _lookup_size;

	_buffer_size = static_cast<size_t> (_bytes_available
					/ total_ratio * _ratio);
	_lookup_size = static_cast<size_t> (_bytes_available
					/ total_ratio / sizeof(struct lookup));

	delete[] _buffer;
        
	_lookup.resize (_lookup_size);
	if (_lookup_size < old_lookup_size) _lookup.shrink_to_fit();

	_buffer = new char[_buffer_size];
        
	// std::cerr << "Resized buffer/lookup ratio to " << _ratio << ".\n";
    }
    _tuned = true;
}

double
sorter_buffer::ideal_ratio() const
{
    double ratio ((double)_buffer_used
		  / (_lookup_used * sizeof(struct lookup)));

    if (ratio < .01) ratio = .01;
    else if (ratio > 100) ratio = 100;

    return ratio;
}
