/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_SORTER_BUFFER_H
#define _HEXTREME_MAPREDO_SORTER_BUFFER_H

#include <cstring>
#include <vector>
#include <iostream>

#include "lookup.h"

class sorter_buffer
{
public:
    sorter_buffer (const size_t bytes_available, const double ratio);
    ~sorter_buffer();

    /** @return pointer to buffer */
    char*& buffer() {return _buffer;}
    /** @return buffer size in bytes */
    size_t buffer_size() const {return _buffer_size;}
    /** @return number of bytes used in buffer */
    size_t& buffer_used()  {return _buffer_used;}

    /** @return a reference to a vector of pointers to keyvalues */
    std::vector<struct lookup>& lookup() {return _lookup;}
    /** @return size of lookup vector in elements */
    size_t lookup_size() const {return _lookup_size;}
    /** @return number of elements used in lookup vector */
    size_t lookup_used() const {return _lookup_used;}

    bool would_overflow (const size_t bytes) const {
	return (_lookup_used == _lookup_size
		|| _buffer_used + bytes >= _buffer_size);
    }
    bool empty() const {return _buffer_used == 0;}
    void clear() {_buffer_used = _lookup_used = 0;}

    void add (const char* keyvalue, const size_t totalsize) {
	//std::cout << std::string(keyvalue, totalsize);
	memcpy (&_buffer[_buffer_used], keyvalue, totalsize);
	_lookup[_lookup_used].set_ptr (&_buffer[_buffer_used]);
	_buffer_used += totalsize + 1;
	_buffer[_buffer_used - 1] = '\n';
	++_lookup_used;
    }

    /** @return true if buffer size vs lookup vector size is tuned */
    bool tuned() const {return _tuned;}
    /**
     * Tune size of buffer vs lookup vector
     * @param ratio ratio of buffer size vs lookup size in memory use
     */
    void tune (const double ratio);
    /** Get the current ratio between buffer and lookup vector sizes */
    double ratio() const {return _ratio;}
    /** Get the used ratio between buffer and lookup vector */
    double ideal_ratio() const;
    
    sorter_buffer (sorter_buffer&& other);
    sorter_buffer (const sorter_buffer&) = delete;

private:
    size_t _bytes_available;

    char* _buffer = nullptr;
    size_t _buffer_size;
    size_t _buffer_used = 0;
    std::vector<struct lookup> _lookup;
    size_t _lookup_size;
    size_t _lookup_used = 0;

    double _ratio;
    bool _tuned = false;
};

#endif
