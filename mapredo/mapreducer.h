/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_MAPREDUCER_H
#define _HEXTREME_MAPREDO_MAPREDUCER_H

#include "base.h"
#include "valuelist.h"

namespace mapredo
{
    /**
     * Base class for mapreducers
     */
    template <class T> class mapreducer : public base
    {
    public:
	typedef valuelist<T> vlist;

	mapreducer (const bool reverse = false) :
	_reverse(reverse), _numeric(false) {
	    static_assert (std::is_same<T,char*>::value
			   || std::is_same<T,int64_t>::value
			   || std::is_same<T,double>::value,
			   "Key needs to be char*, int64_t or double");
	    if (std::is_same<T,char*>::value) set_type (keytype::STRING);
	    else if (std::is_same<T,int64_t>::value) set_type (keytype::INT64);
	    else set_type (keytype::DOUBLE);
	}
	virtual ~mapreducer() {if (_buffer) delete[] _buffer;}

	/**
	 * Reduce function.
	 * @param key key id.
	 * @param values list of nul-terminated char* values that can be
	 *               iterated over.
	 * @param collector used 0 or more times to output reduce results.
	 */
	virtual void reduce (T key, vlist& values, collector& output) = 0;

	bool reverse() const {return _reverse;}
	bool numeric() const {return _numeric;}

    protected:
	/**
	 * Get a pointer to a buffer that may be used for output from
	 * both the map() or reduce() functions.  If the buffer is
	 * used, this function should be called every time; a
	 * different buffer may be returned as the size is increased.
	 * @param size the minimum size of the allocated buffer
	 * @param realloc if true, copy over the data from the previous
	 *                buffer when a new allocation is needed.
	 */
	char* output_buffer (const size_t size, const bool realloc = false) {
	    if (size > _buffer_size)
	    {
		if (realloc)
		{
		    if (size < _prev_size)
		    {
			throw std::runtime_error
			    ("output_buffer() can not be used  with decreased"
			     " buffer size and realloc flag");
		    }

		    if (!_buffer) _buffer_size = 128;
		    while (size > _buffer_size) _buffer_size *= 2;
		    if (_prev_size > 0)
		    {
			char* oldbuffer = _buffer;
			_buffer = new char[_buffer_size];
			memcpy (_buffer, oldbuffer, _prev_size);
			delete oldbuffer;
		    }
		    else _buffer = new char[_buffer_size];
		    _prev_size = size;
		}
		else
		{
		    if (_buffer) delete[] _buffer;
		    else _buffer_size = 128;
		    while (size > _buffer_size) _buffer_size *= 2;
		    _buffer = new char[_buffer_size];
		}
	    }
	    return _buffer;
	}

    private:
	const bool _reverse;
	const bool _numeric;
	char* _buffer = nullptr;
	size_t _buffer_size = 0;
	size_t _prev_size = 0;
    };
}

/// This macro needs to be used exactly once in the map-reducer.
#define MAPREDO_FACTORIES(t) \
    extern "C" mapredo::base* create() {return new t;} \
    extern "C" void destroy(mapredo::base* p) {delete p;}

#endif
