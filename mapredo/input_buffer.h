/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_INPUT_BUFFER_H
#define _HEXTREME_MAPREDO_INPUT_BUFFER_H

#include <memory>

class input_buffer
{
public:
    input_buffer (const size_t bytes) :
	_buf(new char[bytes+1]), _capacity (bytes) {}

    /** Get pointer to correctly sized buffer */
    char* get() {return _buf.get();}

    /** Get buffer capacity in bytes */
    size_t capacity() const {return _capacity;}

    /**
     * Increase buffer capacity.  The actual capacity reserved may be
     * bigger than the requested number of bytes.
     * @param bytes number of bytes to fit in buffer
     */
    void increase_capacity (size_t bytes) {
	abort();
    }

    /** Start of buffer */
    size_t& start() {return _start;}

    /** End of buffer */
    size_t& end() {return _end;}

private:
    std::unique_ptr<char[]> _buf;
    size_t _capacity;
    size_t _start = 0;
    size_t _end = 0;
};

#endif
