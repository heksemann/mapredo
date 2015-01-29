/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_INPUT_BUFFER_H
#define _HEXTREME_MAPREDO_INPUT_BUFFER_H

#include <memory>

class input_buffer
{
public:
    input_buffer (const size_t bytes) : _buf(new char[bytes]) {}

    /** Get pointer to correctly sized buffer */
    char* buf() {return _buf.get();}

    /** Start of buffer */
    size_t& start() {return _start;}

    /** End of buffer */
    size_t& end() {return _end;}

private:
    std::unique_ptr<char[]> _buf;
    size_t _start = 0;
    size_t _end = 0;
};

#endif
