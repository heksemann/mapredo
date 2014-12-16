/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_DATA_READER_H
#define _HEXTREME_MAPREDO_DATA_READER_H

#include <type_traits>
#include <iostream>
#include <stdexcept>

template <class T> class data_reader
{
public:
    virtual ~data_reader() {
	if (_buffer) delete[] _buffer;
    }

    /**
     * Peek at the next key from the data source.
     * @returns pointer to next key if there is more data, nullptr otherwise
     */
    const T* next_key() {
	if (_keylen > 0) return &_key;
	if (_keylen  == 0)
	{
	    fill_next_key();
	    if (_keylen > 0) return &_key;
	}
	return nullptr;
    }
    /**
     * Get the next line (keyvalue) from the file.
     * @param size set to the size of the keyvalue field.
     * @returns pointer to next keyvalue field or nullptr.
     */
    const char* get_next_line (size_t& length);

    /**
     * Get the next value, null terminated.  Remember to check
     * next_key() before calling this function.
     * @returns null terminated pointer to next value field or nullptr.
     */
    char* get_next_value();

protected:
    data_reader() {
	static_assert (std::is_same<T,std::string>::value
                       || std::is_same<T,int64_t>::value
		       || std::is_same<T,double>::value,
                       "Only string, int64_t and double keys are supported");
    }

    /** Prepare next key from buffer */
    void fill_next_key();

    /** Override this if the data source can provide more data. */
    virtual bool read_more() {return false;}

    T _key;
    char* _buffer = nullptr;
    size_t _start_pos = 0;
    size_t _end_pos = 0;
    int _keylen = 0;

private:
    template<class U = T,
	     typename std::enable_if<std::is_floating_point<U>::value>::type*
	     = nullptr>
    void set_key (T& key, char* const buf, const size_t length) {
	key = atof (buf);
    }

    template<class U = T,
	     typename std::enable_if<std::is_integral<U>::value>::type*
	     = nullptr>
    void set_key (T& key, char* const buf, const size_t length) {
	key = atol (buf);
    }

    template<class U = T,
	     typename std::enable_if<std::is_same<U,std::string>::value,
				     bool>::type* = nullptr>
    void set_key (T& key, char* const buf, const size_t length) {
	key = std::string (buf, length);
    }
};

template <class T> void
data_reader<T>::fill_next_key()
{
    if (_start_pos == _end_pos && !read_more())
    {
	_keylen = -1;
	return;
    }

    size_t i;

    for (i = _start_pos; i < _end_pos; i++)
    {
	if (_buffer[i] == '\t' || _buffer[i] == '\n') break;
    }

    if (i == _end_pos)
    {
	if (!read_more())
	{
	    throw std::runtime_error
		("Temporary data does not contain newlines or tabs");
	}
	
	for (i = _start_pos; i < _end_pos; i++)
	{
	    if (_buffer[i] == '\t' || _buffer[i] == '\n') break;
	}

	if (i == _end_pos)
	{
	    throw std::runtime_error
		("Temporary data does not contain newlines or tabs");
	}
    }

    _keylen = i - _start_pos;
    set_key (_key, _buffer + _start_pos, _keylen);
}

template <class T> char*
data_reader<T>::get_next_value()
{
    if (_keylen == 0)
    {
	throw std::runtime_error ("Programming error, keylen is zero in "
				  + std::string(__FUNCTION__) + "()");
    }

    size_t i;

    for (i = _start_pos + _keylen; i < _end_pos; i++)
    {
	//std::cout << "Char: '" << _buffer[i] << "'\n";
	if (_buffer[i] == '\n')
	{
	    char* value (_buffer + _start_pos + _keylen);

	    if (*value == '\t') value++;
	    _buffer[i] = '\0';
	    _start_pos = i + 1;
	    _keylen = 0;
	    return value;
	}
    }
    i -= _start_pos;

    if (!read_more())
    {
	char* value;

	// Premature ending, no newline
	std::cerr << "Premature ending, no newline\n";
	if (_buffer[_start_pos + _keylen] == '\n')
	{
	    value = _buffer + _start_pos + _keylen + 1;
	    _buffer[_end_pos] = '\0';
	}
	else value = const_cast<char*>("");

	_keylen = -1;
	return value;
    }

    for (; i < _end_pos; i++)
    {
	//std::cerr << "Char: '" << _buffer[i] << "'\n";
	if (_buffer[i] == '\n')
	{
	    char* value (_buffer + _start_pos + _keylen);

	    if (*value == '\t') value++;
	    _buffer[i] = '\0';
	    _start_pos = i + 1;
	    _keylen = 0;
	    return value;
	}
    }

    char* value;

    if (_buffer[_start_pos + _keylen] == '\n')
    {
	value = _buffer + _start_pos + _keylen + 1;
	_buffer[_end_pos] = '\0';
    }
    else value = const_cast<char*>("");

    _keylen = 0;
    return value;
}

template <class T> const char*
data_reader<T>::get_next_line (size_t& length)
{
    size_t i;

    for (i = _start_pos + _keylen; i < _end_pos; i++)
    {
	//std::cout << "Char: " << _buffer[i] << "\n";
	if (_buffer[i] == '\n')
	{
	    char* line (_buffer + _start_pos);

	    length = i - _start_pos + 1;
	    _start_pos = i + 1;
	    _keylen = 0;
	    return line;
	}
    }
    i -= _start_pos;

    if (!read_more())
    {
	length = _end_pos - _start_pos;
	_keylen = -1;
	return _buffer + _start_pos;
    }

    for (; i < _end_pos; i++)
    {
	//std::cout << "Char: " << _buffer[i] << "\n";
	if (_buffer[i] == '\n')
	{
	    char* line (_buffer + _start_pos);
	    
	    length = i - _start_pos + 1;
	    _start_pos = i + 1;
	    _keylen = 0;
	    return line;
	}
    }

    length = _end_pos - _start_pos;
    _keylen = 0;
    return _buffer + _start_pos;
}

#endif
