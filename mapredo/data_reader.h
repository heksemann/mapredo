/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_DATA_READER_H
#define _HEXTREME_MAPREDO_DATA_READER_H

#include <type_traits>
#include <iostream>
#include <stdexcept>
#include <queue>

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
	if (_keylen == 0)
	{
	    fill_next_key();
	    if (_keylen > 0) return &_key;
	}
	return nullptr;
    }

    /**
     * Get the next value, null terminated.  Remember to call
     * next_key() before calling this function.
     * @returns null terminated pointer to next value field or nullptr.
     */
    char* get_next_value();

    /**
     * Get the next line (keyvalue) from the file.
     * @param size set to the size of the keyvalue field.
     * @returns pointer to next keyvalue field or nullptr.
     */
    const char* get_next_line (size_t& length);

    /**
     * Get a copy of the next key.  In the char* case, this is only
     * valid until the next call to this function.  It must not be
     * deleted by the caller.
     */
    T get_key_copy() {
	const T* key (next_key());

	if (key) return prepare_key_copy (*key);
	else throw std::runtime_error ("get_key_copy() without next_key()");
    }

    /**
     * This is used to compare data_reader objects in
     * priority queues.
     */
    template <class U> struct drq_compare
    {
	/** The key inside the objects pointed to is compared */
	template<class V = U,
		 typename std::enable_if<std::is_fundamental<V>::value>
		 ::type* = nullptr>
	bool operator()(data_reader<U>* dr1, data_reader<U>* dr2) {
	    return *dr1->next_key() > *dr2->next_key();
	}

	/** The key inside the objects pointed to is compared */
	template<class V = U,
		 typename std::enable_if<std::is_same<V,char*>::value,
					 bool>::type* = nullptr>
	bool operator()(data_reader<U>* dr1, data_reader<U>* dr2) {
	    //std::cerr<< *dr1->next_key()<< " vs " << *dr2->next_key() <<"\n";
	    return strcmp (*dr1->next_key(), *dr2->next_key());
	}
    };
    /** The priority_queue is used when traversing files during merge */
    typedef std::priority_queue<data_reader<T>*,
				std::vector<data_reader<T>*>,
				drq_compare<T>> queue;

    /** Comparison with a key, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_fundamental<U>::value>::type*
	     = nullptr>
    bool operator==(const T key) {
	return next_key() && _key == key;
    }

    /** Comparison with a key, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,
				     bool>::type* = nullptr>
    bool operator==(const char* const key) {
	if (!next_key()) return false;
	
	int i;
	//std::cerr << "Comparing '" << _key << "' vs '" << key << "'\n";
	for (i = 0; i < _keylen && key[i] != '\0'; i++)
	{
	    if (_key[i] != key[i]) return false;
	}
	return i == _keylen && key[i] == '\0';
    }

    /** Comparison with other object, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_fundamental<U>::value>::type*
	     = nullptr>
    int compare (const data_reader& other) {
	if (_key == other._key) return 0;
	if (_key < other._key) return -1;
	return 1;
    }
    
    /** Comparison with other object, used when traversing files during merge */
    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,
				     bool>::type* = nullptr>
    int compare (const data_reader& other) {
	return strcmp (_key, other._key);
    }
    
protected:
    data_reader() {
	static_assert (std::is_same<T,char*>::value
                       || std::is_same<T,int64_t>::value
		       || std::is_same<T,double>::value,
                       "Only char*, int64_t and double keys are supported");
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
    void set_key (T& key, const char* const buf) {
	key = atof (buf);
    }

    template<class U = T,
	     typename std::enable_if<std::is_integral<U>::value>::type*
	     = nullptr>
    void set_key (T& key, const char* const buf) {
	key = atol (buf);
    }

    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,
				     bool>::type* = nullptr>
    void set_key (char*& key, char* const buf) {
	key = buf;
    }

    template<class U = T,
	     typename std::enable_if<std::is_fundamental<U>::value>::type*
	     = nullptr>
    T prepare_key_copy (const T key) {return _key;}

    template<class U = T,
	     typename std::enable_if<std::is_same<U,char*>::value,bool>::type*
	     = nullptr>
    T prepare_key_copy (const char* const key) {
	if (_keylen >= _alloclen)
	{
	    if (_alloclen > 0)
	    {
		delete[] _key_copy;
		do _keylen *= 2;
		while (_alloclen < _keylen);
	    }
	    else
	    {
		_alloclen = 128;
		while (_alloclen < _keylen) _alloclen *= 2;
	    }
	    _key_copy = new char[_alloclen];
	}
	memcpy (_key_copy, _key, _keylen);
	_key_copy[_keylen] = '\0';
	return _key_copy;
    }

    T _key_copy;
    int _alloclen = 0;
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
    set_key (_key, _buffer + _start_pos);
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
