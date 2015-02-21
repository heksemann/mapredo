/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_LOOKUP_H
#define _HEXTREME_MAPREDO_LOOKUP_H

/**
 * Class used in an array to enable sorting of data.
 */
struct lookup
{
    /** @return the key and value of the entry */
    const char* keyvalue() const {return _keyvalue;}
    /** Set the key and value of the entry */
    void set_ptr (const char* keyvalue,
		  const uint16_t keylen,
		  const uint16_t totallen) {
	_keyvalue = keyvalue;
	_keylen = keylen;
	_totallen = totallen;
    }
    /**
     * Operator used when sorting the array
     * @param the array element to compare this with
     */
    bool operator< (const lookup& right) const {
	uint16_t len = std::min(_keylen, right._keylen);

	for (uint16_t i = 0; i < len; i++)
	{
	    int l = _keyvalue[i];
	    int r = right._keyvalue[i];

	    if (l < r)
	    {
#if 0
		std::cerr << '"' << std::string(_keyvalue, _keylen)
			  << "\" < \""
			  << std::string(right._keyvalue, right._keylen)
			  << "\" by value\n";
#endif
		return true;
	    }
	    if (l > r)
	    {
#if 0
		std::cerr << '"' << std::string(_keyvalue, _keylen)
			  << "\" > \""
			  << std::string(right._keyvalue, right._keylen)
			  << "\" by value (" << i << "), "
			  << "\n";
#endif
		return false;
	    }
	}
#if 0
	std::cerr << '"' << std::string(_keyvalue, _keylen) << '"'
		  << (_keylen < right._keylen ? " < " : " >= ") << '"'
		  << std::string(right._keyvalue, right._keylen)
		  << "\"\n";
#endif
	return _keylen < right._keylen;
    }

    uint32_t size() const {return _totallen;}
    
private:
    const char* _keyvalue;
    uint16_t _keylen;
    uint32_t _totallen;
};

#endif
