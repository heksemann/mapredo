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
    void set_ptr (const char* keyvalue) {
	_keyvalue = keyvalue;
    }
    /**
     * Operator used when sorting the array
     * @param the array element to compare this with
     */
    bool operator< (const lookup& right) const {
	for (int i = 0;; i++)
	{
	    register int l = _keyvalue[i];
	    register int r = right._keyvalue[i];

	    if ((l == '\t' || l == '\n' || l == '\0')
		&& r != '\t' && r != '\n' && r != '\0')
	    {
#if 0
		std::cout << '"' << std::string(_keyvalue, _length -1)
			  << "\" < \""
			  << std::string(right._keyvalue, right._length -1)
			  << "\" by length\n";
#endif
		return true;
	    }
	    if (r == '\t' || r == '\n' || r == '\0')
	    {
#if 0
		std::cout << std::string(_keyvalue, _length -1) << " >= "
			  << std::string(right._keyvalue, right._length -1)
			  << " by length\n";
#endif
		return false;
	    }
	    if (l < r)
	    {
#if 0
		std::cout << '"' << std::string(_keyvalue, _length -1)
			  << "\" < \""
			  << std::string(right._keyvalue, right._length -1)
			  << "\" by value\n";
#endif
		return true;
	    }
	    if (l > r)
	    {
#if 0
		std::cout << '"' << std::string(_keyvalue, _length -1)
			  << "\" >= \""
			  << std::string(right._keyvalue, right._length -1)
			  << "\" by value (" << i << "), "
			  << _keyvalue[i] << ">=" << right._keyvalue[i]
			  << "\n";
#endif
		return false;
	    }
	}
	return false;
    }
    
private:
    const char* _keyvalue;
};

#endif
