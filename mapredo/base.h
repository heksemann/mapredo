/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_BASE_H
#define _HEXTREME_MAPREDO_BASE_H

#include "collector.h"

namespace mapredo
{
    class base
    {
    public:
	enum keytype
	{
	    UNKNOWN,
	    STRING,
	    INT64,
	    DOUBLE
	};

	base() = default;
	virtual ~base() {}

	/**
	 * Map function.
	 * @param line input line, nul-terminated.
	 * @param length input line length in bytes
	 * @param collector used 0 or more times to output map results.
	 */
	virtual void map (char* line, const int length,
			  collector& output) = 0;

	/*
	 * Implement this to return true if reduced() can be called  multiple
	 * times.
	 */
	virtual bool reducer_can_combine() const {return false;}

	void set_type (const keytype type) {_type = type;}
	keytype type() const {return _type;}

    private:
	keytype _type = UNKNOWN;
    };
}

#endif
