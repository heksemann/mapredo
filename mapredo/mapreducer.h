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
	enum sortmode
	{
	    TEXT,
	    RTEXT,
	    INT32,
	    RINT32,
	    INT64,
	    RINT64,
	    DOUBLE,
	    RDOUBLE
	};

	mapreducer (const bool reverse = false) :
	_reverse(reverse), _numeric(false) {
	    static_assert (std::is_same<T,std::string>::value
			   || std::is_same<T,int64_t>::value
			   || std::is_same<T,double>::value,
			   "Key needs to be string, int64_t or double");
	    if (std::is_same<T,std::string>::value)
	    {
		set_type (keytype::STRING);
	    }
	    if (std::is_same<T,int64_t>::value)
	    {
		set_type (keytype::INT64);
	    }
	    if (std::is_same<T,double>::value)
	    {
		set_type (keytype::DOUBLE);
	    }
	}
	virtual ~mapreducer() {}

	void sorting (sortmode& from_map, sortmode& from_reduce);

	/**
	 * Reduce function.
	 * @param key key id.
	 * @param values list of nul-terminated char* values that can be
	 *               iterated over.
	 * @param collector used 0 or more times to output reduce results.
	 */
	virtual void reduce (T key, const vlist& values,
			     collector& output) = 0;

	bool reverse() const {return _reverse;}
	bool numeric() const {return _numeric;}
    private:
	const bool _reverse;
	const bool _numeric;
    };
}

/// This macro needs to be used exactly once in the map-reducer.
#define MAPREDO_FACTORIES(t) \
    extern "C" mapredo::base* create() {return new t;} \
    extern "C" void destroy(mapredo::base* p) {delete p;}

#endif
