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
	    if (std::is_same<T,char*>::value)
	    {
		set_type (keytype::STRING);
	    }
	    else if (std::is_same<T,int64_t>::value)
	    {
		set_type (keytype::INT64);
	    }
	    else set_type (keytype::DOUBLE);
	}
	virtual ~mapreducer() {}

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
