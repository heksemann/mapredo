/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_BASE_H
#define _HEXTREME_MAPREDO_BASE_H

#include "collector.h"
#include "configuration.h"

namespace mapredo
{
    /**
     * Base class for all map-reducers
     */
    class base
    {
    public:
	virtual ~base() {}

	/** The different datatypes supported for of key sorting */
	enum keytype
	{
	    UNKNOWN, /// the type is not set yet
	    STRING,  /// char*
	    INT64,   /// 64 bit integer
	    DOUBLE   /// double precision float
	};

	/**
	 * Map function.
	 * @param line input line, nul-terminated.
	 * @param length input line length in bytes
	 * @param collector used 0 or more times to output map results.
	 */
	virtual void map (char* line, const int length,
			  collector& output) = 0;

	/**
	 * Function used to reduce data from mapper.
	 * @return true if reduced() can be used as a combiner (can be
	 *              called again on its own output).
	 */
	virtual bool reducer_can_combine() const {return false;}

	/** @returns the key datatype for this mapreducer */
	keytype type() const {return _type;}

	/**
	 * If configuration arguments are needed for the map-reducer,
	 * override this function.
	 * @param args configuration object where the arguments are defined
	 */
	virtual void setup_configuration (configuration& args) {}

	/**
	 * Any pre-processing based on the configuration parameters
	 * defined in setup_configuration() is done here.  If you just
	 * need the simple configuration parameters, there is no
	 * reason to override configure().
	 */
	virtual void configure() {}

	/**
	 * Fast integer to ascii convertor from Andrei Alexandrescu.  This
	 * may be handy inside your reducer.
	 */
	size_t uint_to_ascii (uint64_t value, char* dst);

    protected:
	base() = default;

	/**
	 * This function is used by the mapreducer class, and must not
	 * be called from any inherited class.
	 * @param type datatype for key
	 */
	void set_type (const keytype type) {_type = type;}

    private:
	keytype _type = UNKNOWN;
    };
}

#endif
