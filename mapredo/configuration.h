/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_CONFIGURATION_H
#define _HEXTREME_MAPREDO_CONFIGURATION_H

#include <memory>
#include <vector>

#include "config_param.h"

namespace mapredo
{
    /**
     * Configuration class for mapreducers.  This can be used to give
     * mapreducers more flexibility.
     */
    class configuration : public std::vector<std::unique_ptr<config_parameter>>
    {
    public:
	/**
	 * Add a settable configuration parameter for the mapreducer.
	 * @param name the name of the configuration parameter.
	 * @param storage where to store the parameter value.  The
	 *        supported types are bool, std::string, int64_t and
	 *        double.
	 * @param optional whether the parameter needs to be present or not.
	 * @param doc short description of the parameter.
	 * @param single character name (argument switch).
	 */
	template<typename T> void add (const std::string& name,
				       T& storage,
				       const bool optional,
				       const std::string& doc,
				       char shortname = '\0') {
	    std::unique_ptr<config_parameter> param
		(new config_param<T>(name,
				     storage,
				     optional,
				     doc,
				     shortname));
	    push_back (std::move(param));
	}
    };
}    

#endif
