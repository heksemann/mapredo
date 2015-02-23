/* -*- C++ -*-
 * mapredo
 * Copyright (C) 2015 Kjell Irgens <hextremist@gmail.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 */

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
