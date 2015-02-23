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

#ifndef _HEXTREME_MAPREDO_CONFIG_PARAM_H
#define _HEXTREME_MAPREDO_CONFIG_PARAM_H

namespace mapredo
{
    /**
     * Base class that holds information about a configuration parameter
     * for a mapreducer.
     */
    class config_parameter
    {
    public:
	config_parameter (const std::string& name,
				 const bool optional,
				 const std::string& doc,
				 char shortname) :
	    _name(name),
	    _optional(optional),
	    _doc(doc),
	    _shortname(shortname) {}
	virtual ~config_parameter() {}
	/**
	 * Set the value of the configuration parameter.
	 * @param value the value to set the parameter to
	 */  
	virtual void set (const std::string& value) = 0;
	
    private:
	const std::string _name;
	const bool _optional;
	const std::string _doc;
	const char _shortname;	
    };
    
    /**
     * Class holds a reference to the object where the configuration is set.
     */
    template <class T> class config_param : public config_parameter
    {
    public:
	/* Constructor for configuration parameter objects */
	config_param (const std::string& name, T& storage, const bool optional,
		      const std::string& doc, char shortname) :
	    config_parameter (name, optional, doc, shortname),
	    _storage(storage) {}
	
	/**
	 * Set the value of the configuration parameter.
	 * @param value the value to set the parameter to
	 */  
	void set (const std::string& value) {
	    std::istringstream stream (value);
	    stream >> _storage;
	}

    private:
	T& _storage;
    };
}

#endif
