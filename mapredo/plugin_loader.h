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

#ifndef _HEXTREME_MAPREDO_PLUGIN_LOADER_H
#define _HEXTREME_MAPREDO_PLUGIN_LOADER_H

#include <dlfcn.h>
#include <sys/stat.h>
#include <iostream>

#include "base.h"

/** Used to load mapreducer plugins dynamically */
class plugin_loader
{
public:
    /**
     * Create a loader able to provide new map-reduce objects from a
     * plugin module.
     * @param plugin_path absolute or relative path to .so module file
     */
    plugin_loader (std::string plugin_path) {
	if (plugin_path.size() < 4
	    || plugin_path.substr(plugin_path.size() - 3) != ".so")
	{
	    plugin_path += ".so";
	}

	if (!load(plugin_path)
	    && !load("plugins/" + plugin_path)
	    && !load(PLUGINSDIR + std::string("/") + plugin_path))
	{
	    throw std::runtime_error ("Unable to find mapreduce plugin "
				      + plugin_path);
	}
    }

    ~plugin_loader() {
	typedef void (*destroy_t)(mapredo::base*);

#ifdef __GNUC__
	__extension__
#endif
	auto destroyer = (destroy_t)dlsym(_lib, "destroy");
	auto err = dlerror();
	if (err)
	{
	    std::cerr << "Can not unload plugin: " << err << "\n";
	}
	else
	{
	    for (auto* obj: _mapred)
	    {
		destroyer (obj);
	    }
	}

	dlclose (_lib);
    }

    /**
     * Get a new mapreducer object.  The object is automatically destroyed
     * with the plugin loader object.
     */
    mapredo::base& get() {
	auto* mapred = _creator();
	if (!mapred)
	{
	    throw std::runtime_error ("The create() function of \"" + _path 
				      + "\" returned a null pointer");
	}
	_mapred.push_back (mapred);
	return *mapred;
    }

private:
    typedef mapredo::base* (*create_t)();

    bool load (const std::string& path) {
	struct stat st;

	if (stat(path.c_str(), &st) == 0)
	{
	    _lib = dlopen (path.c_str(), RTLD_LAZY);
	    if (!_lib)
	    {
		throw std::runtime_error (std::string("Can not load plugin: ")
					  + dlerror());
	    }
	    dlerror(); // reset error

#ifdef __GNUC__
	    __extension__
#endif
	    _creator = (create_t)dlsym(_lib, "create");
	    auto err = dlerror();
	    if (err)
	    {
		dlclose (_lib);
		throw std::runtime_error (std::string("Can not load plugin: ")
					  + err);
	    }
	    _path = path;
	    return true;
	}
	else return false;
    }

    create_t _creator;
    std::vector<mapredo::base*> _mapred;
    void* _lib;
    std::string _path;
};

#endif
