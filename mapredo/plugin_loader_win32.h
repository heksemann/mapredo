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

#ifndef _HEXTREME_MAPREDO_PLUGIN_LOADER_WIN32_H
#define _HEXTREME_MAPREDO_PLUGIN_LOADER_WIN32_H

#ifdef _WIN32

#define NOMINMAX
#include <windows.h>
#include <filesystem>
#include <iostream>

#include "base.h"

class plugin_loader
{
public:
    /**
     * Create a loader able to provide new map-reduce objects from a
     * plugin module.
     * @param plugin_path absolute or relative path to .dll module file
     */
    plugin_loader (std::string plugin_path) {
	if (plugin_path.size() < 5
	    || plugin_path.substr(plugin_path.size() - 4) != ".dll")
	{
	    plugin_path += ".dll";
	}

	if (!load(plugin_path))
	{
	    throw std::runtime_error ("Unable to find mapreduce plugin "
				      + plugin_path);
	}
    }

    ~plugin_loader() {
	typedef void (*destroy_t)(mapredo::base*);
	auto destroyer = (destroy_t)GetProcAddress(_lib, "destroy");
	auto err = GetLastError();
	if (destroyer == nullptr)
	{
	    TCHAR msg[1024];
	    FormatMessage (FORMAT_MESSAGE_FROM_SYSTEM
		| FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL,
		err,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		msg,
		1024,
		NULL);
	    std::cerr << "Can not unload plugin: " << msg << "\n";
	}
	else
	{
	    for (auto* obj: _mapred)
	    {
		destroyer (obj);
	    }
	}

	FreeLibrary (_lib);
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
	const size_t error_msg_size = 1024;
	TCHAR error_msg[error_msg_size];
	std::tr2::sys::path load_path (path);

	if (std::tr2::sys::exists (load_path))
	{
	    SetErrorMode (SEM_FAILCRITICALERRORS);
	    _lib = LoadLibrary (path.c_str());
	    auto load_error = GetLastError();
	    if (_lib == NULL)
	    {
		FormatMessage (FORMAT_MESSAGE_FROM_SYSTEM
		    | FORMAT_MESSAGE_IGNORE_INSERTS,
		    NULL,
		    load_error,
		    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		    error_msg,
		    error_msg_size,
		    NULL);
		throw std::runtime_error (
		    std::string("Can not load plugin '" + path + "': ")
					  + error_msg);
	    }
	    SetErrorMode(0);

	    _creator = (create_t)GetProcAddress(_lib, "create");
	    load_error = GetLastError();
	    if (_creator == nullptr)
	    {
		FreeLibrary (_lib);
		FormatMessage (FORMAT_MESSAGE_FROM_SYSTEM
		    | FORMAT_MESSAGE_IGNORE_INSERTS,
		    NULL,
		    load_error,
		    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		    error_msg,
		    error_msg_size,
		    NULL);
		throw std::runtime_error (
		    std::string("Can not create plugin '" + path + ": ")
					  + error_msg);
	    }
	    _path = path;
	    return true;
	}
	else return false;
    }

    create_t _creator;
    std::vector<mapredo::base*> _mapred;
    HMODULE _lib;
    std::string _path;
};

#endif
#endif
