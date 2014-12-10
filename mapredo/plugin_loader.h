/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_PLUGIN_LOADER_H
#define _HEXTREME_MAPREDO_PLUGIN_LOADER_H

#include <dlfcn.h>
#include <iostream>

#include "base.h"

class plugin_loader
{
public:
    plugin_loader (std::string plugin_path) {
	if (plugin_path.size() < 4
	    || plugin_path.substr(plugin_path.size() - 3) != ".so")
	{
	    plugin_path += ".so";
	}

	if (!load(plugin_path)
	    && !load(".libs/" + plugin_path)
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
	else destroyer (_mapred);

	dlclose (_lib);
    }

    mapredo::base& get() {
	return *_mapred;
    }
private:
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
	    typedef mapredo::base* (*create_t)();

#ifdef __GNUC__
	    __extension__
#endif
	    auto creator = (create_t)dlsym(_lib, "create");
	    auto err = dlerror();
	    if (err)
	    {
		dlclose (_lib);
		throw std::runtime_error (std::string("Can not load plugin: ")
					  + err);
	    }

	    _mapred = creator();
	    if (!_mapred)
	    {
		dlclose (_lib);
		throw std::runtime_error ("The create() function of \"" + path 
					  + "\" returned a null pointer");
	    }
	    
	    return true;
	}
	else return false;
    }

    mapredo::base* _mapred;
    void* _lib;
};

#endif
