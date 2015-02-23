/*
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

#include <sys/stat.h>
#include <unistd.h>
#include <cstring>

#include <stdexcept>

#include "directory.h"



#include <iostream>

directory::directory (const std::string& path) :
    _dirname (path)
{}

void
directory::create (const std::string& path)
{
    if (mkdir (path.c_str(), 0777) < 0)
    {
	char err[80];

	throw std::runtime_error("Can not create " + path + ": "
				 + strerror_r(errno, err, sizeof(err)));
    }
}

bool
directory::exists (const std::string& path)
{
    struct stat st;

    return (stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode));
}

bool
directory::remove (const std::string& path,
		   const bool with_files,
		   const bool recursive)
{
    if (!with_files && !recursive)
    {
	if (rmdir(path.c_str()) < 0)
	{
	    if (errno == ENOENT) return false;
		
	    char err[80];

	    throw std::runtime_error
		("Can not remove " + path + ": "
		 + strerror_r(errno, err, sizeof(err)));
	}
	return true;
    }

    DIR* dir = opendir (path.c_str());

    if (dir)
    {
	struct dirent entry;
	struct dirent* result;
	struct stat st;

	while (readdir_r(dir, &entry, &result) == 0 && result)
	{
	    if (strcmp (result->d_name, ".") == 0
		|| strcmp (result->d_name, "..") == 0)
	    {
		continue;
	    }

	    std::string fname (path + "/" + result->d_name);
	    if (lstat(fname.c_str(), &st) < 0)
	    {
		char err[80];

		throw std::runtime_error
		    ("Can not stat " + fname + ": "
		     + strerror_r(errno, err, sizeof(err)));
	    }
	    if (S_ISDIR(st.st_mode))
	    {
		if (recursive)
		{
		    remove (fname, true);
		    continue;
		}
		else throw std::runtime_error
			 ("Directory " + path + " contains subdirectories"
			  + " and cannot be removed");
	    }

	    if (unlink(fname.c_str()) < 0)
	    {
		char err[80];

		throw std::runtime_error
		    ("Can not remove " + fname + ": "
		     + strerror_r(errno, err, sizeof(err)));
	    }
	}
	closedir (dir);
	if (rmdir(path.c_str()) < 0)
	{
	    char err[80];

	    throw std::runtime_error
		("Can not remove " + path + ": "
		 + strerror_r(errno, err, sizeof(err)));
	}
	return true;
    }

    if (errno != ENOENT)
    {
	char err[80];

	throw std::runtime_error("Can not remove " + path + ": "
				 + strerror_r(errno, err, sizeof(err)));
    }
    return false;
}

directory::const_iterator::const_iterator (const std::string& path) :
    _path (path),
    _dir (opendir(path.c_str()))
{
    if (!_dir)
    {
	char err[80];

	throw std::runtime_error("Can not open " + path + ": "
				 + strerror_r(errno, err, sizeof(err)));
    }
    get_next_file();
}

directory::const_iterator::~const_iterator()
{
    if (_dir) closedir (_dir);
}

const char*
directory::const_iterator::operator*()
{
    if (_result) return _result->d_name;
    return "";
}

const directory::const_iterator&
directory::const_iterator::operator++()
{
    get_next_file();
    return *this;
}

bool
directory::const_iterator::operator!=(const const_iterator& other) const
{
    if (!_result) return (other._result);
    if (!other._result) return true;
    return _result != other._result;
}

bool
directory::const_iterator::get_next_file()
{
    int retval;

    do
    {
	retval = readdir_r(_dir, &_entry, &_result);
    }
    while (retval == 0
	   && _result
	   && _result->d_name[0] == '.');
    if (retval == 0) return _result;

    char err[80];

    throw std::runtime_error ("Can not access " + _path + ": "
			      + strerror_r(errno, err, sizeof(err)));
}
