#ifdef _WIN32

#include "directory_win32.h"

directory::directory (const std::string& path) :
    _dirname (path)
{}

void
directory::create (const std::string& path)
{
    auto create_path = std::tr2::sys::path(path);
    try
    {
	std::tr2::sys::create_directory (create_path);
    }
    catch (const std::tr2::sys::filesystem_error& e)
    {
	throw std::runtime_error("Can not create " + path + ": "
				 + e.what());
    }
}

bool
directory::exists (const std::string& path)
{
    auto check_path = std::tr2::sys::path(path);
    auto stat = std::tr2::sys::status (check_path);
    return (std::tr2::sys::exists (stat) 
	    && std::tr2::sys::is_directory (stat));
}

bool
directory::remove (const std::string& path,
		   const bool with_files,
		   const bool recursive)
{
    auto delete_path = std::tr2::sys::path (path);

    if (!exists (path))
    {
	return false;
    }
    if (recursive)
    {
	std::tr2::sys::remove_all (delete_path);
	return true;
    }
    if(with_files)
    {
	for (auto it = std::tr2::sys::directory_iterator(delete_path);
	     it != std::tr2::sys::directory_iterator(); ++it)
	{
	    const auto& file = it->path ();
	    auto delete_file = delete_path / file;
	    if (!std::tr2::sys::remove_filename (delete_file))
	    {
		throw std::runtime_error
		    ("Can not remove file " + delete_file.filename ());
	    }
	}
    }
    if (!std::tr2::sys::remove_directory (delete_path))
    {
	throw std::runtime_error ("Can not remove " + path);
    }
    return true;
}

directory::const_iterator::const_iterator (const std::string& path)
{
    _path = std::tr2::sys::path (path);
    if (!exists (_path))
    {
	throw std::runtime_error("Can not open " + path
	    + " as is does not exist");
    }
    _dir = std::tr2::sys::directory_iterator (_path);
}

directory::const_iterator::~const_iterator()
{
    if (_dir) closedir (_dir);
}

const char*
directory::const_iterator::operator*()
{
    return _dir->path().filename().c_str();
}

const directory::const_iterator&
directory::const_iterator::operator++()
{
    _dir++;
    return *this;
}

bool
directory::const_iterator::operator!=(const const_iterator& other) const
{
    return _dir != other._dir;
}

#endif
