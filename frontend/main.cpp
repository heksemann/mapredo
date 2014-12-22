
#ifndef _WIN32
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>
#include <dlfcn.h>
#else
#include <windows.h>
#endif
#include <chrono>
#include <iostream>
#include <thread>
#include <stdexcept>
#include <tclap/CmdLine.h>

#include <mapredo/settings.h>
#include <mapredo/engine.h>
#include <mapredo/base.h>
#ifndef _WIN32
#include <mapredo/plugin_loader.h>
#include <mapredo/directory.h>
#else
#include <mapredo/plugin_loader_win32.h>
#include <mapredo/directory_win32.h>
typedef DWORD ssize_t;
#endif

static double time_since (const std::chrono::high_resolution_clock::time_point& time)
{
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>
	(std::chrono::high_resolution_clock::now() - time);
    return duration.count();
}

static void reduce (const std::string& plugin_file,
		    const std::string& work_dir,
		    const std::string& subdir,
		    const bool verbose,
		    const int parallel,
		    const int max_files)
{
    if (verbose)
    {
	std::cerr << "Using working directory " << work_dir << "\n";
	std::cerr << "Using " << parallel << " threads,"
		  << " with HW concurrency at "
		  << std::thread::hardware_concurrency()
		  << "\n";
    }

    plugin_loader plugin (plugin_file);
    auto& mapreducer (plugin.get());
    engine mapred_engine (work_dir, subdir, parallel, 0, max_files);
    auto start_time = std::chrono::high_resolution_clock::now();

    mapred_engine.reduce (mapreducer, plugin);

    if (verbose)
    {
	std::cerr << "Merging finished in " << std::fixed
		  << time_since (start_time) << "s\n";
    }
}

static void run (const std::string& plugin_file,
		 const std::string& work_dir,
		 const std::string& subdir,
		 const bool verbose,
		 const size_t buffer_size,
		 const int parallel,
		 const int max_files,
		 const bool map_only)
{
    plugin_loader plugin (plugin_file);
    auto& mapreducer (plugin.get());
    engine mapred_engine (work_dir, subdir, parallel, buffer_size, max_files);
    size_t buf_size = 0x2000;
    std::unique_ptr<char[]> buf (new char[buf_size]);
    size_t start = 0, end = 0;
    size_t i;
    ssize_t bytes;
    std::string line;
    bool first = true;
    std::chrono::high_resolution_clock::time_point start_time;

#ifdef _WIN32
    auto STDIN_FILENO = GetStdHandle (STD_INPUT_HANDLE);
    while ((ReadFile (STDIN_FILENO, buf.get() + end, 
	    buf_size - end, &bytes, NULL) && bytes > 0))
#else
    while ((bytes = read(STDIN_FILENO, buf.get() + end, buf_size - end)) > 0)
#endif
    {
	end += bytes;
	if (first)
	{
	    first = false;

	    // Skip any Windows style UTF-8 header
	    unsigned char u8header[] = {0xef, 0xbb, 0xbf};
	    if (end - start >= 3 && memcmp(buf.get() + start, u8header, 3) == 0)
	    {
		start += 3;
	    }
	    start_time = std::chrono::high_resolution_clock::now();
	    mapred_engine.enable_sorters (mapreducer.type(),
					  false);
	    if (verbose)
	    {
		std::cerr << "Using working directory " << work_dir << "\n";
		std::cerr << "Maximum buffer size is " << buffer_size
			  << " bytes.\nUsing " << parallel << " threads,"
			  << " with HW concurrency at "
			  << std::thread::hardware_concurrency()
			  << "\n";
	    }
	}
	for (i = start; i < end; i++)
	{
	    if (buf[i] == '\n')
	    {
		if (i == start || buf[i-1] != '\r')
		{
		    buf[i] = '\0';
		    mapreducer.map(buf.get() + start, i - start, mapred_engine);
		}
		else
		{
		    buf[i-1] = '\0';
		    mapreducer.map (buf.get() + start, i - start - 1,
				    mapred_engine);
		}
		start = i + 1;
	    }
	}
	if (start < i)
	{
	    if (start == 0)
	    {
		buf_size *= 2; // double line buffer
		char* nbuf = new char[buf_size];
		memcpy (nbuf, buf.get(), end);
		buf.reset (nbuf);
	    }
	    memmove (buf.get(), buf.get() + start, end - start);
	    end -= start;
	    start = 0;
	}
	else start = end = 0;
    }
    if (first) return; // no input

    if (verbose)
    {
	std::cerr << "Sorting finished in " << std::fixed
		  << time_since (start_time) << "s\n";
    }

    if (!map_only)
    {
	mapred_engine.flush (mapreducer, plugin);
	if (verbose)
	{
	    std::cerr << "Merging finished in " << std::fixed
		      << time_since (start_time) << "s\n";
	}
    }
    else mapred_engine.flush();
}

static std::string get_default_workdir()
{
    return TMPDIR;
}

int
main (int argc, char* argv[])
{
    int64_t buffer_size = 10 * 1024 * 1024;
    int parallel = std::thread::hardware_concurrency();
    int max_files = 20 * parallel;
    bool verbose = false;
    bool compression = true;
    std::string subdir;
    std::string work_dir = get_default_workdir();
    bool map_only = false;
    bool reduce_only = false;
    bool keep_tmpfiles = false;
    std::string plugin_path;

    settings& config (settings::instance());

    try
    {
	TCLAP::CmdLine cmd ("mapredo: Map-reduce engine for small/medium data",
			    ' ', PACKAGE_VERSION);
	TCLAP::ValueArg<std::string> subdirArg
	    ("s", "subdir", "Subdirectory to use",
	     false, "", "string", cmd);
	TCLAP::ValueArg<std::string> workdirArg
	    ("d", "work-dir", "Working directory to use",
	     false, work_dir, "string", cmd);
	TCLAP::ValueArg<std::string> bufferSizeArg
	    ("b", "buffer-size", "Buffer size to use",
	     false, "10M", "size", cmd);
	TCLAP::ValueArg<int> maxFilesArg
	    ("f", "max-open-files", "Maximum number of open files",
	     false, max_files, "number", cmd);
	TCLAP::ValueArg<int> threadsArg
	    ("j", "threads", "Number of threads to use",
	     false, parallel, "threads", cmd);
	TCLAP::SwitchArg verboseArg
	    ("", "verbose", "Verbose output", cmd, false);
	TCLAP::SwitchArg noCompressionArg
	    ("", "no-compression", "Disable compression", cmd, false);
	TCLAP::SwitchArg keepFilesArg
	    ("", "keep-tmpfiles", "Keep the temporary files after completion",
	     cmd, false);
	TCLAP::SwitchArg mapOnlyArg
	    ("", "map-only", "Only perform the mapping stage", cmd, false);
	TCLAP::SwitchArg reduceOnlyArg
	    ("", "reduce-only", "Only perform the reduce stage", cmd, false);
	TCLAP::UnlabeledValueArg<std::string> pluginArg
	    ("plugin", "Plugin file to use", true, "", "plugin file", cmd);

	cmd.parse (argc, argv);
	subdir = subdirArg.getValue();
	work_dir = workdirArg.getValue();
	buffer_size = config.parse_size (bufferSizeArg.getValue());
	max_files = maxFilesArg.getValue();
	parallel = threadsArg.getValue();
	verbose = verboseArg.getValue();
	keep_tmpfiles = keepFilesArg.getValue();
	map_only = mapOnlyArg.getValue();
	reduce_only = reduceOnlyArg.getValue();
	plugin_path = pluginArg.getValue();

	if (!directory::exists(work_dir))
	{
	    throw TCLAP::ArgException
		("The working directory '" + work_dir + "' does not exist",
		 "work-dir");
	}
	if (max_files < 3)
	{
	    throw TCLAP::ArgException
		("Can not work with less than 3 files", "max-open-files");
	}

	if (reduce_only && map_only)
	{
	    throw TCLAP::ArgException
		("Options --map-only and --reduce-only are mutually exclusive",
		 "map_only");
	}
	if (map_only && subdir.empty())
	{
	    throw TCLAP::ArgException
		("Option --map-only cannot be used without --subdir",
		 "map-only");
	}
	if (reduce_only && subdir.empty())
	{
	    throw TCLAP::ArgException
		("Option --reduce-only cannot be used without --subdir",
		 "reduce-only");
	}
    }
    catch (const TCLAP::ArgException& e)
    {
	std::cerr << "error: " << e.error() << " for " << e.argId()
		  << std::endl;
	return (1);
    }

    if (verbose) config.set_verbose();
    if (compression) config.set_compressed();
    if (keep_tmpfiles) config.set_keep_tmpfiles();

    try
    {
	if (reduce_only)
	{
	    reduce (plugin_path, work_dir, subdir, verbose, parallel,
		    max_files);
	}
	else
	{
	    run (plugin_path, work_dir, subdir, verbose, buffer_size,
		 parallel, max_files, map_only);
	}
    }
    catch (const std::exception& e)
    {
	std::cerr << e.what() << std::endl;
	return 1;
    }

    return 0;
}
