
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
#include <mapredo/buffer_trader.h>
#ifndef _WIN32
#include <mapredo/plugin_loader.h>
#include <mapredo/directory.h>
#else
#include <mapredo/plugin_loader_win32.h>
#include <mapredo/directory_win32.h>
typedef DWORD ssize_t;
#endif

static double duration (std::chrono::high_resolution_clock::time_point& time)
{
    auto now (std::chrono::high_resolution_clock::now());
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>
	(now - time);
    time = now;
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

    engine mapred_engine (plugin_file, work_dir, subdir, parallel, 0,
			  max_files);
    auto start_time (std::chrono::high_resolution_clock::now());

    mapred_engine.reduce_existing_files();

    if (verbose)
    {
	std::cerr << "Merging finished in " << std::fixed
		  << duration (start_time) << "s\n";
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
    engine mapred_engine (plugin_file, work_dir, subdir, parallel, buffer_size,
			  max_files);
    size_t i;
    ssize_t bytes;
    std::string line;
    bool first = true;
    std::chrono::high_resolution_clock::time_point start_time;
    buffer_trader& trader (mapred_engine.prepare_sorting());
    input_buffer* current = trader.producer_get();
    input_buffer* next = trader.producer_get();
    size_t& start (current->start());
    size_t& end (current->end());
    char* buf (current->get());

    while ((bytes = fread(buf + end, 1, current->capasity() - end, stdin)) > 0)
    {
	end += bytes;
	if (first)
	{
	    first = false;

	    // Skip any Windows style UTF-8 header
	    unsigned char u8header[] = {0xef, 0xbb, 0xbf};
	    if (end - start >= 3
		&& memcmp(buf + start, u8header, 3) == 0)
	    {
		start += 3;
	    }
	    start_time = std::chrono::high_resolution_clock::now();
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
	for (i = end-1; i > start; i--) if (buf[i] == '\n') break;

	if (start == i)
	{
	    throw std::runtime_error ("No newlines found in input buffer");
	}

	next->end() = end - i;
	memcpy (next->get(), buf + i, next->end());
	input_buffer* tmp = trader.producer_swap (current);
	current = next;
	next = tmp;
	start = current->start();
	end = current->end();
	buf = current->get();
    }
    if (first) return; // no input

    trader.wait_emptied();

    if (verbose)
    {
	std::cerr << "Sorting finished in " << std::fixed
		  << duration (start_time) << "s\n";
    }

    if (!map_only)
    {
	mapred_engine.reduce();
	if (verbose)
	{
	    std::cerr << "Merging finished in " << std::fixed
		      << duration (start_time) << "s\n";
	}
    }
}

static std::string get_default_workdir()
{
    return TMPDIR;
}

int
main (int argc, char* argv[])
{
    int64_t buffer_size = 2 * 1024 * 1024;
    int parallel = std::thread::hardware_concurrency();
    int max_files = 20 * parallel;
    bool verbose = false;
    bool compression = true;
    std::string subdir;
    std::string work_dir = get_default_workdir();
    bool map_only = false;
    bool reduce_only = false;
    bool sort_output = false;
    bool reverse_sort = false;
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
	    ("", "no-compression", "Disable compression", cmd, true);
	TCLAP::SwitchArg keepFilesArg
	    ("", "keep-tmpfiles", "Keep the temporary files after completion",
	     cmd, false);
	TCLAP::SwitchArg mapOnlyArg
	    ("", "map-only", "Only perform the mapping stage", cmd, false);
	TCLAP::SwitchArg reduceOnlyArg
	    ("", "reduce-only", "Only perform the reduce stage", cmd, false);
	TCLAP::SwitchArg sortArg
	    ("", "sort", "Sort keys in final output", cmd, false);
	TCLAP::SwitchArg reverseSortArg
	    ("", "rsort", "Reverse sort keys in final output", cmd, false);
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
	sort_output = sortArg.getValue();
	if (reverseSortArg.getValue())
	{
	    sort_output = reverse_sort = true;
	}
	plugin_path = pluginArg.getValue();
	compression = noCompressionArg.getValue();

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
    if (sort_output) config.set_sort_output();
    if (reverse_sort) config.set_reverse_sort();

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
