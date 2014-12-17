
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <libgen.h>
#include <dlfcn.h>
#include <iostream>
#include <thread>
#include <stdexcept>
#include <tclap/CmdLine.h>

#include <mapredo/settings.h>
#include <mapredo/engine.h>
#include <mapredo/base.h>
#include <mapredo/plugin_loader.h>

static double time_since (timeval& time)
{
    double duration;
    timeval stop;

    gettimeofday (&stop, NULL);
    if (stop.tv_usec >= time.tv_usec)
    {
	duration = (stop.tv_sec - time.tv_sec
		    + (stop.tv_usec - time.tv_usec) / 1000000.0);
    }
    else
    {
	duration = (stop.tv_sec - time.tv_sec - 1
		    + ((1000000 - time.tv_usec) + stop.tv_usec) / 1000000.0);
    }
    time = stop;
    return duration;
}

static void reduce (const std::string& plugin_file,
		    const std::string& subdir,
		    const bool verbose,
		    const int parallel,
		    const int max_files)
{
    if (verbose)
    {
	std::cerr << "Using " << parallel << " threads,"
		  << " with HW concurrency at "
		  << std::thread::hardware_concurrency()
		  << "\n";
    }

    plugin_loader plugin (plugin_file);
    auto& mapreducer (plugin.get());
    engine mapred_engine (TMPDIR, subdir, parallel, 0, max_files);
    timeval start_time;

    if (verbose) gettimeofday (&start_time, NULL);

    mapred_engine.reduce (mapreducer, plugin);

    if (verbose)
    {
	std::cerr << "Merging finished in " << std::fixed
		  << time_since (start_time) << "s\n";
    }
}

static void run (const std::string& plugin_file,
		 const std::string& subdir,
		 const bool verbose,
		 const size_t buffer_size,
		 const int parallel,
		 const int max_files,
		 const bool map_only)
{
    plugin_loader plugin (plugin_file);
    auto& mapreducer (plugin.get());
    engine mapred_engine (TMPDIR, subdir, parallel, buffer_size, max_files);
    size_t buf_size = 0x2000;
    std::unique_ptr<char[]> buf (new char[buf_size]);
    size_t start = 0, end = 0;
    size_t i;
    ssize_t bytes;
    std::string line;
    bool first = true;
    timeval start_time;

    while ((bytes = read(STDIN_FILENO, buf.get() + end, buf_size - end)) > 0)
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
	    gettimeofday (&start_time, NULL);
	    mapred_engine.enable_sorters (mapreducer.type(),
					  false);
	    if (verbose)
	    {
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

int
main (int argc, char* argv[])
{
    int64_t buffer_size = 10 * 1024 * 1024;
    int parallel = std::thread::hardware_concurrency();
    int max_files = 20 * parallel;
    int verbose = false;
    int compression = true;
    std::string subdir;
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
	buffer_size = config.parse_size (bufferSizeArg.getValue());
	max_files = maxFilesArg.getValue();
	parallel = threadsArg.getValue();
	verbose = verboseArg.getValue();
	keep_tmpfiles = keepFilesArg.getValue();
	map_only = mapOnlyArg.getValue();
	reduce_only = reduceOnlyArg.getValue();
	plugin_path = pluginArg.getValue();

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
	std::cerr << "error: " << e.error() << " for arg " << e.argId()
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
	    reduce (plugin_path, subdir, verbose, parallel, max_files);
	}
	else
	{
	    run (plugin_path, subdir, verbose, buffer_size, parallel,
		 max_files, map_only);
	}
    }
    catch (const std::exception& e)
    {
	std::cerr << e.what() << std::endl;
	return 1;
    }

    return 0;
}
