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
		    const uint16_t parallel,
		    const int max_files)
{
    if (verbose)
    {
	std::ostringstream stream;
	
	stream << plugin_file << ":\n"
	       << " Using working directory " << work_dir << "\n"
	       << " Using " << parallel << " threads, with HW concurrency at "
	       << std::thread::hardware_concurrency() << "\n";
	std::cerr << stream.str();
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
		 const std::string& input_file,
		 const std::string& work_dir,
		 const std::string& subdir,
		 const bool verbose,
		 const size_t buffer_size,
		 const uint16_t parallel,
		 const int max_files,
		 const bool map_only)
{
    engine mapred_engine (plugin_file, work_dir, subdir, parallel, buffer_size,
			  max_files);
    size_t bytes;
    bool first = true;
    std::chrono::high_resolution_clock::time_point start_time;    
    input_buffer* buffer = mapred_engine.prepare_input();
    FILE* fp = stdin;

    if (input_file.size())
    {
	fp = fopen (input_file.c_str(), "r");
	if (!fp)
	{
	    char err[80];
#ifdef _WIN32
		strerror_s(err, sizeof(err), errno);
#endif
		throw std::runtime_error
			(std::string("Can not open input file: ")
#ifndef _WIN32
			+ strerror_r(errno, err, sizeof(err))
#else
			= err
#endif
			);
	}
    }

    while ((bytes = fread(buffer->get() + buffer->end(), 1,
			  buffer->capacity() - buffer->end(), fp)) > 0)
    {
	buffer->end() += bytes;
	if (first)
	{
	    first = false;

	    // Skip any Windows style UTF-8 header
	    unsigned char u8header[] = {0xef, 0xbb, 0xbf};
	    if (buffer->end() - buffer->start() >= 3
		&& memcmp(buffer->get() + buffer->start(), u8header, 3) == 0)
	    {
		buffer->start() += 3;
	    }
	    start_time = std::chrono::high_resolution_clock::now();
	    if (verbose)
	    {
		std::ostringstream stream;

		stream << plugin_file << ":\n"
		       << " Using working directory " << work_dir << "\n"
		       << " Sort buffer size is " << buffer_size << " bytes.\n"
		       << " Using " << parallel << " threads,  with HW"
		       << " concurrency at "
		       << std::thread::hardware_concurrency() << "\n";
		std::cerr << stream.str();
	    }
	}

	buffer = mapred_engine.provide_input_data (buffer);
    }

    mapred_engine.complete_input (buffer);

    if (first) return; // no input

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
    int64_t buffer_size;
    const char* buffer_size_str = "2M";
    uint16_t parallel = std::thread::hardware_concurrency() + 1;
    int max_files = 20 * parallel;
    bool compression = true;
    bool verbose = false;
    std::string workdir = get_default_workdir();
    std::string subdir;

    settings& config (settings::instance());

    char* env = getenv ("MAPREDO_THREADS");
    if (env) parallel = atoi (env);

    env = getenv ("MAPREDO_MAX_OPEN_FILES");
    if (env) max_files = atoi (env);

    env = getenv ("MAPREDO_BUFFER_SIZE");
    if (env) buffer_size_str = env;

    env = getenv ("MAPREDO_COMPRESSION");
    if (env) compression = (env[0] != '0' && env[0] != 'f' && env[0] != 'F');

    env = getenv ("MAPREDO_VERBOSE");
    if (env) verbose = (env[0] != '0' && env[0] != 'f' && env[0] != 'F');

    env = getenv ("MAPREDO_WORKDIR");
    if (env) workdir  = env;

    try
    {
	TCLAP::CmdLine cmd ("mapredo: Map-reduce engine for small/medium data",
			    ' ', PACKAGE_VERSION);
	TCLAP::ValueArg<std::string> subdir_arg
	    ("s", "subdir", "Subdirectory to use",
	     false, "", "string", cmd);
	TCLAP::ValueArg<std::string> work_dir
	    ("d", "work-dir", "Working directory to use",
	     false, workdir, "string", cmd);
	TCLAP::ValueArg<std::string> buffer_size_arg
	    ("b", "buffer-size", "Buffer size to use",
	     false, buffer_size_str, "size", cmd);
	TCLAP::ValueArg<int> max_files_arg
	    ("f", "max-open-files", "Maximum number of open files",
	     false, max_files, "number", cmd);
	TCLAP::ValueArg<int> threads_arg
	    ("j", "threads", "Number of threads to use",
	     false, parallel, "threads", cmd);
	TCLAP::SwitchArg verbose_arg
	    ("", "verbose", "Verbose output", cmd, verbose);
	TCLAP::SwitchArg no_compression_arg
	    ("", "no-compression", "Disable compression", cmd, true);
	TCLAP::SwitchArg keep_tmpfiles
	    ("", "keep-tmpfiles", "Keep the temporary files after completion",
	     cmd, false);
	TCLAP::SwitchArg map_only
	    ("", "map-only", "Only perform the mapping stage", cmd, false);
	TCLAP::SwitchArg reduce_only
	    ("", "reduce-only", "Only perform the reduce stage", cmd, false);
	TCLAP::SwitchArg sort_arg
	    ("", "sort", "Sort keys in final output", cmd, false);
	TCLAP::SwitchArg reverse_sort_arg
	    ("", "rsort", "Reverse sort keys in final output", cmd, false);
	TCLAP::ValueArg<std::string> inputfile
	    ("i", "input",
	     "Input file to use, defaults to reading standard input",
	     false, "", "string", cmd);
	TCLAP::UnlabeledValueArg<std::string> plugin_path
	    ("plugin", "Plugin file to use", true, "", "plugin file", cmd);

	cmd.parse (argc, argv);
	subdir = subdir_arg.getValue();
	buffer_size = config.parse_size (buffer_size_arg.getValue());
	max_files = max_files_arg.getValue();
	parallel = threads_arg.getValue();
	compression = no_compression_arg.getValue();

	if (!directory::exists(work_dir.getValue()))
	{
	    throw TCLAP::ArgException
		("The working directory '" + work_dir.getValue()
		 + "' does not exist",
		 "work-dir");
	}
	if (max_files < 3)
	{
	    throw TCLAP::ArgException
		("Can not work with less than 3 files", "max-open-files");
	}

	if (reduce_only.getValue() && map_only.getValue())
	{
	    throw TCLAP::ArgException
		("Options --map-only and --reduce-only are mutually exclusive",
		 "map-only");
	}
	if (sort_arg.getValue() && reverse_sort_arg.getValue())
	{
	    throw TCLAP::ArgException
		("Options --sort and --rsort are mutually exclusive",
		 "sort");
	}
	if (map_only.getValue() && subdir.empty())
	{
	    throw TCLAP::ArgException
		("Option --map-only cannot be used without --subdir",
		 "map-only");
	}
	if (reduce_only.getValue() && subdir.empty())
	{
	    throw TCLAP::ArgException
		("Option --reduce-only cannot be used without --subdir",
		 "reduce-only");
	}

        if (verbose_arg.getValue()) config.set_verbose();
	if (compression) config.set_compressed();
	if (keep_tmpfiles.getValue()) config.set_keep_tmpfiles();
	if (sort_arg.getValue()) config.set_sort_output();
	if (reverse_sort_arg.getValue()) config.set_reverse_sort();

	if (reduce_only.getValue())
	{
	    reduce (plugin_path.getValue(), work_dir.getValue(), subdir,
		    verbose_arg.getValue(), parallel, max_files);
	}
	else
	{
	    run (plugin_path.getValue(), inputfile.getValue(),
		 work_dir.getValue(), subdir, verbose_arg.getValue(),
		 buffer_size, parallel, max_files, map_only.getValue());
	}
    }
    catch (const TCLAP::ArgException& e)
    {
	std::cerr << "error: " << e.error() << " for " << e.argId()
		  << std::endl;
	return 1;
    }
    catch (const std::exception& e)
    {
	std::cerr << e.what() << std::endl;
	return 1;
    }

	std::cerr << "All is well\n";
	return 0;
}
