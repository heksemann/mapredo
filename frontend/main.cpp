
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <getopt.h>
#include <libgen.h>
#include <dlfcn.h>
#include <iostream>
#include <thread>
#include <stdexcept>

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

static void reduce (const char* const plugin_file,
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

    mapred_engine.reduce (mapreducer);

    if (verbose)
    {
	std::cerr << "Merging finished in " << std::fixed
		  << time_since (start_time) << "s\n";
    }
}

static void run (const char* const plugin_file,
		 const std::string& subdir,
		 const bool verbose,
		 const size_t buffer_size,
		 const int parallel,
		 const int max_files,
		 const bool map_only)
{
    if (verbose)
    {
	std::cerr << "Maximum buffer size is " << buffer_size << " bytes.\n"
		  << "Using " << parallel << " threads,"
		  << " with HW concurrency at "
		  << std::thread::hardware_concurrency()
		  << "\n";
    }

    plugin_loader plugin (plugin_file);
    auto& mapreducer (plugin.get());
    engine mapred_engine (TMPDIR, subdir, parallel, buffer_size, max_files);
    char buf[0x2000];
    size_t start = 0, end = 0;
    size_t i;
    ssize_t bytes;
    std::string line;
    bool first = true;
    timeval start_time;

    while ((bytes = read(STDIN_FILENO, buf + end, sizeof(buf) - end)) > 0)
    {
	end += bytes;
	if (first)
	{
	    // Skip any Windows style UTF-8 header
	    unsigned char u8header[] = {0xef, 0xbb, 0xbf};
	    if (end - start >= 3 && memcmp(buf + start, u8header, 3) == 0)
	    {
		start += 3;
	    }
	    gettimeofday (&start_time, NULL);
	    mapred_engine.enable_sorters (mapreducer.type(),
					  false);
	    first = false;
	}
	for (i = start; i < end; i++)
	{
	    if (buf[i] == '\n')
	    {
		mapreducer.map(buf + start, i - start, mapred_engine);
		start = i + 1;
	    }
	}
	if (start < i)
	{
	    if (start == 0)
	    {
		std::ostringstream stream;

		stream << "No support for input lines longer than "
			<< sizeof(buf) << " bytes";
		throw std::runtime_error (stream.str());
	    }
	    memmove (buf, buf + start, end - start);
	    end -= start;
	    start = 0;
	}
	else start = end = 0;
    }

    if (verbose)
    {
	std::cerr << "Sorting finished in " << std::fixed
		  << time_since (start_time) << "s\n";
    }

    if (!map_only)
    {
	mapred_engine.flush (&mapreducer);
	if (verbose)
	{
	    std::cerr << "Merging finished in " << std::fixed
		      << time_since (start_time) << "s\n";
	}
    }
    else mapred_engine.flush (nullptr);
}

int
main (int argc, char* argv[])
{
    int c;
    int64_t buffer_size = 10 * 1024 * 1024;
    int parallel = std::thread::hardware_concurrency();
    int max_files = 20 * parallel;
    int verbose = false;
    int compression = true;
    std::string subdir;
    int map_only = false;
    int reduce_only = false;
    int keep_tmpfiles = false;
    bool error = false;
    const struct option long_options[] = {
	{"buffer-size", required_argument, 0, 'b'},
	{"max-open-files", required_argument, 0, 'f'},
	{"threads", required_argument, 0, 'j'},
	{"verbose", no_argument, &verbose, 1},
	{"no-compression", no_argument, &compression, 0},
	{"subdir", required_argument, 0, 's'},
	{"map-only", no_argument, &map_only, 1},
	{"reduce-only", no_argument, &reduce_only, 1},
	{"keep-tmpfiles", no_argument, &keep_tmpfiles, 1},
	{0, 0, 0, 0}
    };
    settings& config (settings::instance());

    for (;;)
    {
	/* getopt_long stores the option index here. */
	int option_index = 0;

	c = getopt_long (argc, argv, "b:f:j:s:mr", long_options, &option_index);
	if (c == -1) break;

	switch (c)
	{
	case 0:
	    break;

	case 'b':
	    buffer_size = config.parse_size (optarg);
	    break;

	case 'f':
	    max_files = atoi (optarg);
	    if (max_files < 3)
	    {
		std::cerr << "Can not work with less than 3 files\n";
		error = true;
	    }
	    break;

	case 'j':
	    parallel = atoi (optarg);
	    break;

	case 'm':
	    map_only = true;
	    break;

	case 'r':
	    reduce_only = true;
	    break;

	case 's':
	    subdir = optarg;
	    break;
	  
	case '?': /* getopt_long already printed an error message. */
	    error = true;
	    break;
	  
	default:
	    return 1;
	}
    }

    if (reduce_only && map_only)
    {
	error = true;
	std::cerr << "Options --map-only and --reduce-only are mutually"
		  << " exclusive\n";
    }
    if (map_only && subdir.empty())
    {
	error = true;
	std::cerr << "Option --map-only cannot be used without --subdir\n";
    }
    if (reduce_only && subdir.empty())
    {
	error = true;
	std::cerr << "Option --reduce-only cannot be used without --subdir\n";
    }

    if (argc - optind != 1 || error)
    {
	std::cerr << "Usage: "  << basename(argv[0])
		  << " [OPTIONS] <plugin.so>\n"
		  << "\t-b,--buffer-size <size>\n"
	    	  << "\t-j,--threads <threads>\n"
		  << "\t-f,--max-open-files <number>\n"
		  << "\t-s,--subdir <name>\n"
		  << "\t-m,--map-only\n"
		  << "\t-r,--reduce-only\n"
		  << "\t--no-compression\n"
		  << "\t--verbose\n";
	return (1);
    }
    if (verbose) config.set_verbose();
    if (compression) config.set_compressed();
    if (keep_tmpfiles) config.set_keep_tmpfiles();

    try
    {
	if (reduce_only)
	{
	    reduce (argv[optind], subdir, verbose, parallel, max_files);
	}
	else
	{
	    run (argv[optind], subdir, verbose, buffer_size, parallel,
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
