/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_CONSUMER_H
#define _HEXTREME_MAPREDO_CONSUMER_H

#include <map>
#include <thread>

#include "collector.h"
#include "sorter.h"
#include "buffer_trader.h"

class plugin_loader;
class mapreducer;

/**
 * Class used to run map and sort
 */
class consumer : public mapredo::collector
{
public:
    /**
     * Create object and start thread waiting for 
     * @param mapreducer map-reducer plugin object.
     * @param tmpdir root of temporary directory.
     * @param is_subdir true if the directory is a specified subdirectory.
     * @param type type to use for sorting.
     * @param reverse if true, sort in descending order instead of ascending.
     */
    consumer (mapredo::base& mapred,
	      const std::string& tmpdir,
	      const bool is_subdir,
	      const size_t buckets,
	      const size_t bytes_buffer,
	      const bool reverse);
    virtual ~consumer();

    /**
     * Process input data in a separate thread.
     * @param trader object to pull jobs from.
     */
    void start_thread (buffer_trader& trader);

    /**
     * Wait for input data processing thread to finish.
     */
    void join_thread();

    /** Append all temporary files of a given index to a list of files */
    void append_tmpfiles (const size_t index, std::list<std::string>& files);

    /** Used to collect data, called from the mapper */
    void collect (const char* line, const size_t length);


    consumer(consumer&&) = delete;
    consumer& operator=(const consumer&) = delete;
    
private:
    void work (buffer_trader& trader);

    std::thread _thread;
    mapredo::base& _mapreducer;
    const std::string _tmpdir;
    bool _is_subdir = false;
    size_t _buckets;
    std::exception_ptr _texception;

    std::vector<sorter> _sorters;
    std::map<int, std::list<std::string>> _tmpfiles;
};

#endif
