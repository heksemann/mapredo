/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_CONSUMER_H
#define _HEXTREME_MAPREDO_CONSUMER_H

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
     * Process input data.
     * @param trader object to pull jobs from.
     */
    void work (buffer_trader& trader);

    /** Flush content all memory buffers temporary files. */
    void flush();

    /** Used to collect data, called from the mapper */
    void collect (const char* line, const size_t length);

    consumer(consumer&&) = delete;
    consumer& operator=(const consumer&) = delete;
    
private:
    mapredo::base& _mapreducer;
    const std::string _tmpdir;
    bool _is_subdir = false;
    size_t _buckets;
    std::exception_ptr _texception;

    std::vector<sorter> _sorters;
};

#endif
