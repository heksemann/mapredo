/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_CONSUMER_H
#define _HEXTREME_MAPREDO_CONSUMER_H

#include <memory>
#include <deque>

#include "collector.h"
#include "sorter.h"
#include "file_merger.h"
#include "base.h"

class plugin_loader;

/**
 * Class used to run map reduce algorithm
 */
class consumer : public mapredo::collector
{
public:
    /**
     * @param tmpdir root of temporary directory
     * @param subdir use a subdirectory for output, if not ""
     * @param numeric if true, sort numerically instead of alphabetically.
     * @param reverse if true, sort in descending order instead of ascending.
     */
    consumer (const std::string& tmpdir,
	      const std::string& subdir,
	      const size_t buckets,
	      const size_t bytes_buffer,
	      const mapredo::base::keytype type,
	      const bool reverse);
    virtual ~consumer();

    /** Flush content all memory buffers temporary files. */
    void flush();

    /** Used to collect data, called from the mapper */
    void collect (const char* line, const size_t length);

private:
    const std::string _tmpdir;
    bool _is_subdir = false;
    size_t _buckets;

    std::vector<sorter> _sorters;
};

#endif
