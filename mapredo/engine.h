/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_ENGINE_H
#define _HEXTREME_MAPREDO_ENGINE_H

#include <memory>

#include "collector.h"
#include "sorter.h"
#include "file_merger.h"
#include "base.h"

class plugin_loader;

/**
 * Class used to run map reduce algorithm
 */
class engine : public mapredo::collector
{
public:
    engine (const std::string& tmpdir,
	    const std::string& subdir,
	    const size_t parallel,
	    const size_t bytes_buffer,
	    const int max_open_files);
    virtual ~engine();

    /**
     * Prepare for sorting.  This must be called before feeding mappers.
     * @param numeric if true, sort numerically instead of alphabetically.
     * @param reverse if true, sort in descending order instead of ascending.
     */
    void enable_sorters (const mapredo::base::keytype type, const bool reverse);

    /**
     * Flush content of all sorted temporary files.
     */
    void flush();

    /**
     * Go through all sorted temporary files and generate a reduced
     * file.
     * @param mapreducer mapreducer object
     * @param loader plugin loader factory for creation of extra mapreducers
     */
    void flush (mapredo::base& mapreducer, plugin_loader& loader);

    /**
     * Reduce only.  Reduction is normally done inside flush, but this can be
     * used to reduce existing files in subdir.
     * @param mapreducer mapreducer object
     * @param loader plugin loader factory for creation of extra mapreducers
     */
    void reduce (mapredo::base& mapreducer, plugin_loader& loader);

    /** Used to collect data, called from mapper */
    void collect (const char* line, const size_t length);

private:
    void merge (mapredo::base& mapreducer);

    const std::string _tmpdir;
    bool _is_subdir = false;
    size_t _parallel;
    size_t _bytes_buffer;
    int _max_files;
    bool _unprepared = true;
    size_t _unique_id = 0;

    std::vector<sorter> _sorters;
    std::vector<file_merger> _mergers;
    std::list<std::string> _files_final_merge;
};

#endif
