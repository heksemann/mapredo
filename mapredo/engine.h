/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_ENGINE_H
#define _HEXTREME_MAPREDO_ENGINE_H

#include <memory>

#include "collector.h"
#include "sorter.h"
#include "file_merger.h"
#include "base.h"

/**
 * Class used to run map reduce algorithm
 */
class engine : public mapredo::collector
{
public:
    engine (const std::string& tmpdir,
	    const std::string& subdir,
	    const int parallel,
	    const int bytes_buffer,
	    const int max_open_files);
    virtual ~engine();

    /**
     * Prepare for sorting.  This must be called before feeding mappers.
     * @param numeric if true, sort numerically instead of alphabetically.
     * @param reverse if true, sort in descending order instead of ascending.
     */
    void enable_sorters (const mapredo::base::keytype type, const bool reverse);

    /**
     * Go through all sorted temporary files and generate a reduced
     * file.  If map_to is set, skip merging of files.
     */
    void flush (mapredo::base* mapreducer);

    /**
     * Reduce only.  Reduction is normally done inside flush, but this can be
     * used to reduce existing files in subdir.
     */
    void reduce (mapredo::base& mapreducer);

    /** Used to collect data, called from mapper */
    void collect (const char* line, const int length);

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
