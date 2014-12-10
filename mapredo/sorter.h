/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_SORTER_H
#define _HEXTREME_MAPREDO_SORTER_H

#include <string>
#include <vector>
#include <list>
#include <future>

#include "sorter_buffer.h"
#include "base.h"

class compression;

class sorter
{
public:
    sorter (const std::string& tmpdir,
	    const size_t index,
	    const size_t max_bytes_buffer,
	    const mapredo::base::keytype type,
	    const bool reverse);
    ~sorter();

    /**
     * Add a key and value to the buffer, flush if necessary
     * @param keyvalue tab separated key and value
     * @param size length of keyvalue
     */
    void add (const char* keyvalue, const size_t size);

    /**
     * Take the list of temporary files. This empties the list in this object
     */
    std::list<std::string> grab_tmpfiles();

    sorter (sorter&& other);
    sorter (const sorter&) = delete;

    void flush();
    void wait_flushed();

private:
    std::string flush_buffer_safe (sorter_buffer* const buffer);
    std::string flush_buffer (sorter_buffer* const buffer);

    std::list<sorter_buffer> _buffers;
    sorter_buffer* _current = 0;
    const std::string _tmpdir;
    const int _bytes_per_buffer;
    std::string _file_prefix;
    int _tmpfile_id = 0;
    std::list<std::string> _tmpfiles;
    std::unique_ptr<compression> _compressor;
    bool _merging_off = false;
    bool _flushing_in_progress = false;
    std::future<std::string> _flush_result;
    std::exception_ptr _texception;
    const mapredo::base::keytype _type;
    const bool _reverse;
};

#endif
