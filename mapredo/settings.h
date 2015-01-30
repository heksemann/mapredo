/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_SETTINGS_H
#define _HEXTREME_MAPREDO_SETTINGS_H

#include <cstdint>
#include <string>

/** Global settings for the engine */
class settings
{
public:
    /** @returns a singleton settings object */
    static settings& instance();

    int64_t parse_size (const std::string& size) const;
    bool verbose() const {return _verbose;}
    void set_verbose() {_verbose = true;}
    bool compressed() const {return _compressed;}
    void set_compressed (const bool on = true) {_compressed = on;}
    bool keep_tmpfiles() const {return _keep_tmpfiles;}
    void set_keep_tmpfiles (const bool on = true) {_keep_tmpfiles = on;}
    bool sort_output() const {return _sort_output;}
    void set_sort_output (const bool on = true) {_sort_output = on;}
    bool reverse_sort() const {return _reverse_sort;}
    void set_reverse_sort (const bool on = true) {_reverse_sort = on;}

private:
    settings() = default;

    bool _verbose = false;
    bool _compressed = false;
    bool _keep_tmpfiles = false;
    bool _sort_output = false;
    bool _reverse_sort = false;
};

#endif

