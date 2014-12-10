/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_SETTINGS_H
#define _HEXTREME_MAPREDO_SETTINGS_H

#include <cstdint>
#include <string>

class settings
{
public:
    static settings& instance();

    int64_t parse_size (const std::string& size) const;
    bool verbose() const {return _verbose;}
    void set_verbose() {_verbose = true;}
    bool compressed() const {return _compressed;}
    void set_compressed (const bool on = true) {_compressed = on;}
    bool keep_tmpfiles() const {return _keep_tmpfiles;}
    void set_keep_tmpfiles (const bool on = true) {_keep_tmpfiles = on;}
private:
    settings() = default;

    bool _verbose = false;
    bool _compressed = false;
    bool _keep_tmpfiles = false;
};

#endif

