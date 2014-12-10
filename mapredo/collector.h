/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_COLLECTOR_H
#define _HEXTREME_MAPREDO_COLLECTOR_H

#include <sstream>

namespace mapredo
{
    /**
     * Collect class for mapreducers
     */
    class collector
    {
    public:
	virtual void collect (const char* line, const int length) = 0;
	template<typename T1, typename T2>
	void collect_keyval (const T1& key, const T2& value) {
	    std::ostringstream stream;
	    stream << key << '\t' << value;
	    const std::string& val (stream.str());
	    collect (val.c_str(), val.size());
	}
	virtual ~collector() {}

    protected:
	collector() = default;
    };
}

#endif
