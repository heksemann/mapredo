
#include <sstream>
#include <stdexcept>

#include "settings.h"

settings&
settings::instance()
{
    static settings s;

    return s;
}

int64_t
settings::parse_size (const std::string& size) const
{
    static const std::string scale("kMGTPE");
    std::istringstream stream (size);
    int64_t num;
    char c = 0;

    stream >> num >> c;
    if (c == 0) return num;

    size_t pos = scale.find(c);
    if (pos != std::string::npos)
    {
	for (size_t i = 0; i <= pos; i++) num *= 1024;
    }
    else
    {
	std::ostringstream stream;

	stream << "No valid size symbol '" << c << "'";
	throw std::runtime_error (stream.str());
    }

    return num;
}
