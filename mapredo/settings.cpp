/*
 * mapredo
 * Copyright (C) 2015 Kjell Irgens <hextremist@gmail.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 */

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
