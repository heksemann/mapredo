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

merge_cache::merge_cache (const std::string& tmpdir,
			  const size_t total_size,
			  const uint16_t buckets)
{
    std::ostringstream filename;

    filename << tmpdir << "/sort_" << std::this_thread::get_id();
    _file_prefix = filename.str();
}
c
