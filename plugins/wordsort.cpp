#include <iostream>

#include "wordsort.h"

MAPREDO_FACTORIES (wordsort)

void
wordsort::map (char* line, const int length, mapredo::mcollector& output)
{
    for (int i = 0; i < length; i++)
    {
	if (line[i] == '\t')
	{
	    output.collect_keyval (atoll(line + i + 1),
				   std::string(line, i));
	    return;
	}
    }
    throw std::runtime_error ("No tab in input data");
}

void
wordsort::reduce (int64_t key, vlist& values, mapredo::rcollector& output)
{
    for (char* value : values)
    {
	output.collect_keyval (value, key);
    }
}
