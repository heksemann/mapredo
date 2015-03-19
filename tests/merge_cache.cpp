#include <gtest/gtest.h>

#include "merge_cache.h"

TEST(merge_cache, add)
{
    merge_cache<char*> cache ("./sort_000.", 20, 4);

    add (3, "This is some data", 17);
}
