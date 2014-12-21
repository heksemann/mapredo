
#include <gtest/gtest.h>

#include "compression.h"
#include "directory.h"
#include "plugin_loader.h"
#include "mapreducer.h"

TEST(compression, restore)
{
    compression comp;
    char data[] = "Let's compress some meaningless data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n"
	" data data data data data data data data data data data data data\n";
    char buffer1[900];
    char buffer2[900];
    char *in = data;
    char *out = buffer1;
    size_t insize = strlen(data) + 1;
    size_t outsize = sizeof (buffer1);

    comp.compress (in, insize, out, outsize);
    EXPECT_LT (outsize, insize);
    
    in = buffer1;
    out = buffer2;
    insize = outsize;
    outsize = sizeof (buffer2);
    EXPECT_EQ (true, comp.inflate(in, insize, out, outsize));
    
    EXPECT_EQ (0, strcmp (data, out));
}

TEST(directory, remove)
{
    directory::remove ("testdir", true, true);
    directory::create ("testdir");
    EXPECT_THROW (directory::create ("nonexisting/testdir"),
		  std::runtime_error);
    EXPECT_EQ (0, system ("touch testdir/file1 testdir/file2 testdir/file3"));
    directory::create ("testdir/another");
    EXPECT_EQ (0, system ("touch testdir/another/file4"));

    directory dir ("testdir");
    int i = 0;

    for (auto file: dir)
    {
	EXPECT_GT (strlen(file), 4);
	i++;
    }
    EXPECT_EQ (4, i);

    EXPECT_FALSE (directory::remove("nonexisting", true, true));
    EXPECT_TRUE (directory::exists("testdir"));
    EXPECT_THROW (directory::remove("testdir"), std::runtime_error);
    EXPECT_TRUE (directory::exists("testdir"));
    EXPECT_THROW (directory::remove("testdir", true), std::runtime_error);
    EXPECT_TRUE (directory::exists("testdir"));
    EXPECT_TRUE (directory::remove("testdir", true, true));
    EXPECT_FALSE (directory::exists("testdir"));
}

TEST(plugin_loader, get)
{
    plugin_loader loader ("../plugins/.libs/wordcount.so");

    mapredo::base& mapreducer (loader.get());
    (void)mapreducer;
}

#if 0
class  btest : public mapredo::mapreducer<double>
{
public:
    void map (char *line, const int length, mapredo::collector& output) {
        output.collect ("1.0\tx", 5);
    }
    void reduce (double key, const vlist& values,
                 mapredo::collector& output) {
        std::string story;
        for (const char* value : values) story += value;
        output.collect (story.c_str(), story.length());
    }
};
class mcollector: public mapredo::collector
{
public:
    void collect (const char* line, const int length) {
        std::cout << std::string (line, length) << "\n";
    }
};

TEST(base, map)
{
    btest m;
    m.map
}
#endif

int main (int argc, char **argv)
{
    ::testing::InitGoogleTest (&argc, argv);
    return RUN_ALL_TESTS();
}
