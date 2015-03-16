
#include <string>
#include <gtest/gtest.h>

#include "configuration.h"
#include "mapreducer.h"

class someplug : public mapredo::mapreducer<char*>
{
public:
    void setup_configuration (mapredo::configuration& config) {
	config.add ("someparam", _integer, true, "Nice param", 's');
	config.add ("anotherparam", _text, true, "Nice param", 'a');
	config.add ("lastparam", _boolean, true, "Nice param");
    }

    void map (char *line, int length, mapredo::mcollector&) {
	if (_integer != 1 || _text != "1" || _boolean != true)
	{
	    throw std::runtime_error ("Value mismatch");
	}
    }
    void reduce (char* key, vlist& values, mapredo::rcollector&) {}
private:
    int _integer;
    std::string _text;
    bool _boolean;
};

class ncollector: public mapredo::mcollector
{
public:
    void collect (const char*, const  size_t) {}
    char* reserve (const char* const key, const size_t bytes) {return nullptr;}
    void collect_reserved (const size_t) {}
};

TEST(configuration, add)
{
    someplug p;
    mapredo::configuration c;

    p.setup_configuration (c);

    EXPECT_EQ (3, c.size());
    for (auto& param: c)
    {
	param->set ("1");
    }

    ncollector coll;
    p.map ((char*)"", 0, coll);
}
