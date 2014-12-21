

#include <string>
#include <gtest/gtest.h>

#include "configuration.h"
#include "mapreducer.h"

class someplug : public mapredo::mapreducer<std::string>
{
public:
    void setup_configuration (mapredo::configuration& config) {
	config.add ("someparam", _integer, true, "Nice param", 's');
	config.add ("anotherparam", _text, true, "Nice param", 'a');
	config.add ("lastparam", _boolean, true, "Nice param");
    }

    void map (char *line, int length, mapredo::collector&) {
	if (_integer != 1 || _text != "1" || _boolean != true)
	{
	    throw std::runtime_error ("Value mismatch");
	}
    }
    void reduce (std::string key, vlist& values, mapredo::collector&) {}
private:
    int _integer;
    std::string _text;
    bool _boolean;
};

class ncollector: public mapredo::collector
{
public:
    void collect (const char*, const  size_t) {}
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
