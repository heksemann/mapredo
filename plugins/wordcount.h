/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_WORDCOUNT_H
#define _HEXTREME_WORDCOUNT_H

#include <mapredo/mapreducer.h>

/**
 * Class used to count and sort words on popularity.
 */
class wordcount : public mapredo::mapreducer<std::string>
{
public:
    wordcount() {}
    virtual ~wordcount() {}
    void map (char* line, const int length, mapredo::collector& output);
    void reduce (std::string key, vlist& values,
		 mapredo::collector& output);
    bool reducer_can_combine() const {return true;}

private:    
};

#endif
