/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_WORDCOUNT_H
#define _HEXTREME_WORDCOUNT_H

#include <mapredo/mapreducer.h>

/**
 * Class used to count and sort words on popularity.
 */
class wordcount : public mapredo::mapreducer<char*>
{
public:
    wordcount() {}
    virtual ~wordcount() {}
    void map (char* line, const int length, mapredo::mcollector& output);
    void reduce (char* key, vlist& values, mapredo::rcollector& output);
    bool reducer_can_combine() const {return true;}

private:    
};

#endif
