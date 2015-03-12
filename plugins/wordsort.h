/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_WORDSORT_H
#define _HEXTREME_WORDSORT_H

#include <mapredo/mapreducer.h>

/**
 * Class used to sort words on popularity.
 */
class wordsort final : public mapredo::mapreducer<int64_t>
{
public:
    void map (char* line, const int length, mapredo::collector& output);
    void reduce (int64_t key, vlist& values, mapredo::collector& output);
};

#endif
