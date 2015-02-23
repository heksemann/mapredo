#include <gtest/gtest.h>

#include "data_reader_queue.h"
#include "tmpfile_reader.h"

TEST(data_reader_queue, forward_int64)
{
    EXPECT_EQ (0, system (R"(printf "1\n5\n6\n" >testfile1)"));
    EXPECT_EQ (0, system (R"(printf "3\n4\n6\n" >testfile2)"));

    data_reader_queue<int64_t> q;
    tmpfile_reader<int64_t> r1 ("testfile1", 0x10000, false);
    tmpfile_reader<int64_t> r2 ("testfile2", 0x10000, false);

    EXPECT_EQ (true, q.empty());
    
    q.push (&r1);
    q.push (&r2);

    EXPECT_EQ (false, q.empty());
    EXPECT_EQ (2, q.size());

    auto proc = q.top();
    EXPECT_EQ (1, *proc->next_key());
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (3, *proc->next_key());    
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (4, *proc->next_key());
}

TEST(data_reader_queue, reverse_int64)
{
    EXPECT_EQ (0, system (R"(printf "6\n5\n1\n" >testfile1)"));
    EXPECT_EQ (0, system (R"(printf "7\n4\n3\n" >testfile2)"));

    data_reader_queue<int64_t> q (true);
    tmpfile_reader<int64_t> r1 ("testfile1", 0x10000, false);
    tmpfile_reader<int64_t> r2 ("testfile2", 0x10000, false);

    EXPECT_EQ (true, q.empty());
    
    q.push (&r1);
    q.push (&r2);

    EXPECT_EQ (false, q.empty());
    EXPECT_EQ (2, q.size());

    auto proc = q.top();
    EXPECT_EQ (7, *proc->next_key());
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (6, *proc->next_key());    
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (5, *proc->next_key());

    EXPECT_EQ (0, system ("rm -f testfile1 testfile2"));
}

TEST(data_reader_queue, forward_string)
{
    EXPECT_EQ (0, system (R"(printf "abc\ndef\nghi\n" >testfile1)"));
    EXPECT_EQ (0, system (R"(printf "bcd\nefg\nhij\n" >testfile2)"));

    data_reader_queue<char*> q;
    tmpfile_reader<char*> r1 ("testfile1", 0x10000, false);
    tmpfile_reader<char*> r2 ("testfile2", 0x10000, false);

    EXPECT_EQ (true, q.empty());
    
    q.push (&r1);
    q.push (&r2);

    EXPECT_EQ (false, q.empty());
    EXPECT_EQ (2, q.size());

    auto proc = q.top();
    EXPECT_EQ (0, strcmp("abc", *proc->next_key()));
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (0, strcmp("bcd", *proc->next_key()));    
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (0, strcmp("def", *proc->next_key()));
}

TEST(data_reader_queue, reverse_string)
{
    EXPECT_EQ (0, system (R"(printf "ghi\ndef\nabc\n" >testfile1)"));
    EXPECT_EQ (0, system (R"(printf "hij\nefg\nbcd\n" >testfile2)"));

    data_reader_queue<char*> q (true);
    tmpfile_reader<char*> r1 ("testfile1", 0x10000, false);
    tmpfile_reader<char*> r2 ("testfile2", 0x10000, false);

    EXPECT_EQ (true, q.empty());
    
    q.push (&r1);
    q.push (&r2);

    EXPECT_EQ (false, q.empty());
    EXPECT_EQ (2, q.size());

    auto proc = q.top();
    EXPECT_EQ (0, strcmp("hij", *proc->next_key()));
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (0, strcmp("ghi", *proc->next_key()));    
    proc->get_next_value();

    q.pop();
    q.push (proc);
    proc = q.top();
    EXPECT_EQ (0, strcmp("efg", *proc->next_key()));
}
