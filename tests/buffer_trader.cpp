#include <gtest/gtest.h>

#include <future>

#include "buffer_trader.h"

TEST(buffer_trader, producer_swap)
{
    buffer_trader bt (0x10000, 1);

    input_buffer* current = bt.producer_get();
    input_buffer* next = bt.producer_get();
    EXPECT_THROW (bt.producer_get(), std::runtime_error);

    EXPECT_NE (nullptr, current);
    EXPECT_NE (nullptr, next);

    current->end() = 111;
    auto* tmp = current;
    current = next;

    auto result (std::async(std::launch::async,
			    [](buffer_trader* bt)
			    {
				auto* buffer = bt->consumer_get(0);
				size_t value = buffer->end();
				if (bt->consumer_swap (buffer, 0))
				{
				    return (size_t)1;
				}
				return value;
			    },
			    &bt));
    
    next = bt.producer_swap (tmp);
    EXPECT_NE (nullptr, next);
    EXPECT_NE (next->end(), 111);

    bt.producer_finish();
    EXPECT_EQ (111, result.get());
}

TEST(buffer_trader, consumer_swap)
{
    const size_t num_threads = 10;
    const size_t num_pushes = 10000;
    buffer_trader bt (0x10000, num_threads);

    input_buffer* current = bt.producer_get();
    input_buffer* next = bt.producer_get();

    ASSERT_NE (nullptr, current);
    ASSERT_NE (nullptr, next);

    std::vector<std::future<size_t>> results;

    for (size_t i = 0; i < num_threads; i++)
    {
	results.push_back
	    (std::async(std::launch::async,
			[=](buffer_trader* bt)
			{
			    size_t total = 0;
			    auto* buffer = bt->consumer_get(i);

			    if (buffer)
			    {
				total += buffer->end();

				buffer->end() = 0;
				while ((buffer = bt->consumer_swap(buffer, i)))
				{
				    total += buffer->end();
				}
			    }

			    return total;
			},
			&bt));
    }

    for (size_t i = 0; i < num_pushes; i++)
    {
        current->end() = 111;
	auto* tmp = current;
	current = next;
	next = bt.producer_swap (tmp);
	ASSERT_EQ (0, next->end());
    }

    bt.producer_finish();

    size_t total = 0;

    for (auto& result: results)
    {
	total += result.get();
    }

    EXPECT_EQ (num_pushes * 111, total);
}

TEST(buffer_trader, small_data)
{
    buffer_trader bt (0x10000, 2);

    input_buffer* current = bt.producer_get();
    bt.producer_get();

    auto res1 = std::async(std::launch::async, [](buffer_trader* bt)
			   {while (bt->consumer_get(0));}, &bt);
    auto res2 = std::async(std::launch::async, [](buffer_trader* bt)
			   {while (bt->consumer_get(1));}, &bt);

    bt.producer_swap (current);
    bt.producer_finish();
    res1.get();
    res2.get();
}


TEST(buffer_trader, consumer_failure)
{
    buffer_trader bt (0x10000, 1);

    input_buffer* current = bt.producer_get();
    bt.producer_get();

    auto res = std::async
	(std::launch::async,
	 [](buffer_trader* bt) {bt->consumer_fail(0);},
	 &bt);

    current = bt.producer_swap(current);
    EXPECT_EQ (nullptr, bt.producer_swap(current));
    res.get();
}
