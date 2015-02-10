#include <gtest/gtest.h>

#include <future>

#include "buffer_trader.h"

TEST(buffer_trader, producer_swap)
{
    buffer_trader bt (0x10000, 3, 2);

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
				auto* buffer = bt->consumer_get();
				return buffer->end();
			    },
			    &bt));
    
    next = bt.producer_swap (tmp);
    EXPECT_NE (nullptr, next);
    EXPECT_NE (next->end(), 111);

    EXPECT_EQ (111, result.get());
}

TEST(buffer_trader, consumer_swap)
{
    const size_t num_threads = 10;
    const size_t num_pushes = 10000;
    buffer_trader bt (0x10000, num_threads + 5, num_threads);

    input_buffer* current = bt.producer_get();
    input_buffer* next = bt.producer_get();

    ASSERT_NE (nullptr, current);
    ASSERT_NE (nullptr, next);

    std::vector<std::future<size_t>> results;

    for (size_t i = 0; i < num_threads; i++)
    {
	results.push_back
	    (std::async(std::launch::async,
			[](buffer_trader* bt)
			{
			    size_t total = 0;
			    auto* buffer = bt->consumer_get();

			    if (buffer)
			    {
				total += buffer->end();

				buffer->end() = 0;
				while ((buffer = bt->consumer_swap(buffer)))
				{
				    total += buffer->end();
				    buffer->end() = 0;
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

    bt.finish();

    size_t total = 0;

    for (auto& result: results)
    {
	total += result.get();
    }

    EXPECT_EQ (num_pushes * 111, total);
}

TEST(buffer_trader, small_data)
{
    buffer_trader bt (0x10000, 3, 2);

    input_buffer* current = bt.producer_get();
    bt.producer_get();

    auto res1 = std::async(std::launch::async, [](buffer_trader* bt)
			   {while (bt->consumer_get());}, &bt);
    auto res2 = std::async(std::launch::async, [](buffer_trader* bt)
			   {while (bt->consumer_get());}, &bt);

    bt.producer_swap (current);
    bt.finish();
    res1.get();
    res2.get();
}


TEST(buffer_trader, consumer_failure)
{
    buffer_trader bt (0x10000, 3, 1);

    input_buffer* current = bt.producer_get();
    bt.producer_get();

    auto res = std::async
	(std::launch::async, [](buffer_trader* bt) {bt->finish (false);}, &bt);

    current = bt.producer_swap(current);
    EXPECT_EQ (nullptr, bt.producer_swap(current));
    res.get();
}
