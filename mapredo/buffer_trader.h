/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_BUFFER_TRADER_H
#define _HEXTREME_MAPREDO_BUFFER_TRADER_H

#include <list>
#include <stack>
#include <mutex>
#include <condition_variable>

#include "input_buffer.h"

/**
 * Used to keep track of input buffers in memory.  This class is thread safe.
 */
class buffer_trader
{
public:
    /**
     * @param buffer_size size of each input buffer in bytes
     * @param num_buffers number of buffers to use
     * @param num_threads number of consumer threads
     */
    buffer_trader (const size_t buffer_size,
		   const size_t num_buffers,
		   const size_t num_threads);

    /**
     * Get an empty buffer.  This function is called initially from
     * the input thread, before it is able to swap with a filled
     * buffer.  In will throw an exception if called more times than
     * num_buffers.
     */
    input_buffer* producer_get();

    /**
     * Swap a filled buffer with an empty one.  This function is
     * called from the input thread and may hang until there is an
     * available buffer.
     * @param buffer filled buffer
     * @returns empty buffer
     */
     input_buffer* producer_swap (input_buffer* buffer);

    /**
     * Get the next buffer ready to be sorted.  This function may hang until
     * there is an available buffer.
     */
    input_buffer* consumer_get();

    /**
     * Swap a now empty buffer with the next buffer ready to be
     * sorted.  This function may hang until there is an available
     * buffer.
     */
    input_buffer* consumer_swap (input_buffer* buffer);

    /**
     * There will be no more incoming data, hang until all buffers are emptied
     */
    void wait_emptied();
    
private:
    const size_t _buffer_size;
    const size_t _max_buffers;
    std::mutex _mutex;
    std::condition_variable _cond;
    std::list<input_buffer> _all_buffers;
    std::stack<input_buffer*> _filled_buffers;
    std::stack<input_buffer*> _empty_buffers;
    std::stack<int> _waiting_consumers;
    std::stack<int> _waiting_producers;
};

#endif
