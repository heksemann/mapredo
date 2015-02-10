/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_BUFFER_TRADER_H
#define _HEXTREME_MAPREDO_BUFFER_TRADER_H

#include <list>
#include <stack>
#include <mutex>
#include <condition_variable>

#include "input_buffer.h"

/**
 * Used to keep track of input buffers in memory.  This class is
 * thread safe, but needs to be be used with caution to avoid hanging
 * consumer threads.
 */
class buffer_trader
{
public:
    /**
     * @param buffer_size size of each input buffer in bytes
     * @param num_buffers number of buffers to use
     * @param num_consumers number of consumer threads
     */
    buffer_trader (const size_t buffer_size,
		   const size_t num_buffers,
		   const size_t num_consumers);
    ~buffer_trader();

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
     * @returns empty buffer or nullptr if a consumer has indicated failure
     */
     input_buffer* producer_swap (input_buffer* buffer);

    /**
     * Get the next buffer ready to be sorted.  This function may hang until
     * there is an available buffer.
     * @returns filled buffer or nullptr if there will be no more data
     */
    input_buffer* consumer_get();

    /**
     * Swap a now empty buffer with the next buffer ready to be
     * sorted.  This function may hang until there is an available
     * buffer.
     * @param buffer emptied buffer
     * @returns filled buffer or nullptr if there will be no more data
     */
    input_buffer* consumer_swap (input_buffer* buffer);

    /**
     * Called by producer to indicate that there will be no more
     * incoming data (successfully or not), or by a consumer to
     * indicate failure.  Make sure the producer calls this before
     * joining consumer threads, and always join any consumer threads
     * before destroying this object!
     * @param success if true, any buffered data shall be ignored if false
     */
    void finish (const bool success = true);
    
private:
    void consumer_finish();

    const size_t _buffer_size;
    const size_t _max_buffers;
    const size_t _num_consumers;
    std::mutex _mutex;
    bool _finished = false;
    bool _success;

    std::list<input_buffer> _all_buffers;
    std::condition_variable _producer_cond;
    std::condition_variable _consumer_cond;
    std::stack<input_buffer*> _filled_buffers;
    std::stack<input_buffer*> _empty_buffers;
};

#endif
