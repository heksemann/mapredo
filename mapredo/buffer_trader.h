/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_BUFFER_TRADER_H
#define _HEXTREME_MAPREDO_BUFFER_TRADER_H

#include <list>
#include <vector>
#include <stack>
#include <mutex>
#include <condition_variable>
#include <atomic>

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
     * @param num_consumers number of consumer threads
     */
    buffer_trader (const size_t buffer_size,
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
     * called from the input thread and may spin until there is an
     * available buffer.
     * @param buffer filled buffer
     * @returns empty buffer, or nullptr if a consumer has indicated failure
     */
     input_buffer* producer_swap (input_buffer* buffer);

    /**
     * Get the next buffer ready to be sorted.  This function may hang until
     * there is an available buffer.
     * @param id thread identifier
     * @returns filled buffer or nullptr if there will be no more data
     */
    input_buffer* consumer_get (const size_t id);

    /**
     * Swap a now empty buffer with the next buffer ready to be
     * sorted.  This function may hang until there is an available
     * buffer.
     * @param buffer emptied buffer
     * @param id thread identifier
     * @returns filled buffer or nullptr if there will be no more data
     */
    input_buffer* consumer_swap (input_buffer* buffer, const size_t id);

    /**
     * Called by producer to indicate that there will be no more
     * incoming data (successfully or not).  Make sure the producer
     * calls this before joining consumer threads, and always join any
     * consumer threads before destroying this object!
     * @param success if true, any buffered data shall be ignored if false
     */
    void producer_finish();

    /**
     * Called by consumer to indicate failure.  This will cause
     * processing to stop.
     * @param id thread identifier
     */
    void consumer_fail (const size_t id);

private:
    class bsem
    {
    public:
	void post() {
	    {
		std::unique_lock<std::mutex> locker(_mutex);
		if (_value)
		{
		    throw std::runtime_error
			("Unexpected value in bsem::post()");
		}
		_value = true;
	    }
	    _cv.notify_one();
	}
	void wait() {
	    std::unique_lock<std::mutex> locker(_mutex);
	    while (!_value) _cv.wait(locker);
	    _value = false;
	}
    private:
	std::mutex _mutex;
	std::condition_variable _cv;
	bool _value = false;
    };

    enum state
    {
	INITIAL, // no buffer, should be filled by producer shortly
	WORKING, // consumer is working, empty buffer available
	FILLED,  // filled buffer, ready for processing
	FINISHED // no more input
    };

    static const size_t _max_consumers = 100;
    const size_t _buffer_size;
    const size_t _num_consumers;
    std::list<input_buffer> _all_buffers;
    std::array<std::atomic<state>, _max_consumers> _states;
    std::vector<input_buffer*> _buffers;
    std::array<bsem, _max_consumers> _sems;
    size_t _current_task = 0;
    size_t _initiated = 0;
    std::atomic<bool> _producer_scan;
    bsem _producer_sem;
};

#endif
