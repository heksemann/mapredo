/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_VALUELIST_H
#define _HEXTREME_MAPREDO_VALUELIST_H

#include <queue>

#include "tmpfile_reader.h"

namespace mapredo
{
    /** List of values relating to a key */
    template <class T> class valuelist
    {
    public:
	/**
	 * Iterator for values to reducer.
	 */
	class iterator
	{
	public:
	    iterator (typename data_reader<T>::queue& queue, const T key) :
		_queue(&queue), _index(0), _key(key) {
		auto* proc = queue.top();
		_value = proc->get_next_value();
		//std::cerr << "F " << _key << '\n';
	    }
	    iterator () {} // for end

	    const iterator& operator++() {
		auto* proc = _queue->top();

		if (proc->next_key())
		{
		    //std::cerr << "N " << *proc->next_key() << "\n";
		    if (*proc == _key)
		    {
			_value = proc->get_next_value();
			//std::cerr << "V0:" << _value << "\n";
			++_index;
			return *this;
		    }
		    if (!_queue->empty())
		    {
			_queue->pop();
			auto* nproc = _queue->top();
			//std::cerr << "NF " << *nproc->next_key() << '\n';
			if (*nproc == _key)
			{
			    _value = nproc->get_next_value();
			    //std::cerr << "V1:" << _value << "\n";
			    _queue->push (proc);
			    return *this;
			}
			//std::cerr << "All done for " << _key <<  "\n";
			_queue->push (proc);
		    }
		}
		else // file emptied, check next
		{
		    _queue->pop();
		    //std::cerr << "Check next, delete\n";
		    delete proc;
		    if (!_queue->empty()) //  check next
		    {
			proc = _queue->top();
			if (proc->next_key())
			{
			    //std::cerr << "NF2 " << *proc->next_key() << '\n';
			    if (*proc == _key)
			    {
				//std::cerr << "V2\n";
				_value = proc->get_next_value();
				//std::cerr << "V2:" << _value << "\n";
				++_index;
				return *this;
			    }
			}
			//else std::cerr << "NF is empty\n";
		    }
		    //else std::cerr << "No more files\n";
		}
		_index = -1;
		return *this;
	    }
	    bool operator==(const iterator& other) const {
		return _index == other._index;
	    }
	    bool operator!=(const iterator& other) const {
		return _index != other._index;
	    }

	    char* operator*() {return _value;}
	private:
	    typename data_reader<T>::queue* _queue = nullptr;
	    int _index = -1;
	    T _key;
	    char* _value;
	};

	valuelist (typename data_reader<T>::queue& queue) :
	    _queue (queue) {}

	void set_key (T key) { _key = key;}

	iterator begin() const {
	    return iterator (_queue, _key);
	}
	const iterator& end() const {return _end;}

    private:

	typename data_reader<T>::queue& _queue;
	iterator _end;
	T _key;
    };
}

#endif
