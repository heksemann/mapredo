/*** -*- C++ -*- *********************************************************/

#ifndef _HEXTREME_MAPREDO_VALUELIST_H
#define _HEXTREME_MAPREDO_VALUELIST_H

#include <map>

#include "tmpfile_reader.h"

namespace mapredo
{
    /** List of values relating to a key */
    template <class T> class valuelist
    {
    public:
	typedef std::pair<T, data_reader<T>*> tmpfile_entry;
	typedef std::multimap<T, data_reader<T>*>
	data_reader_queue;

	/**
	 * Iterator for values to reducer.
	 */
	class iterator
	{
	public:
	    iterator(data_reader_queue& queue) :
		_queue(&queue), _index(0) {
		auto* proc = (*_queue->begin()).second;

		_key = *proc->next_key();
		_value = proc->get_next_value();
		//std::cerr << "F " << _key << "(" << proc->filename() << ")\n";
	    }
	    iterator () {} // for end
	
	    const iterator& operator++() {
		auto* proc = (*_queue->begin()).second;
		const T* next_key = proc->next_key();

		if (next_key)
		{
#if 0
		    std::cerr << "N " << *next_key
			      << " (" << _key << ' ' <<proc->filename()
			      << ")\n";
#endif
		    if (*next_key == _key)
		    {
			//std::cerr << "V0\n";
			_value = proc->get_next_value();
			//std::cerr << "V0:" << _value << "\n";
			++_index;
			return *this;
		    }
		    if (_queue->size() > 1)
		    {
			auto niter = _queue->begin();
			niter++;
#if 0
			std::cerr << "NF " << niter->first
				  << " (" << niter->second->filename()
				  << ")\n";
#endif
			if (niter->first == _key)
			{
			    //std::cerr << "V1\n";
			    _value = niter->second->get_next_value();
			    //std::cerr << "V1:" << _value << "\n";
			    _queue->erase (_queue->begin());
			    _queue->insert (tmpfile_entry(*next_key, proc));
			    return *this;
			}
			//std::cerr << "All done for " << _key <<  "\n";
			if (*next_key > niter->first)
			{
			    _queue->erase (_queue->begin());
			    _queue->insert (tmpfile_entry(*next_key, proc));
			}
			//else std::cerr << "Keeping sequence\n";
		    }
		}
		else // file emptied, check next
		{
		    delete proc;
		    //std::cerr << "Check next, delete " << filename << "\n";
		    _queue->erase (_queue->begin());
		    if (!_queue->empty()) //  check next
		    {
			proc = (*_queue->begin()).second;
#if 0
			if (proc->next_key())
			{
			    std::cerr << "NF2 " << *proc->next_key()
				      << " (" << proc->filename()
				      << ")\n";
			}
			else std::cerr << "NF is empty\n";
#endif
			if (*proc->next_key() == _key)
			{
			    //std::cerr << "V2\n";
			    _value = proc->get_next_value();
			    //std::cerr << "V2:" << _value << "\n";
			    ++_index;
			    return *this;
			}
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
	    data_reader_queue* _queue = nullptr;
	    int _index = -1;
	    T _key;
	    char* _value;
	};

	valuelist (data_reader_queue& queue) :
	    _queue (queue) {}

	iterator begin() const {
	    if (!_queue.empty())
	    {
		auto* proc ((*_queue.begin()).second);
		const T* key (proc->next_key());

		if (key) return iterator(_queue);
		else return iterator();
	    }
	    else return iterator();
	}
	const iterator& end() const {return _end;}

    private:
	data_reader_queue& _queue;
	iterator _end;
    };
}

#endif
