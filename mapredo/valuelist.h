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
	typedef std::pair<T, tmpfile_reader<T>*> tmpfile_entry;
	typedef std::multimap<T, tmpfile_reader<T>*>
	tmpfile_reader_queue;

	/**
	 * Iterator for values to reducer.
	 */
	class const_iterator
	{
	public:
	    const_iterator(tmpfile_reader_queue& queue) :
		_queue(&queue), _index(0) {
		auto* proc = (*_queue->begin()).second;

		_key = *proc->next_key();
		_value = proc->get_next_value();
		//std::cerr << "F " << _key << "(" << proc->filename() << ")\n";
	    }
	    const_iterator () {} // for end
	
	    const const_iterator& operator++() {
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
		    std::string filename (proc->filename());
		    delete proc;
		    //std::cerr << "Check next, delete " << filename << "\n";
		    remove (filename.c_str());
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
	    bool operator==(const const_iterator& other) const {
		return _index == other._index;
	    }
	    bool operator!=(const const_iterator& other) const {
		return _index != other._index;
	    }

	    const char* operator*() const {return _value;}
	private:
	    tmpfile_reader_queue* _queue = nullptr;
	    int _index = -1;
	    T _key;
	    const char* _value;
	};

	valuelist (tmpfile_reader_queue& queue) :
	    _queue (queue) {}

	const_iterator begin() const {
	    if (!_queue.empty())
	    {
		auto* proc ((*_queue.begin()).second);
		const T* key (proc->next_key());

		if (key) return const_iterator(_queue);
		else return const_iterator();
	    }
	    else return const_iterator();
	}
	const const_iterator& end() const {return _end;}

    private:
	tmpfile_reader_queue& _queue;
	const_iterator _end;
    };
}

#endif
