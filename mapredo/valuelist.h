/* -*- C++ -*-
 * mapredo
 * Copyright (C) 2015 Kjell Irgens <hextremist@gmail.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 */

#ifndef _HEXTREME_MAPREDO_VALUELIST_H
#define _HEXTREME_MAPREDO_VALUELIST_H

#include <queue>

#include "tmpfile_reader.h"
#include "data_reader_queue.h"

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
	    iterator (data_reader_queue<T>& queue, const T key) :
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
			    _queue->push (proc);
			    _value = nproc->get_next_value();
			    //std::cerr << "V1:" << _value << "\n";
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
	    data_reader_queue<T>* _queue = nullptr;
	    int _index = -1;
	    T _key;
	    char* _value;
	};

	valuelist (data_reader_queue<T>& queue) :
	    _queue (queue) {}

	template<class U = T,
		 typename std::enable_if<std::is_fundamental<U>::value>
                 ::type* = nullptr>
	U get_key() {
	    auto key = _queue.top()->next_key();
	    if (key)
	    {
		_key = *key;
		return *key;
	    }
	    throw std::runtime_error
		("Attempted to valuelist::get_key() on an empty file");
	}

	template<class U = T, 
                 typename std::enable_if<std::is_same<U,char*>::value,
                                         bool>::type* = nullptr>
	char* get_key() {
	    _key_copy = *_queue.top()->next_key();
	    _key = const_cast<char*>(_key_copy.c_str());
	    return _key;
	}

	iterator begin() const {return iterator (_queue, _key);}
	const iterator& end() const {return _end;}

    private:

	data_reader_queue<T>& _queue;
	iterator _end;
	T _key = 0;
	std::string _key_copy;
    };
}

#endif
