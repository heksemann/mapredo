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

#ifndef _HEXTREME_MAPREDO_DATA_READER_QUEUE_H
#define _HEXTREME_MAPREDO_DATA_READER_QUEUE_H

//#include <type_traits>
#include <queue>

#include "data_reader.h"

/** This is a priority queue used when traversing files during merge */
template <class T> class data_reader_queue
{
public:
    data_reader_queue (const bool reverse = false) : _reverse (reverse) {}

    bool empty() {return (_reverse ? _rqueue.empty() : _queue.empty());}
    size_t size() {return (_reverse ? _rqueue.size() : _queue.size());}

    void push (data_reader<T>* const reader) {
	if (_reverse) _rqueue.push (reader);
	else _queue.push(reader);
    }

    data_reader<T>* const top() {
	return (_reverse ? _rqueue.top() : _queue.top());
    }
    void pop() {_reverse ? _rqueue.pop() : _queue.pop();}
    
private:
    template <class U> struct comparison
    {
	template<class V = U,
		 typename std::enable_if<std::is_fundamental<V>::value>
		 ::type* = nullptr>
	bool operator()(data_reader<U>* dr1, data_reader<U>* dr2) {
	    return *dr1->next_key() > *dr2->next_key();
	}

	template<class V = U,
		 typename std::enable_if<std::is_same<V,char*>::value,
					 bool>::type* = nullptr>
	bool operator()(data_reader<U>* dr1, data_reader<U>* dr2) {
	    return strcmp (*dr1->next_key(), *dr2->next_key()) > 0;
	}
    };

    template <class U> struct reverse_comparison
    {
	template<class V = U,
		 typename std::enable_if<std::is_fundamental<V>::value>
		 ::type* = nullptr>
	bool operator()(data_reader<U>* dr1, data_reader<U>* dr2) {
	    return *dr1->next_key() < *dr2->next_key();
	}

	template<class V = U,
		 typename std::enable_if<std::is_same<V,char*>::value,
					 bool>::type* = nullptr>
	bool operator()(data_reader<U>* dr1, data_reader<U>* dr2) {
	    return strcmp (*dr1->next_key(), *dr2->next_key()) < 0;
	}
    };

    bool _reverse;
    std::priority_queue<data_reader<T>*,
			std::vector<data_reader<T>*>,
			comparison<T>> _queue;
    std::priority_queue<data_reader<T>*,
			std::vector<data_reader<T>*>,
			reverse_comparison<T>> _rqueue;
};

#endif
