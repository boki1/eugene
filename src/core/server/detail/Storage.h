#pragma once

// make it generic with key and value
template <typename Key, typename Value>
class Storage {
	virtual void set(Key key, Value value) = 0;
	virtual Value get(Key key) = 0;
	virtual void remove(Key key) = 0;
};