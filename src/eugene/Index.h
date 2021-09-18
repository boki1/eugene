#ifndef _EUGENE_INDEX_INCLUDED_
#define _EUGENE_INDEX_INCLUDED_

#include <internal/btree/Btree.h>

namespace indexing {

using namespace internal;

template<typename Config>
concept IndexConfig = btree::BtreeConfig<Config>;

template<IndexConfig config>
class Index {
	using Btree = btree::Btree<config>;
	using Storage = config::StorageDev;
	using Key = config::Key;
	using Val = config::Val;
	using EntryIterator = Btree::Iterator;

public:
	template<typename... Args>
	explicit Index(Args&& ... args)
			: m_btree(std::forward<Args>(args)...)
	{
	}

public:
	void sync() { }

	void load(std::string_view uid) { }

	EntryIterator get_entry(Key key) { }

	void add_entry(Key key, Val val) { }

	void del_entry(Key key) { }

private:
	std::string m_uid;
	Btree m_btree{Storage{}};
};

}

#endif // _EUGENE_INDEX_INCLUDED_