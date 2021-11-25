#pragma once

#include <memory>
#include <utility>

#include <core/Util.h>
#include <core/storage/Page.h>
#include <core/storage/PageCache.h>
#include <core/storage/btree/Config.h>
#include <core/storage/btree/Node.h>

namespace internal::storage::btree {

template<BtreeConfig Config = DefaultConfig>
class Btree final {
	using Self = Btree<Config>;

	using Key = typename Config::Key;
	using Val = typename Config::Val;
	using Ref = typename Config::Ref;
	using Nod = Node<Config>;

	using PosNod = std::pair<Position, Self::Nod>;

	inline static constexpr uint32_t NUM_RECORDS = Config::NUM_RECORDS;

private:
	[[nodiscard]] Self::Nod &root() const noexcept {}

	[[nodiscard]] bool is_node_full(const Self::Nod &node) { return node.is_full(NUM_RECORDS); }

	[[nodiscard]] auto node_split(Self::Nod &node) { return node.split(Config::NUM_RECORDS); }

public:
	void insert(const Self::Key &key, const Self::Val &val) {
		(void) key;
		(void) val;
	}

	[[nodiscard]] optional_cref<Self::Val> at(const Self::Key &key) const noexcept {
		(void) key;
		return {};
	}

	[[nodiscard]] bool contains(const Self::Key &key) const noexcept {
		(void) key;
		return false;
	}

public:
	/*
	 * Every property of the tree is configured at compile-tume using
	 * the Config structure. Therefore, this ctor is supposed only
	 * to pass the name of the page cache to its own ctor.
	 */
	Btree(std::string_view pgcache_name)
	    : m_pgcache{pgcache_name, Config::PAGE_CACHE_SIZE} {}

private:
	PageCache m_pgcache;

	Self::Nod *m_rootptr;
	Position m_rootpos;
};

}// namespace internal::storage::btree
