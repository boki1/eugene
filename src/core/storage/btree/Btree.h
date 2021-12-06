#pragma once

#include <memory>
#include <optional>
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
	[[nodiscard]] bool is_node_full(const Self::Nod &node) { return node.is_full(NUM_RECORDS); }

	[[nodiscard]] auto node_split(Self::Nod &node) { return node.split(Config::NUM_RECORDS); }

	[[nodiscard]] auto search_subtree(const Self::Nod &node, const Self::Key &target_key) -> optional_cref<Val> {
		if (node.is_branch()) {
			const auto &refs = node.branch().m_refs;
			const std::size_t index = std::distance(refs.cbegin(), std::lower_bound(refs.cbegin(), refs.cend(), target_key));
			const Position pos = node.branch().m_links[index];
			const auto opt_node = Nod::from_pager(pgcache().get_page(pos));
			if (!opt_node)
				return {};
			return search_subtree(opt_node.value(), target_key);
		}
		assert(node.is_leaf());

		const auto &keys = node.leaf().m_keys;
		const auto it = std::lower_bound(keys.cbegin(), keys.cend(), target_key);
		if (it == keys.cend() || *it != target_key)
			return {};
		return node.leaf().m_vals[std::distance(keys.cbegin(), it)];
	}

public:
	[[nodiscard]] auto &pgcache() const noexcept { return m_pgcache; }

	[[nodiscard]] Self::Nod &root() noexcept {}
	[[nodiscard]] const Self::Nod &root() const noexcept {}

	void put(const Self::Key &key, const Self::Val &val) {
		(void) key;
		(void) val;
	}

	[[nodiscard]] auto get(const Self::Key &key) const noexcept -> optional_cref<Self::Val> {
		return search_subtree(root(), key);
	}

	[[nodiscard]] bool contains(const Self::Key &key) const noexcept {
		return search_subtree(root(), key).has_value();
	}

public:
	/*
	 * Every property of the tree is configured at compile-tume using
	 * the Config structure. Therefore, this ctor is supposed only
	 * to pass the name of the page cache to its own ctor.
	 */
	explicit Btree(std::string_view pgcache_name)
	    : m_pgcache{pgcache_name, Config::PAGE_CACHE_SIZE} {}

private:
	PageCache m_pgcache;

	Self::Nod *m_rootptr{nullptr};
	Position m_rootpos;
};

}// namespace internal::storage::btree
