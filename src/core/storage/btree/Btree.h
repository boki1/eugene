#pragma once

#include <memory>
#include <optional>
#include <utility>

#include <gsl/pointers>

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

	[[nodiscard]] auto node_split(Self::Nod &node) { return node.split(NUM_RECORDS / 2); }


	[[nodiscard]] auto search_subtree(const Self::Nod &node, const Self::Key &target_key) const noexcept -> optional_cref<Val> {
		if (node.is_branch()) {
			const auto &refs = node.branch().m_refs;
			const std::size_t index = std::distance(refs.cbegin(), std::lower_bound(refs.cbegin(), refs.cend(), target_key));
			const Position pos = node.branch().m_links[index];
			const auto other = Nod::from_page(m_pgcache.get_page(pos));
			return search_subtree(other, target_key);
		}
		assert(node.is_leaf());

		const auto &keys = node.leaf().m_keys;
		const auto it = std::lower_bound(keys.cbegin(), keys.cend(), target_key);
		if (it == keys.cend() || *it != target_key)
			return {};
		return node.leaf().m_vals[std::distance(keys.cbegin(), it)];
	}

	void make_new_root() {
		auto &old_root = root();
		auto old_pos = m_rootpos;

		auto new_pos = m_pgcache.get_new_pos();

		old_root.set_parent(new_pos);
		old_root.set_root(false);

		auto [midkey, sibling] = old_root.split();
		auto sibling_pos = m_pgcache.get_new_pos();

		Nod new_root(typename Nod::Metadata(typename Nod::Branch({midkey}, {old_pos, sibling_pos})), new_pos, true);

		m_pgcache.put_page(new_pos, new_root.make_page());
		m_pgcache.put_page(old_pos, old_root.make_page());
		m_pgcache.put_page(sibling_pos, sibling.make_page());

		m_root = std::move(new_root);
	}

public:
	[[nodiscard]] auto &root() noexcept { return m_root; }
	[[nodiscard]] const auto &root() const noexcept { return m_root; }

	void put(const Self::Key &key, const Self::Val &val) {
		if (is_node_full(m_root))
			make_new_root();

		Nod *curr = &m_root;
		Position currpos = m_rootpos;

		while (1) {
			if (curr->is_leaf()) {
				auto &keys = curr->leaf().m_keys;
				auto &vals = curr->leaf().m_vals;
				const std::size_t index = std::distance(keys.begin(), std::lower_bound(keys.begin(), keys.end(), key));
				keys.insert(keys.begin() + index, key);
				vals.insert(vals.begin() + index, val);
				break;
			}

			auto &branch = curr->branch();
			const std::size_t index = std::distance(
					std::lower_bound(branch.m_refs.begin(), branch.m_refs.end(), key), branch.m_refs.begin());
			const Position child_pos = branch.m_links[index];
			auto child = Nod::from_page(m_pgcache.get_page(child_pos));

			if (!child.is_full(NUM_RECORDS)) {
				curr = new Nod{std::move(child)};
				currpos = m_rootpos;
				continue;
			}

			auto [midkey, sibling] = child.split();
			m_pgcache.put_page(child_pos, child.make_page());
			auto sibling_pos = m_pgcache.get_new_pos();
			m_pgcache.put_page(sibling_pos, sibling.make_page());

			auto refs_it = branch.m_refs.begin() + index;
			branch.m_refs.insert(refs_it, midkey);
			auto links_it = branch.m_links.begin() + index;
			branch.m_links.insert(links_it, sibling_pos);
			if (key < midkey) {
				curr = new Nod{std::move(child)};
				currpos = child_pos;
			} else {
				curr = new Nod{std::move(sibling)};
				currpos = sibling_pos;
			}
		}
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
	mutable PageCache m_pgcache;

	Position m_rootpos{Position()};
	Nod m_root{typename Nod::Metadata(typename Nod::Leaf({}, {})), m_rootpos, true};
};

}// namespace internal::storage::btree
