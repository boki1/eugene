#pragma once

#include <algorithm>
#include <cassert>
#include <exception>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <utility>
#include <variant>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/utility/die.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>

#include <core/Util.h>
#include <core/storage/Pager.h>
#include <core/storage/btree/Config.h>
#include <core/storage/btree/Node.h>

namespace internal::storage::btree {

namespace util {
template<BtreeConfig Config = DefaultConfig>
class BtreePrinter;
}

///
/// Each removal from the tree may result in one of the following outcomes:
/// 	1. Exception is thrown - `BadTreeRemove` with an appropriate message formatted. This happens only if an
///		   unexpected event occurs. It does not denote that no element was removed, but some condition under which no
///		   direct solution is implemented (e.g parent of a node being leaf, rather than branch).
/// 	2. RemovalReturnMark<>
///		   Algebraic type which denotes whether any element was removed and if so, what was its associated data. If
///		   such instance is returned, then the operation was successfully executed.
///		   2.1 RemovedVal<>
///			The pair with key matching the provied one was found and successfully erased from the tree. The associated
///			data could be found in '.val'
///		  2.2 RemovedNothing
///			No such item was found and nothing was removed.
///
struct BadTreeRemove : std::runtime_error {
	explicit BadTreeRemove(std::string_view msg) : std::runtime_error{fmt::format("Eugene: Bad tree remove {}", msg.data())} {}
};

template<typename Val>
struct RemovedVal {
	Val val;
};

struct RemovedNothing {
};

template<typename Val>
using RemovalReturnMark = std::variant<RemovedVal<Val>, RemovedNothing>;

template<BtreeConfig Config = DefaultConfig>
class Btree final {
	using Self = Btree<Config>;

	using Key = typename Config::Key;
	using Val = typename Config::Val;
	using Ref = typename Config::Ref;
	using Nod = Node<Config>;

	using PagerAllocatorPolicy = typename Config::PageAllocatorPolicy;
	using PagerEvictionPolicy = typename Config::PageEvictionPolicy;

	friend util::BtreePrinter<Config>;

public:
	//! Number of entries in branch and leaf nodes may differ
	//! Directly unwrap with `.value()` since we _want to fail at compile time_ in case their is no value which
	//! satisfies the predicates
	int NUM_LINKS_BRANCH = BRANCHING_FACTOR_BRANCH > 0 ? BRANCHING_FACTOR_BRANCH : ::internal::binsearch_primitive(2ul, PAGE_SIZE / 2, [](auto current, auto, auto) {
		                                                                               auto sz = nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Branch(std::vector<Ref>(current), std::vector<Position>(current))), 10, true});
		                                                                               return sz - PAGE_SIZE;
	                                                                               }).value_or(0);
	int NUM_RECORDS_BRANCH = NUM_LINKS_BRANCH - 1;

	//! Equivalent to `m` in Knuth's definition
	//! Make sure that when a leaf is split, its contents could be distributed among the two branch nodes.
	//! Directly unwrap with `.value()` since we _want to fail at compile time_ in case their is no value which
	//! satisfies the predicates
	int _NUM_RECORDS_LEAF = BRANCHING_FACTOR_LEAF > 0 ? BRANCHING_FACTOR_LEAF : ::internal::binsearch_primitive(1ul, PAGE_SIZE / 2, [](auto current, auto, auto) {
		                                                                            return nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Leaf(std::vector<Key>(current), std::vector<Val>(current))), 10, true}) - PAGE_SIZE;
	                                                                            }).value_or(0);
	int NUM_RECORDS_LEAF = _NUM_RECORDS_LEAF - 1 >= NUM_RECORDS_BRANCH * 2
	        ? NUM_RECORDS_BRANCH * 2 - 1
	        : _NUM_RECORDS_LEAF - 1;

private:
	static inline constexpr bool APPLY_COMPRESSION = Config::APPLY_COMPRESSION;
	static inline constexpr int PAGE_CACHE_SIZE = Config::PAGE_CACHE_SIZE;
	static inline constexpr int BRANCHING_FACTOR_LEAF = Config::BRANCHING_FACTOR_LEAF;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = Config::BRANCHING_FACTOR_BRANCH;

	static inline constexpr uint32_t MAGIC = 0xB75EEA41;

public:
	struct Header {
		Position m_rootpos;
		std::size_t m_size{};
		std::size_t m_depth{};
		uint32_t m_magic{MAGIC};
		uint32_t m_pgcache_size{PAGE_CACHE_SIZE};
		uint8_t m_apply_compression{APPLY_COMPRESSION};

		/*
		 * The storage locations of the tree header and the tree contents differ.
		 * This one stores the name of the file which contains the header of the tree,
		 * whereas the other name passed to the Btree constructor is the one where
		 * the actual nodes of the tree are stored.
		 */
		std::string m_content_file;

		/*
		 * We don't want to serialize this. It is used only to check whether the header
		 * we are currently possessing is valid.
		 */
		bool m_dirty{false};

		Header() = default;
		explicit Header(Position rootpos, std::size_t size, std::size_t depth, std::string_view content_file)
		    : m_rootpos{rootpos},
		      m_size{size},
		      m_depth{depth},
		      m_content_file{std::string{content_file}} {}

		[[nodiscard]] auto &rootpos() noexcept { return m_rootpos; }

		[[nodiscard]] auto &size() noexcept { return m_size; }

		[[nodiscard]] auto &depth() noexcept { return m_depth; }

		[[nodiscard]] auto &dirty() noexcept { return m_dirty; }

		auto operator<=>(const Header &) const noexcept = default;

		friend std::ostream &operator<<(std::ostream &os, const Header &h) {
			os << "Header { .rootpos = " << h.m_rootpos << ", .size =" << h.m_size << ", .depth =" << h.m_depth << " }";
			return os;
		}

		NOP_STRUCTURE(Header, m_rootpos, m_size, m_depth, m_magic, m_pgcache_size, m_apply_compression, m_content_file);
	};

private:
	[[nodiscard]] constexpr bool is_node_full(const Self::Nod &node) {
		if (node.is_branch())
			return node.is_full(max_num_records_branch());
		return node.is_full(max_num_records_leaf());
	}

	[[nodiscard]] constexpr bool is_node_underfull(const Self::Nod &node) {
		if (node.is_branch())
			return node.is_underfull(min_num_records_branch());
		return node.is_underfull(min_num_records_leaf());
	}

	[[nodiscard]] constexpr auto node_split(Self::Nod &node) {
		if (node.is_branch())
			return node.split(max_num_records_branch());
		return node.split(max_num_records_leaf());
	}

	[[nodiscard]] constexpr std::optional<Val> search_subtree(const Self::Nod &node, const Self::Key &target_key) {
		if (node.is_branch()) {
			const auto &refs = node.branch().m_refs;
			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), target_key) - refs.cbegin();
			const Position pos = node.branch().m_links[index];
			const auto other = Nod::from_page(m_pager.get(pos));
			return search_subtree(other, target_key);
		}

		// assert(node.is_leaf());

		const auto &keys = node.leaf().m_keys;
		const auto &vals = node.leaf().m_vals;
		const auto it = std::lower_bound(keys.cbegin(), keys.cend(), target_key);
		if (it == keys.cend() || *it != target_key)
			return {};
		return vals[it - keys.cbegin()];
	}

	Nod make_new_root() {
		auto old_root = root();
		auto old_pos = m_rootpos;

		auto new_pos = m_pager.alloc();

		old_root.set_parent(new_pos);
		old_root.set_root(false);

		auto [midkey, sibling] = node_split(old_root);
		auto sibling_pos = m_pager.alloc();

		Nod new_root{typename Nod::Metadata(typename Nod::Branch({midkey}, {old_pos, sibling_pos})), new_pos, true};

		m_pager.place(new_pos, new_root.make_page());
		m_pager.place(old_pos, old_root.make_page());
		m_pager.place(sibling_pos, sibling.make_page());

		m_rootpos = new_pos;
		++m_depth;
		m_header.m_dirty = true;

		return new_root;
	}

	void remove_rebalance(Nod &node, const Position node_pos, std::optional<std::size_t> node_idx_in_parent = {}, optional_cref<Key> key_to_remove = {}) {
		/// There is no need to check the upper levels if the current node is valid.
		/// And there is no more to check if the current node is the root node.
		if (!is_node_underfull(node) || node.is_root())
			return;

		/// If 'node' was root we would have already returned. Therefore it is safe to assume that
		/// 'node.parent()' contains a valid Position.
		Nod parent = Nod::from_page(m_pager.get(node.parent()));

		const auto &parent_links = parent.branch().m_links;
		if (!node_idx_in_parent)
			node_idx_in_parent.emplace(std::find(parent_links.cbegin(), parent_links.cend(), node_pos) - parent_links.cbegin());
		if (*node_idx_in_parent >= parent_links.size())
			throw BadTreeRemove(fmt::format(" - [remove_rebalance] node_idx_in_parent (={}) is out of bounds for parent (@{}) and node (@{})", *node_idx_in_parent, node.parent(), node_pos));

		/* TODO: Link value 0 as of now is a valid link position.
		   This means that if a node is deleted and the link is replaced by 0, it will seem as if the link is valid. */
		const bool has_left_sibling = *node_idx_in_parent > 0;
		const bool has_right_sibling = *node_idx_in_parent < parent.branch().m_links.size() - 1;

		enum class RelativeSibling { Left,
			                     Right };
		auto borrow_from_sibling = [&](RelativeSibling sibling_rel) {
			const auto sibling_idx = sibling_rel == RelativeSibling::Left ? *node_idx_in_parent - 1 : *node_idx_in_parent + 1;
			const Position sibling_pos = parent.branch().m_links[sibling_idx];
			Nod sibling = Nod::from_page(m_pager.get(sibling_pos));

			if ((sibling.is_branch() && sibling.num_filled() <= min_num_records_branch())
			    || (sibling.is_leaf() && sibling.num_filled() <= min_num_records_leaf()))
				return;

			/// If we are borrowing from the left sibling, then we need its biggest key. Otherwise, if we are borrowing
			/// from the right sibling, then we need its smallest key in order to not violate the tree properties.
			/// If we borrow the last element of the sibling (its biggest one), then we should put that as node's first,
			/// since it's smaller than all elements in node.
			const auto borrowed_idx = sibling_rel == RelativeSibling::Left ? sibling.num_filled() - 1 : 0;
			const auto borrowed_dest_idx = sibling_rel == RelativeSibling::Left ? 0 : node.num_filled();

			if (node.is_leaf()) {
				node.leaf().m_vals.insert(node.leaf().m_vals.cbegin() + borrowed_dest_idx, sibling.leaf().m_vals.at(borrowed_idx));
				node.leaf().m_keys.insert(node.leaf().m_keys.cbegin() + borrowed_dest_idx, sibling.leaf().m_keys.at(borrowed_idx));
				sibling.leaf().m_vals.erase(sibling.leaf().m_vals.cbegin() + borrowed_idx);
				sibling.leaf().m_keys.erase(sibling.leaf().m_keys.cbegin() + borrowed_idx);
			} else {
				// TODO
				// We are currently propagating a merge operation upwards in the tree.
				abort();
			}
			const auto new_separator_key = [&] {
				if (sibling_rel == RelativeSibling::Left)
					return sibling.items().at(borrowed_idx - 1);
				return node.items().at(borrowed_dest_idx);
			}();
			parent.items()[*node_idx_in_parent] = new_separator_key;

			m_pager.place(node.parent(), parent.make_page());
			m_pager.place(node_pos, node.make_page());
			m_pager.place(sibling_pos, sibling.make_page());
		};

		if (has_left_sibling)
			borrow_from_sibling(RelativeSibling::Left);
		if (is_node_underfull(node) && has_right_sibling)
			borrow_from_sibling(RelativeSibling::Right);

		/// We tried borrowing a single element from the siblings and that successfully
		/// rebalanced the tree after the removal.
		if (!is_node_underfull(node))
			return;

		/// There is a chance that the last element in 'node' is the same as the 'separator_key'. Duplicates are unwanted.
		if (const auto separator_key = parent.branch().m_refs[*node_idx_in_parent]; separator_key != node.leaf().m_keys.back()) {
			/// If the separator_key is the same as the key we removed just before calling `remove_rebalance()` we would not like to keep duplicates, so skip adding it.
			if (key_to_remove.has_value() && separator_key != *key_to_remove)
				node.leaf().m_keys.push_back(separator_key);
		}
		parent.branch().m_refs.pop_back();

		auto make_merged_node_from = [&](const Nod &left, const Nod &right) {
			if (auto maybe_merged_node = left.merge_with(right); maybe_merged_node) {
				const auto merged_node_pos = m_pager.alloc();
				// Replace previous 'left' link with new merged node
				parent.branch().m_links[*node_idx_in_parent - 1] = merged_node_pos;
				// Remove trailing link of previous 'right'. Expected the rest of the links to shift-left due to 'erase()'.
				parent.branch().m_links.erase(parent.branch().m_links.cbegin() + *node_idx_in_parent);
				parent.branch().m_refs[*node_idx_in_parent - 1] = maybe_merged_node->leaf().m_keys.back();
				m_pager.place(merged_node_pos, maybe_merged_node->make_page());
			}
		};

		if (has_left_sibling)
			make_merged_node_from(Nod::from_page(m_pager.get(*node_idx_in_parent - 1)), node);
		else if (has_right_sibling)
			make_merged_node_from(node, Nod::from_page(m_pager.get(*node_idx_in_parent + 1)));

		remove_rebalance(parent, node.parent());
	}

public:
	[[nodiscard]] auto root() { return Nod::from_page(m_pager.get(rootpos())); }

	[[nodiscard]] auto &header() noexcept { return m_header; }

	[[nodiscard]] std::string header_name() const noexcept { return fmt::format("{}-header", m_identifier); }

	[[nodiscard]] const auto &rootpos() const noexcept { return m_rootpos; }

	[[nodiscard]] std::size_t size() noexcept { return m_size; }

	[[nodiscard]] std::size_t size() const noexcept { return m_size; }

	[[nodiscard]] bool empty() const noexcept { return size() == 0; }

	[[nodiscard]] bool empty() noexcept { return size() == 0; }

	[[nodiscard]] std::size_t depth() { return m_depth; }

	[[nodiscard]] auto min_num_records_leaf() const noexcept { return (NUM_RECORDS_LEAF + 1) / 2; }
	[[nodiscard]] auto max_num_records_leaf() const noexcept { return NUM_RECORDS_LEAF; }

	[[nodiscard]] auto min_num_records_branch() const noexcept { return (NUM_RECORDS_BRANCH + 1) / 2; }
	[[nodiscard]] auto max_num_records_branch() const noexcept { return NUM_RECORDS_BRANCH; }

public:
	/*
	 *  Operations API
	 */

	void insert(const Self::Key &key, const Self::Val &val) {
		Position currpos{rootpos()};
		Nod curr{root()};

		if (is_node_full(curr))

			curr = make_new_root();

		while (true) {
			if (curr.is_leaf()) {
				auto &keys = curr.leaf().m_keys;
				auto &vals = curr.leaf().m_vals;

				const std::size_t index = std::lower_bound(keys.cbegin(), keys.cend(), key) - keys.cbegin();
				if (!keys.empty() && index < keys.size() && keys[index] == key)
					return;

				keys.insert(keys.begin() + index, key);
				vals.insert(vals.begin() + index, val);
				m_pager.place(currpos, curr.make_page());
				++m_size;
				break;
			}

			auto &refs = curr.branch().m_refs;
			auto &links = curr.branch().m_links;

			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), key) - refs.cbegin();
			const Position child_pos = links[index];
			auto child = Nod::from_page(m_pager.get(child_pos));

			if (!is_node_full(child)) {
				currpos = child_pos;
				curr = std::move(child);
				continue;
			}

			auto [midkey, sibling] = node_split(child);
			auto sibling_pos = m_pager.alloc();

			assert(std::find(refs.cbegin(), refs.cend(), midkey) == refs.cend());
			assert(std::find(links.cbegin(), links.cend(), sibling_pos) == links.cend());

			refs.insert(refs.begin() + index, midkey);
			links.insert(links.begin() + index + 1, sibling_pos);

			m_pager.place(sibling_pos, std::move(sibling.make_page()));
			m_pager.place(child_pos, std::move(child.make_page()));
			m_pager.place(currpos, std::move(curr.make_page()));

			if (key < midkey) {
				currpos = child_pos;
				curr = std::move(child);
			} else if (key > midkey) {
				currpos = sibling_pos;
				curr = std::move(sibling);
			}
		}
	}

	[[nodiscard]] RemovalReturnMark<Val> remove(const Self::Key &key) {
		Position currpos{rootpos()};
		Nod curr{root()};
		std::size_t curr_idx_in_parent = 0;

		std::optional<Val> removed;
		while (true) {
			if (curr.is_branch()) {
				auto &refs = curr.branch().m_refs;
				auto &links = curr.branch().m_links;

				curr_idx_in_parent = std::lower_bound(refs.cbegin(), refs.cend(), key) - refs.cbegin();
				const Position child_pos = links[curr_idx_in_parent];

				curr = Nod::from_page(m_pager.get(child_pos));
				currpos = child_pos;

				continue;
			}

			auto &keys = curr.leaf().m_keys;
			auto &vals = curr.leaf().m_vals;

			const std::size_t index = std::lower_bound(keys.cbegin(), keys.cend(), key) - keys.cbegin();
			if (index == keys.size() || keys[index] != key)
				break;

			removed = vals.at(index);
			keys.erase(keys.cbegin() + index);
			vals.erase(vals.cbegin() + index);
			m_pager.place(currpos, curr.make_page());
			--m_size;

			if (curr.is_root())
				break;

			auto parent = Nod::from_page(m_pager.get(curr.parent()));
			if (parent.is_leaf())
				throw BadTreeRemove("- parent of current is invalid");

			/// Performs any rebalance operations if needed.
			/// Calls itself recursively to check upper tree levels as well.
			remove_rebalance(curr, currpos, curr_idx_in_parent, key);

			break;
		}

		if (removed)
			return RemovedVal(*removed);
		return RemovedNothing();
	}

	[[nodiscard]] constexpr std::optional<Val> get(const Self::Key &key) {
		return search_subtree(root(), key);
	}

	[[nodiscard]] constexpr bool contains(const Self::Key &key) {
		return search_subtree(root(), key).has_value();
	}

private:
	/// Bare intialize an empty tree
	constexpr void bare() noexcept {
		m_rootpos = m_pager.alloc();
		Nod root_initial{typename Nod::Metadata(typename Nod::Leaf({}, {})), m_rootpos, true};
		m_pager.place(m_rootpos, std::move(root_initial.make_page()));

		// Fill in initial header
		m_header.rootpos() = m_rootpos;
		m_header.size() = m_size;
		m_header.depth() = m_depth;
	}

public:
	/*
	 *  Persistence API
	 */

	constexpr void load() {
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{header_name()};
		if (!deserializer.Read(&m_header))
			throw BadRead();

		m_rootpos = m_header.m_rootpos;
		m_size = m_header.m_size;
		m_depth = m_header.m_depth;

		m_pager.load();
	}

	constexpr void save() {
		if (m_header.dirty()) {
			m_header.rootpos() = m_rootpos;
			m_header.size() = m_size;
			m_header.dirty() = false;
		}

		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{header_name(), std::ios::trunc};
		if (!serializer.Write(m_header))
			throw BadWrite();

		m_pager.save();
	}

public:
	explicit Btree(std::string_view identifier, bool load_from_header = false)
	    : m_pager{identifier},
	      m_identifier{identifier} {

		// static_assert(NUM_LINKS_BRANCH >= 2);
		// static_assert(NUM_RECORDS_BRANCH > 1);
		// static_assert(NUM_RECORDS_LEAF > 1);

		load_from_header ? load() : bare();
	}

private:
	Pager<PagerAllocatorPolicy, PagerEvictionPolicy> m_pager;

	const std::string_view m_identifier;

	mutable Header m_header;

	Position m_rootpos;

	std::size_t m_size{0};

	std::size_t m_depth{0};
};
}// namespace internal::storage::btree
