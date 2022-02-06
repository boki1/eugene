#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <exception>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <variant>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <cppcoro/generator.hpp>

#include <cppitertools/zip.hpp>

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

struct BadTreeRemove : std::runtime_error {
	explicit BadTreeRemove(std::string_view msg) : std::runtime_error{fmt::format("Eugene: Bad tree remove {}", msg.data())} {}
};

struct BadTreeSearch : std::runtime_error {
	explicit BadTreeSearch(std::string_view msg) : std::runtime_error{fmt::format("Eugene: Bad tree search {}", msg.data())} {}
};

struct BadTreeInsert : std::runtime_error {
	explicit BadTreeInsert(std::string_view msg) : std::runtime_error{fmt::format("Eugene: Bad tree insert {}", msg.data())} {}
};

/// Action to undertake on B-tree construction
/// Denotes whether to load the metadata from storage, or to construct a new
/// bare one.
enum class ActionOnConstruction : std::uint8_t { Load,
	                                         Bare };

template<BtreeConfig Config = DefaultConfig>
class Btree {
	using Key = typename Config::Key;
	using Val = typename Config::Val;
	using Ref = typename Config::Ref;
	using Nod = Node<Config>;

	using PagerAllocatorPolicy = typename Config::PageAllocatorPolicy;
	using PagerEvictionPolicy = typename Config::PageEvictionPolicy;

	friend util::BtreePrinter<Config>;

	static inline constexpr auto APPLY_COMPRESSION = Config::APPLY_COMPRESSION;
	static inline constexpr auto PAGE_CACHE_SIZE = Config::PAGE_CACHE_SIZE;
	static inline constexpr auto BRANCHING_FACTOR_LEAF = Config::BRANCHING_FACTOR_LEAF;
	static inline constexpr auto BRANCHING_FACTOR_BRANCH = Config::BRANCHING_FACTOR_BRANCH;

	static inline constexpr std::uint32_t HEADER_MAGIC = 0xB75EEA41;

public:
	///
	/// Dependent types
	///

	/// Tree header
	/// Stores important metadata about the tree instance that must remain persistent
	/// Created by 'header()' and stored inside 'header_name()' file.
	///
	/// Note: Consider `magic` to be const. However, libnop refuses to compile the library that way.
	struct Header {
		std::uint32_t magic{HEADER_MAGIC};
		Position tree_rootpos;
		std::size_t tree_size;
		std::size_t tree_depth;
		long tree_num_leaf_records;
		long tree_num_branch_records;

		NOP_STRUCTURE(Header, magic, tree_rootpos, tree_size, tree_depth, tree_num_branch_records, tree_num_leaf_records);
	};

	/// Tree entry of the type <key, val>
	struct Entry {
		Key key;
		Val val;

		[[nodiscard]] auto operator<=>(const Entry &) const noexcept = default;
	};

private:
	///
	/// Helper functions
	///

	/// Wrapper for checking whether a node has too many elements
	[[nodiscard]] constexpr bool is_node_full(const Nod &node) {
		if (node.is_branch())
			return node.is_full(max_num_records_branch());
		return node.is_full(max_num_records_leaf());
	}

	/// Wrapper for checking whether a node has too few elements
	[[nodiscard]] constexpr bool is_node_underfull(const Nod &node) {
		if (node.is_branch())
			return node.is_underfull(min_num_records_branch());
		return node.is_underfull(min_num_records_leaf());
	}

	/// Wrapper for calling split API on a given node
	[[nodiscard]] constexpr auto node_split(Nod &node) {
		if (node.is_branch())
			return node.split(max_num_records_branch());
		return node.split(max_num_records_leaf());
	}

	/// Tree search logic
	/// Given a node, traverse it searching for a 'target_key' in the leaves below it.
	/// Acquire the value associated with it an return it. If no such <key, value> entry
	/// is found return an empty std::optional. If an error occurs during traversal,
	/// throw a 'BadTreeSearch' with a descriptive message.
	/// It gets called by both 'contains()' and 'get()'.
	[[nodiscard]] std::optional<Val> search_subtree(const Nod &node, const Key &target_key) {
		if (node.is_branch()) {
			const auto &refs = node.branch().m_refs;
			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), target_key) - refs.cbegin();
			if (node.branch().m_link_status[index] == LinkStatus::Inval)
				throw BadTreeSearch(fmt::format(" - invalid link w/ index={} pointing to pos={} in branch node\n", index, node.branch().m_links[index]));
			const Position pos = node.branch().m_links[index];
			const auto other = Nod::from_page(m_pager.get(pos));
			return search_subtree(other, target_key);
		}

		assert(node.is_leaf());

		const auto &keys = node.leaf().m_keys;
		const auto &vals = node.leaf().m_vals;
		const auto it = std::lower_bound(keys.cbegin(), keys.cend(), target_key);
		if (it == keys.cend() || *it != target_key)
			return {};
		return vals[it - keys.cbegin()];
	}

	/// Get node element positioned at the "corner" of the subtree
	/// The returned node contains either the keys with smallest values, or the biggest ones,
	/// depending on the value of corner.
	/// Used by 'get_min' and 'get_max'.

	enum class CornerDetail { MIN,
		                  MAX };

	[[nodiscard]] Nod get_corner_subtree(const Nod &node, CornerDetail corner) {
		if (node.is_leaf())
			return node;

		const auto &link_status = node.branch().m_link_status;
		const std::size_t index = [&] {
			if (corner == CornerDetail::MAX)
				return std::distance(std::cbegin(link_status), std::find(std::crbegin(link_status), std::crend(link_status), LinkStatus::Valid).base()) - 1;
			assert(corner == CornerDetail::MIN);
			return std::distance(std::cbegin(link_status), std::find(std::cbegin(link_status), std::cend(link_status), LinkStatus::Valid));
		}();
		if (index >= link_status.size() || link_status[index] != LinkStatus::Valid)
			throw BadTreeSearch(fmt::format(" - no valid link in node marked as branch\n"));
		const Position pos = node.branch().m_links[index];
		return get_corner_subtree(Nod::from_page(m_pager.get(pos)), corner);
	}

	/// Make tree root
	/// Create a new tree level
	/// This can either happen if the tree has no root element at all, or because a node
	/// split has been propagated up to the root node. Either way, a new root is created,
	/// initialized and stored, as well as other node (if such are created).

	/// Flag denoting whether to make an initial root, or to make it from the existing one.
	enum class MakeRootAction { BareInit,
		                    NewTreeLevel };

	Nod make_root(MakeRootAction action) {
		auto new_pos = m_pager.alloc();
		auto new_metadata = [&] {
			if (action == MakeRootAction::BareInit)
				return Nod::template metadata_ctor<typename Nod::Leaf>();

			auto old_root = root();
			auto old_pos = m_rootpos;
			old_root.set_parent(new_pos);
			old_root.set_root_status(Nod::RootStatus::IsInternal);

			auto [midkey, sibling] = node_split(old_root);
			auto sibling_pos = m_pager.alloc();
			old_root.set_next_node(sibling_pos);

			m_pager.place(sibling_pos, sibling.make_page());
			m_pager.place(old_pos, old_root.make_page());

			return Nod::template metadata_ctor<typename Nod::Branch>(std::vector<Ref>{midkey}, std::vector<Position>{old_pos, sibling_pos}, std::vector<LinkStatus>(2, LinkStatus::Valid));
		}();

		Nod new_root{std::move(new_metadata), new_pos, Nod::RootStatus::IsRoot};
		m_pager.place(new_pos, new_root.make_page());
		m_rootpos = new_pos;
		++m_depth;

		return new_root;
	}

	/// Balance tree out after performed removal operation
	/// Fix the tree invariants after a performed removal from 'node', place at position 'node_pos'.
	/// The following operations may be performed: if node's invariants are not violated- nothing,
	/// if the node is underfull, but any of its siblings are able to lend an entry- do that, and else-
	/// merge 'node' with one of its siblings. If a merge occurred, the function calls itself recursively,
	/// fixing any underflows occurring upwards in the tree.
	///
	/// TODO: Fix me
	void remove_rebalance(Nod &node, const Position node_pos, std::optional<std::size_t> node_idx_in_parent = {}, optional_cref<Key> key_to_remove = {}) {
		/// There is no need to check the upper levels if the current node is valid.
		/// And there is no more to check if the current node is the root node.
		if (!is_node_underfull(node) || node.is_root())
			return;

		/// If 'node' was root we would have already returned. Therefore it is safe to assume that
		/// 'node.parent()' contains a valid Position.
		Nod parent = Nod::from_page(m_pager.get(node.parent()));

		auto &parent_links = parent.branch().m_links;
		auto &parent_link_status = parent.branch().m_link_status;

		if (!node_idx_in_parent)
			node_idx_in_parent.emplace(std::find(parent_links.cbegin(), parent_links.cend(), node_pos) - parent_links.cbegin());
		if (*node_idx_in_parent >= parent_links.size())
			throw BadTreeRemove(fmt::format(" - [remove_rebalance] node_idx_in_parent (={}) is out of bounds for parent (@{}) and node (@{})", *node_idx_in_parent, node.parent(), node_pos));

		const bool has_left_sibling = *node_idx_in_parent > 0 && parent_link_status[*node_idx_in_parent - 1] == LinkStatus::Valid;
		const bool has_right_sibling = *node_idx_in_parent < parent.branch().m_links.size() - 1 && parent_link_status[*node_idx_in_parent + 1] == LinkStatus::Valid;

		enum class RelativeSibling { Left,
			                     Right };
		auto borrow_from_sibling = [&](RelativeSibling sibling_rel) {
			const auto sibling_idx = sibling_rel == RelativeSibling::Left ? *node_idx_in_parent - 1 : *node_idx_in_parent + 1;
			const Position sibling_pos = parent_links[sibling_idx];
			if (parent_link_status[sibling_idx] == LinkStatus::Valid)
				throw BadTreeRemove(fmt::format(" - link status of pos={} marks it as invalid\n", sibling_pos));
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
				parent_links[*node_idx_in_parent - 1] = merged_node_pos;
				parent_link_status[*node_idx_in_parent - 1] = LinkStatus::Valid;

				// Remove trailing link of previous 'right'. Expected the rest of the links to shift-left due to 'erase()'.
				parent_links.erase(parent_links.cbegin() + *node_idx_in_parent);
				parent_link_status.erase(parent_link_status.cbegin() + *node_idx_in_parent);

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

	/// Action flag to mark whether a <k,v> pair should be replaced if encountered during '???' operation.
	/// Used in order to provide a reasonable abstraction for update and insert APIs to call.
	enum class ActionOnKeyPresent { SubmitChange,
		                        AbandonChange };

	/// Create new <key, value> entry into the tree.
	/// Wraps insertion/replacement logic. If the key is already present into the tree, it takes action based
	/// on the 'action' flag passed. The returned value denotes whether a new entry was put into the tree.
	/// 'BadTreeInsert' may be thrown if an error occurs.
	/// It gets called by both 'insert()' and 'update()'.

	struct InsertedEntry {};
	struct InsertedNothing {};
	using InsertionReturnMark = std::variant<InsertedEntry, InsertedNothing>;

	[[nodiscard]] InsertionReturnMark place_kv_entry(const Key &key, const Val &val, ActionOnKeyPresent action) {
		return place_kv_entry(Entry{.key = key, .val = val}, action);
	}

	[[nodiscard]] InsertionReturnMark place_kv_entry(const Entry &entry, ActionOnKeyPresent action) {
		Position currpos{rootpos()};
		Nod curr{root()};

		if (is_node_full(curr))
			curr = make_root(MakeRootAction::NewTreeLevel);

		while (true) {
			if (curr.is_leaf()) {
				auto &keys = curr.leaf().m_keys;
				auto &vals = curr.leaf().m_vals;

				const std::size_t index = std::lower_bound(keys.cbegin(), keys.cend(), entry.key) - keys.cbegin();

				/// If we are called from 'insert' and the key is already present, do nothing,
				/// if we are called from 'update' and the key is _not_ already present, do nothing.
				const bool key_is_present = index < keys.size() && keys[index] == entry.key;
				if ((action == ActionOnKeyPresent::AbandonChange && key_is_present)
				    || (action == ActionOnKeyPresent::SubmitChange && !key_is_present))
					return InsertedNothing();

				keys.insert(keys.cbegin() + index, entry.key);
				vals.insert(vals.cbegin() + index, entry.val);
				m_pager.place(currpos, curr.make_page());
				++m_size;
				return InsertedEntry();
			}

			auto &refs = curr.branch().m_refs;
			auto &links = curr.branch().m_links;
			auto &link_status = curr.branch().m_link_status;

			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), entry.key) - refs.cbegin();
			const Position child_pos = links[index];
			if (link_status[index] == LinkStatus::Inval)
				throw BadTreeInsert(fmt::format(" - link status of pos={} marks it as invalid\n", child_pos));

			auto child = Nod::from_page(m_pager.get(child_pos));

			if (!is_node_full(child)) {
				currpos = child_pos;
				curr = std::move(child);
				continue;
			}

			auto [midkey, sibling] = node_split(child);
			auto sibling_pos = m_pager.alloc();
			child.set_next_node(sibling_pos);

			assert(std::find(refs.cbegin(), refs.cend(), midkey) == refs.cend());
			assert(std::find(links.cbegin(), links.cend(), sibling_pos) == links.cend());

			refs.insert(refs.begin() + index, midkey);
			links.insert(links.begin() + index + 1, sibling_pos);
			link_status.insert(link_status.begin() + index + 1, LinkStatus::Valid);

			m_pager.place(sibling_pos, std::move(sibling.make_page()));
			m_pager.place(child_pos, std::move(child.make_page()));
			m_pager.place(currpos, std::move(curr.make_page()));

			if (entry.key < midkey) {
				currpos = child_pos;
				curr = std::move(child);
			} else if (entry.key > midkey) {
				currpos = sibling_pos;
				curr = std::move(sibling);
			} else
				return InsertedNothing();
		}
	}

	/// Construct a new empty tree
	/// Initializes an empty root node leaf and calculates the appropriate value for 'm'
	constexpr void bare() {
		[[maybe_unused]] auto new_root = make_root(MakeRootAction::BareInit);

		/// FIXME: A q.a.d solution here.
		/// Space evaluation done here.
		/// Perform a binary search in the PAGE_SIZE range to calculate the maximum number of entries that could be stored.

		/// Make sure that when a leaf is split, its contents could be distributed among the two branch nodes.
		/// Number of entries in branch and leaf nodes may differ
		m_num_links_branch = BRANCHING_FACTOR_BRANCH > 0
		        ? BRANCHING_FACTOR_BRANCH
		        : ::internal::binsearch_primitive(2ul, PAGE_SIZE / 2, [](auto current, auto, auto) {
			          return nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Branch(std::vector<Ref>(current), std::vector<Position>(current), std::vector<LinkStatus>(current))), 10, Nod::RootStatus::IsInternal}) - PAGE_SIZE;
		          }).value_or(0);
		m_num_records_branch = m_num_links_branch - 1;

		auto num_records_leaf_candidate = BRANCHING_FACTOR_LEAF > 0
		        ? BRANCHING_FACTOR_LEAF
		        : ::internal::binsearch_primitive(1ul, PAGE_SIZE / 2, [](auto current, auto, auto) {
			          return nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Leaf(std::vector<Key>(current), std::vector<Val>(current))), 10, Nod::RootStatus::IsInternal}) - PAGE_SIZE;
		          }).value_or(0);

		m_num_records_leaf = num_records_leaf_candidate - 1 >= m_num_records_branch * 2
		        ? m_num_records_branch * 2 - 1
		        : num_records_leaf_candidate - 1;
	}

public:
	///
	/// Properties access API
	///

	/// Get root node of tree
	/// Somewhat expensive operation if cache is not hot, since a disk read has to be made.
	[[nodiscard]] Nod root() { return Nod::from_page(m_pager.get(rootpos())); }

	/// Get position of root node
	[[nodiscard]] const auto &rootpos() const noexcept { return m_rootpos; }

	/// Get header of tree
	[[nodiscard]] Header header() noexcept {
		return Header{
		        .magic = HEADER_MAGIC,
		        .tree_rootpos = rootpos(),
		        .tree_size = size(),
		        .tree_depth = depth(),
		        .tree_num_leaf_records = min_num_records_leaf(),
		        .tree_num_branch_records = min_num_records_branch()};
	}

	/// Get header name of tree
	[[nodiscard]] std::string_view header_name() const {
		static std::string hn = fmt::format("{}-header", m_identifier);
		return hn;
	}

	/// Get tree name
	[[nodiscard]] std::string_view name() const { return m_identifier; }

	/// Get tree size (# of items present)
	[[nodiscard]] std::size_t size() noexcept { return m_size; }

	/// Check if the tree is empty
	[[nodiscard]] bool empty() const noexcept { return size() == 0; }

	/// Get tree depth (max level of the tree)
	[[nodiscard]] std::size_t depth() { return m_depth; }

	/// Get limits of the size of leaf nodes (leaves contain [min; max] entries)
	[[nodiscard]] long min_num_records_leaf() const noexcept { return (m_num_records_leaf + 1) / 2; }
	[[nodiscard]] long max_num_records_leaf() const noexcept { return m_num_records_leaf; }

	/// Get limits of the size of branch nodes (branch contain [min; max] entries)
	[[nodiscard]] long min_num_records_branch() const noexcept { return (m_num_records_branch + 1) / 2; }
	[[nodiscard]] long max_num_records_branch() const noexcept { return m_num_records_branch; }

	///
	/// Operations API
	///

	/// Submit a new <key, value> entry into the tree
	/// In order for the change to be applied, it is a requirement that no such key is already
	/// associated with data in the tree.
	/// The returned value may contain either 'InsertedEntry' or 'InsertedNothing' depending on
	/// whether the key is distinct.
	/// An exception 'BadTreeInsert' may be thrown if an unexpected error occurs. It contains an
	/// appropriate message describing the failure.
	constexpr InsertionReturnMark insert(const Key &key, const Val &val) {
		return place_kv_entry(Entry{.key = key, .val = val}, ActionOnKeyPresent::AbandonChange);
	}

	constexpr InsertionReturnMark insert(const Entry &entry) {
		return place_kv_entry(entry, ActionOnKeyPresent::AbandonChange);
	}

	/// Remove an existing <key, value> entry from the tree
	/// If no such entry with the given key is found, no change is made to tree. The returned value
	/// may contain either 'RemovedVal(Val)' containing a copy of the removed value, or 'RemovedNothing()'
	/// if no such key was found. It is possible for 'BadTreeRemove' to be thrown if an unexpected
	/// error occurs.
	///
	/// Slight caveat: only the value is discarded from the tree. The key remains since it does not
	/// break in any way any of the tree invariants. It remains as an additional element to compare with
	/// in the branch nodes.
	struct RemovedVal {
		Val val;
	};
	struct RemovedNothing {};
	using RemovalReturnMark = std::variant<RemovedVal, RemovedNothing>;

	constexpr RemovalReturnMark remove(const Key &key) {
		Position currpos{rootpos()};
		Nod curr{root()};
		std::size_t curr_idx_in_parent = 0;

		std::optional<Val> removed;
		while (true) {
			if (curr.is_branch()) {
				const auto &refs = curr.branch().m_refs;
				const auto &links = curr.branch().m_links;
				const auto &link_status = curr.branch().m_link_status;

				curr_idx_in_parent = std::lower_bound(refs.cbegin(), refs.cend(), key) - refs.cbegin();
				const Position child_pos = links[curr_idx_in_parent];
				if (link_status[curr_idx_in_parent] == LinkStatus::Inval)
					throw BadTreeRemove(fmt::format(" - link status of pos={} marks it as invalid\n", child_pos));

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
			return RemovedVal{.val = *removed};
		return RemovedNothing();
	}

	/// Replace an existing <key, value> entry with a new <key, value2>
	/// If no such entry with the given key is found, 'InsertedNothing' is returned, else-
	/// 'InsertedEntry'. An exception 'BadTreeInsert' may be thrown if an unexpected error
	/// occurs. It contains an appropriate message describing the failure.
	constexpr InsertionReturnMark update(const Key &key, const Val &val) {
		return place_kv_entry(key, val, ActionOnKeyPresent::SubmitChange);
	}

	///
	/// Query API
	///

	/// Acquire the value associated with a given key
	/// Finds the entry described by 'key'. If no such entry is found, an empty std::optional<>.
	/// If an error occurs during the tree traversal 'BadTreeSearch' is thrown.
	constexpr std::optional<Val> get(const Key &key) {
		return search_subtree(root(), key);
	}

	/// Check whether <key, val> entry described by the given key is present in the tree
	/// If an error occurs during the tree traversal 'BadTreeSearch' is thrown.
	constexpr bool contains(const Key &key) {
		return search_subtree(root(), key).has_value();
	}

	/// Get the entry with the smallest key
	std::optional<Entry> get_min_entry() {
		const auto node_with_smallest_keys = get_corner_subtree(root(), CornerDetail::MIN);
		if (node_with_smallest_keys.num_filled() <= 0)
			return {};

		return std::make_optional<Entry>({.key = node_with_smallest_keys.leaf().m_keys.front(),
		                                  .val = node_with_smallest_keys.leaf().m_vals.front()});
	}

	/// Get the entry with the biggest key
	std::optional<Entry> get_max_entry() {
		const auto node_with_biggest_keys = get_corner_subtree(root(), CornerDetail::MAX);
		if (node_with_biggest_keys.num_filled() <= 0)
			return {};

		return std::make_optional<Entry>({.key = node_with_biggest_keys.leaf().m_keys.back(),
		                                  .val = node_with_biggest_keys.leaf().m_vals.back()});
	}

	/// Acquire all entries present in the tree
	/// This returns a generator over <key, val> pairs using the `next_node` member in the nodes.
	/// It does not traverse the whole tree, but only level 0.
	cppcoro::generator<const Entry &> get_all_entries() {
		Nod curr = get_corner_subtree(root(), CornerDetail::MIN);
		if (!curr.is_leaf())
			throw BadTreeSearch(" - returned branch corner node\n");

		while (true) {
			for (const auto &&[key, val] : iter::zip(curr.leaf().m_keys, curr.leaf().m_vals))
				co_yield Entry{.key = key, .val = val};
			if (!curr.next_node())
				co_return;
			curr = Nod::from_page(m_pager.get(*curr.next_node()));
		}
	}

	/// Similar to 'get_all_entries', but filters all entries by a given 'filter_function'.
	/// Additionally a "smart break" is provided. If 'wrap_up_function' returns true then the execution
	/// of the coroutine will be ended. This could be considered as a simple optimization which is not
	/// mandatory for regular queries on the tree.
	cppcoro::generator<const Entry &> get_all_entries_filtered(auto filter_function, std::optional<std::function<bool(const Entry &)>> wrap_up_function = {}) {
		for (const auto &entry : get_all_entries()) {
			if (filter_function(entry))
				co_yield entry;
			else if (wrap_up_function && (*wrap_up_function)(entry))
				co_return;
		}
	}

	/// Get all entries whose keys are in the range [key_min, key_max].
	cppcoro::generator<const Entry &> get_all_entries_in_key_range(const Key &key_min, const Key &key_max) {
		auto range_filter_function = [key_min, key_max](const Entry &entry) {
			return key_min <= entry.key && entry.key < key_max;
		};

		auto wrap_up_function = [key_max](const Entry &entry) {
			return entry.key >= key_max;
		};

		for (const auto &entry : get_all_entries_filtered(range_filter_function, std::make_optional(wrap_up_function)))
			co_yield entry;
	}

	///
	/// Persistence API
	///

	/// Load tree metadata from storage
	/// Reads from 'identifier' and 'identifier'-header and initializes the tree's metadata.
	void load() {
		Header header_;

		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{header_name().data()};
		if (!deserializer.Read(&header_))
			throw BadRead();

		m_rootpos = header_.tree_rootpos;
		m_size = header_.tree_size;
		m_depth = header_.tree_depth;
		m_num_records_leaf = header_.tree_num_leaf_records;
		m_num_records_branch = header_.tree_num_branch_records;
		m_num_links_branch = m_num_records_branch + 1;

		m_pager.load();
	}

	/// Store tree metadata to storage
	/// Stores tree's metadata inside files 'identifier' and 'identifier'-header.
	void save() {
		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{header_name().data(), std::ios::trunc};
		if (!serializer.Write(header()))
			throw BadWrite();

		m_pager.save();
	}

	/// Validity
	/// Check whether the tree object is valid
	/// Returns 'true' if it is alright and 'false' if not.
	constexpr bool sanity_check() const {
		return (m_num_records_leaf > 1)
		        && (m_num_records_branch > 1)
		        && (m_num_links_branch >= 2)
		        && (m_num_records_leaf - 1 >= m_num_records_branch * 2);
	}

public:
	Btree(std::string_view identifier = "/tmp/eu-btree-default", ActionOnConstruction action_on_construction = ActionOnConstruction::Bare) : m_pager{identifier}, m_identifier{identifier} {
		using enum ActionOnConstruction;

		// clang-format off
		switch (action_on_construction) {
		  break; case Load: load();
                  break; case Bare: bare();
		}
		// clang-format on

		/// For debug build, additionally check whether the initialized instance represent a valid B-tree
		//		assert(sanity_check());
	}

	Btree(const Btree &) = default;
	Btree(Btree &&) noexcept = default;

	Btree &operator=(const Btree &) = default;
	Btree &operator=(Btree &&) noexcept = default;

	virtual ~Btree() noexcept = default;

	auto operator<=>(const Btree &) const noexcept = default;

private:
	Pager<PagerAllocatorPolicy, PagerEvictionPolicy> m_pager;
	const std::string m_identifier;
	Position m_rootpos;
	std::size_t m_size{0};
	std::size_t m_depth{0};

	/// Minimum num of records stored in a leaf node
	std::size_t m_num_records_leaf{0};

	/// Minimum num of records stored in a branch node
	std::size_t m_num_records_branch{0};
	std::size_t m_num_links_branch{0};
};
}// namespace internal::storage::btree
