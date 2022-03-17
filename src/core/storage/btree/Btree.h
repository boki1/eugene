#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <exception>
#include <fstream>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <stack>
#include <stdexcept>
#include <utility>

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

#include <core/Config.h>
#include <core/Util.h>
#include <core/storage/Pager.h>
#include <core/storage/IndirectionVector.h>
#include <core/storage/btree/Node.h>

/// Define configuration for a Btree of order m.
/// Used primarily in unit tests as of now, but it may come in handy in other situtations too.
/// Simple example usage:
///
/// class MyConfig : Config {
///     BTREE_OF_ORDER(3);
/// };
#define BTREE_OF_ORDER(m)\
	static inline constexpr int BRANCHING_FACTOR_LEAF = (m);\
	static inline constexpr int BRANCHING_FACTOR_BRANCH = (m)

namespace internal::storage::btree {

namespace util {
template<EugeneConfig Config = Config>
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
enum class ActionOnConstruction : std::uint8_t {
	Load,
	Bare,
	InMemoryOnly
};

/// Action flag to mark whether a <k,v> pair should be replaced if encountered during '???' operation.
/// Used in order to provide a reasonable abstraction for update and insert APIs to call.
enum class ActionOnKeyPresent { SubmitChange,
	                        AbandonChange };

template<EugeneConfig Config = Config>
class Btree {
	using Key = typename Config::Key;
	using Val = typename Config::Val;
	using RealVal = typename Config::RealVal;

	// If not using dyn entries, Val and RealVal are the same type.
	static_assert(std::same_as<Val, RealVal> == !Config::DYN_ENTRIES);

	using Ref = typename Config::Ref;
	using Nod = Node<Config>;

	using PagerAllocatorPolicy = typename Config::PageAllocatorPolicy;
	using PagerEvictionPolicy = typename Config::PageEvictionPolicy;
	using PagerType = typename Config::PagerType;

	friend util::BtreePrinter<Config>;

	static inline constexpr auto APPLY_COMPRESSION = Config::APPLY_COMPRESSION;
	static inline constexpr auto PAGE_CACHE_SIZE = Config::PAGE_CACHE_SIZE;
	static inline constexpr auto BRANCHING_FACTOR_LEAF = Config::BRANCHING_FACTOR_LEAF;
	static inline constexpr auto BRANCHING_FACTOR_BRANCH = Config::BRANCHING_FACTOR_BRANCH;

	static inline constexpr std::uint32_t HEADER_MAGIC = 0xB75EEA41;

	/// Same configuration as the provided, but non-persistent
	struct MemConfig : Config {
		static inline constexpr bool PERSISTENT = false;
		using PagerType = InMemoryPager<typename Config::PageAllocatorPolicy>;
	};

	using MemTree = Btree<MemConfig>;
	using MemNode = Node<MemConfig>;
	friend MemTree;
	friend MemNode;

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
	using Entry = typename Nod::Entry;

	/// Node, its position and its index in the parent links
	/// 'idx_in_parent' may be empty if the node has no parent. That is the case for every "root"
	/// node when calling 'search_subtree()'.
	/// 'idx_of_key' may be empty if the key we are searching for (usually we are searching for some key 😀) is
	/// not the node. If the node actually contains a 'ref' rather than 'key', 'idx_of_key' is still not set.
	struct PosNod {
		Position node_pos;
		std::optional<std::size_t> idx_in_parent;
		std::optional<std::size_t> idx_of_key;
	};

	/// Root-to-leaf path populated by a search operation
	using TreePath = std::stack<PosNod>;

	/// Insertion return type
	/// See 'place_kv_entry' for more information
	struct InsertedEntry {};
	struct InsertedNothing {};
	using InsertionReturnMark = std::variant<InsertedEntry, InsertedNothing>;
	/// The bulk version of `InsertionReturnMark`
	using ManyInsertionReturnMarks = std::unordered_map<Key, InsertionReturnMark>;

	/// Insertion tree is an in-memory instance of a Btree created during bulk-insertion
	/// following the algorithm implemented as 'insert_many()'. Check its definiton for
	/// a more in-depth look.
	template<typename Key>
	struct InsertionTree {
		TreePath path;
		Btree<Config> tree;
		Key lofence;
		Key hifence;
		Position leaf_pos;
	};

private:
	///
	/// Helper functions
	///

	/// Wrapper for checking whether a node has too many elements
	[[nodiscard]] constexpr bool is_node_over(const Nod &node) {
		if (node.is_branch())
			return node.is_over(max_num_records_branch());
		return node.is_over(max_num_records_leaf());
	}

	/// Wrapper for checking whether a node has too few elements
	[[nodiscard]] constexpr bool is_node_under(const Nod &node) {
		if (node.is_branch())
			return node.is_under(min_num_records_branch());
		return node.is_under(min_num_records_leaf());
	}

	/// Wrapper for calling split API on a given node
	[[nodiscard]] constexpr auto node_split(Nod &node, const SplitBias bias) {
		if (node.is_branch())
			return node.split(max_num_records_branch(), bias);
		return node.split(max_num_records_leaf(), bias);
	}

public:
	void fix_sibling_links(std::variant<Nod, Position> node_or_position, std::optional<Position> next_ = {}) {
		namespace rng = std::ranges;
		auto node = [&] {
			if (std::holds_alternative<Nod>(node_or_position))
				return std::get<Nod>(node_or_position);
			if (std::holds_alternative<Position>(node_or_position))
				return Nod::from_page(m_pager->get(std::get<Position>(node_or_position)));
			throw BadTreeAccess("node_or_position is neither node nor position");
		}();
		if (!node.is_branch())
			return;

		auto &br = node.branch();
		auto link_cbegin = br.links.cbegin();
		for (auto link_status_cbegin = br.link_status.cbegin(); std::distance(link_status_cbegin, br.link_status.cend()) > 1; ++link_status_cbegin) {
			if (*link_status_cbegin == LinkStatus::Valid) {
				auto curr_child = Nod::from_page(m_pager->get(*link_cbegin));
				auto maybe_curr_child_sibling_pos = [&]() -> std::optional<Position> {
					const std::size_t idx = std::find(br.link_status.cbegin(), br.link_status.cend(), LinkStatus::Valid) - br.link_status.cbegin();
					if (idx < br.link_status.size())
						return {br.links.at(idx)};
					if (next_)
						return {*next_};
					return std::nullopt;
				}();
				if (maybe_curr_child_sibling_pos)
					curr_child.set_next_node(*maybe_curr_child_sibling_pos);
				else
					break;
				++link_cbegin;
			}
		}
	}

private:
	/// Tree search logic
	/// Given a node, traverse it searching for a 'target_key' in the leaves below it. During the top-down traversal
	/// keep track of the visited nodes using a 'TreePath' instance. The return value contains the node where the
	/// 'target_key' may be found, or empty if it is not present in the tree. Additionally, the path of the traversal
	/// is provided to the called. 'key_expected_pos' contains the index where the key should be positioned and
	/// 'key_is_present' is a flag denoting whether that is actually the case. If an error occurs during traversal,
	/// throw a 'BadTreeSearch' with a descriptive message. It gets called by both 'contains()' and 'get()'.
	///
	/// Contracts. Guarantees that...
	///	- on exit, if no exception is thrown, the returned node is a leaf.
	struct SearchResultMark {
		Nod node;
		TreePath path;
		std::size_t key_expected_pos;
		bool key_is_present;
	};

	[[nodiscard]] SearchResultMark search_subtree(const Key &target_key, const Nod &origin, const Position origin_pos) {
		TreePath path;
		Nod curr = origin;
		Position curr_pos = origin_pos;
		std::optional<std::size_t> curr_idx_in_parent{};
		bool key_is_present = false;
		std::size_t key_expected_pos;

		while (true) {
			path.push(PosNod{
			        .node_pos = curr_pos,
			        .idx_in_parent = curr_idx_in_parent,
			        .idx_of_key = {}});

			if (curr.is_branch()) {
				const auto &branch_node = curr.branch();
				const std::size_t index = [&] {
					auto it = std::lower_bound(branch_node.refs.cbegin(), branch_node.refs.cend(), target_key);
					return it - branch_node.refs.cbegin() + (it != branch_node.refs.cend() && *it == target_key);
				}();

				if (branch_node.link_status[index] == LinkStatus::Inval)
					throw BadTreeSearch(fmt::format("- invalid link w/ index={} pointing to pos={} in branch node\n", index, curr.branch().links[index]));
				curr_idx_in_parent = index;
				curr_pos = branch_node.links[index];
				curr = Nod::from_page(m_pager->get(curr_pos));
			} else if (curr.is_leaf()) {
				const auto &leaf_node = curr.leaf();
				key_expected_pos = std::lower_bound(leaf_node.keys.cbegin(), leaf_node.keys.cend(), target_key) - leaf_node.keys.cbegin();
				key_is_present = key_expected_pos < leaf_node.keys.size() && leaf_node.keys[key_expected_pos] == target_key;
				if (key_is_present)
					path.top().idx_of_key = key_expected_pos;

				break;
			} else
				throw BadTreeSearch(fmt::format("- node @{} is neither branch nor leaf", curr_pos));
		}

		if (!curr.is_leaf())
			throw BadTreeSearch("- branch as final visited node");

		return SearchResultMark{
		        .node = curr,
		        .path = path,
		        .key_expected_pos = key_expected_pos,
		        .key_is_present = key_is_present};
	}

	[[nodiscard]] SearchResultMark search(const Key &target_key) {
		return search_subtree(target_key, root(), rootpos());
	}

	/// Get node element positioned at the "corner" of the subtree
	/// The returned node contains either the keys with smallest values, or the biggest ones,
	/// depending on the value of corner.
	/// Used by 'get_min' and 'get_max'.

	enum class CornerDetail { MIN,
		                  MAX };

	const static auto LEAF_LEVEL_HEIGHT = 0ul;

	[[nodiscard]] std::pair<Nod, Position> get_corner_subtree_at_height(const Nod &node_, CornerDetail corner, size_t height = LEAF_LEVEL_HEIGHT) {
		auto curr_height = depth() - 1;
		auto node = node_;
		Position node_pos;
		while (!(node.is_leaf() || curr_height-- <= height)) {
			const auto &link_status = node.branch().link_status;
			const std::size_t index = [&] {
				if (corner == CornerDetail::MAX)
					return std::distance(std::cbegin(link_status), std::find(std::crbegin(link_status), std::crend(link_status), LinkStatus::Valid).base()) - 1;
				assert(corner == CornerDetail::MIN);
				return std::distance(std::cbegin(link_status), std::find(std::cbegin(link_status), std::cend(link_status), LinkStatus::Valid));
			}();
			if (index >= link_status.size() || link_status[index] != LinkStatus::Valid)
				throw BadTreeSearch(fmt::format(" - no valid link in node marked as branch\n"));
			node_pos = node.branch().links[index];
			node = Nod::from_page(m_pager->get(node_pos));
		}
		return {std::move(node), node_pos};
	}

	[[nodiscard]] Nod get_corner_subtree(const Nod &node, CornerDetail corner) {
		return get_corner_subtree_at_height(node, corner).first;
	}

	/// Make tree root
	/// Create a new tree level
	/// This can either happen if the tree has no root element at all, or because a node
	/// split has been propagated up to the root node. Either way, a new root is created,
	/// initialized and stored, as well as other node (if such are created).

	/// Flag denoting whether to make an initial root, or to make it from the existing one.
	enum class MakeRootAction { BareInit,
		                    NewTreeLevel,
		                    DuringBulkRebalancing };

	Nod make_root(MakeRootAction action) {
		auto new_pos = m_pager->alloc();
		auto new_metadata = [&] {
			if (action == MakeRootAction::BareInit)
				return Nod::template metadata_ctor<typename Nod::Leaf>();

			auto old_root = root();
			auto old_pos = m_rootpos;
			old_root.set_parent(new_pos);
			old_root.set_root_status(Nod::RootStatus::IsInternal);

			auto [midkey, sibling] = node_split(old_root, SplitBias::DistributeEvenly);
			auto sibling_pos = m_pager->alloc();
			old_root.set_next_node(sibling_pos);

			m_pager->place(sibling_pos, sibling.make_page());
			m_pager->place(old_pos, old_root.make_page());

			return Nod::template metadata_ctor<typename Nod::Branch>(std::vector<Ref>{midkey}, std::vector<Position>{old_pos, sibling_pos}, std::vector<LinkStatus>(2, LinkStatus::Valid));
		}();

		Nod new_root{std::move(new_metadata), new_pos, Nod::RootStatus::IsRoot};
		m_pager->place(new_pos, new_root.make_page());
		m_rootpos = new_pos;
		++m_depth;

		return new_root;
	}

	void rebalance_after_insert(TreePath &visited, const SplitBias bias) {
		std::optional<Nod> node;
		while (true) {
			const PosNod &path_of_node = visited.top();
			if (!node)
				node = Nod::from_page(m_pager->get(path_of_node.node_pos));

			/// Having the current node valid, means that the upper levels of the tree are fine as well.
			if (!is_node_over(*node))
				break;

			if (node->is_root()) {
				[[maybe_unused]] const auto new_root = make_root(MakeRootAction::NewTreeLevel);
				break;
			}

			auto [midkey, sibling] = node_split(*node, bias);
			auto sibling_pos = m_pager->alloc();
			node->set_next_node(sibling_pos);
			/// TODO: Can 'sibling' already have a right neighbor?

			visited.pop();
			const PosNod &path_of_parent = visited.top();
			auto parent = Nod::from_page(m_pager->get(path_of_parent.node_pos));

			/// Safety: 'path_of_node.idx_in_parent' is guaranteed to contain a value since '*node' is not root.
			const auto idx = path_of_node.idx_in_parent.value();

			parent.branch().refs.insert(parent.branch().refs.cbegin() + idx, midkey);
			parent.branch().links.insert(parent.branch().links.cbegin() + idx + 1, sibling_pos);
			parent.branch().link_status.insert(parent.branch().link_status.cbegin() + idx + 1, LinkStatus::Valid);

			m_pager->place(sibling_pos, sibling.make_page());
			m_pager->place(path_of_node.node_pos, node->make_page());
			m_pager->place(path_of_parent.node_pos, parent.make_page());

			/// Cache next node that will be looked at
			node = parent;
		}
	}

	void rebalance_after_bulk_insert(std::vector<InsertionTree<Key>> &insertion_trees) {
		// No rebalancing has to be performed, since we are not added any auxiliary nodes
		for (auto &instree : std::views::filter(insertion_trees, [](auto &instree) {
			     // We do not perform rebalancing on insertion trees with max depth = 1, since not additional nodes are emplaced
			     // and insertion trees whose root element has no parent- this means that the bulk is placed in an initially empty
			     // (or almost empty) tree.
			     return instree.tree.depth() > 1 && instree.path.size() > 1;
		     })) {
			const PosNod path_to_instree_root = consume_back<PosNod, TreePath>(instree.path);
			auto instree_root = Nod::from_page(m_pager->get(path_to_instree_root.node_pos));
			const PosNod path_to_p = consume_back<PosNod, TreePath>(instree.path);
			auto ppos = path_to_p.node_pos;
			auto p = Nod::from_page(m_pager->get(ppos));

			for (auto height = 1ul; height < instree.tree.depth() - 1; ++height) {
				/// Deref safety: We already filtered insertion trees without parents and fetched this insertion tree's parent
				const auto idx = std::min(*path_to_instree_root.idx_in_parent - 1, 0ul);
				auto [_, right_sibling_of_p] = p.split(idx, SplitBias::TakeLiterally, SplitType::ExplodeOnly);
				auto right_sibling_of_p_pos = m_pager->alloc();
				p.set_next_node(right_sibling_of_p_pos);

				// Removes the insertion tree link from its parent 'p'
				right_sibling_of_p.branch().links.erase(right_sibling_of_p.branch().links.cbegin());

				auto [smallest_in_instree, smallest_in_instree_pos] = instree.tree.get_corner_subtree_at_height(instree.tree.root(), CornerDetail::MIN, height);
				auto [biggest_in_instree, biggest_in_instree_pos] = instree.tree.get_corner_subtree_at_height(instree.tree.root(), CornerDetail::MAX, height);
				p = p.fuse_with(smallest_in_instree);
				right_sibling_of_p = biggest_in_instree.fuse_with(right_sibling_of_p);

				fix_sibling_links({right_sibling_of_p_pos});
				fix_sibling_links(right_sibling_of_p);

				auto &l = instree_root.branch().links;
				if (auto fr = std::find(l.cbegin(), l.cend(), smallest_in_instree_pos); fr != l.cend())
					l.erase(fr);
				if (auto fr = std::find(l.cbegin(), l.cend(), biggest_in_instree_pos); fr != l.cend())
					l.erase(fr);

				l.push_back(ppos);
				l.push_back(right_sibling_of_p_pos);

				if (p.is_root()) {
					p.set_root_status(Nod::RootStatus::IsInternal);
					m_rootpos = path_to_instree_root.node_pos;
					instree_root.set_root_status(Nod::RootStatus::IsRoot);
				}

				m_pager->place(right_sibling_of_p_pos, right_sibling_of_p.make_page());
				m_pager->place(ppos, p.make_page());
				m_pager->place(path_to_instree_root.node_pos, instree_root.make_page());
			}
		}
	}

	/// Balance tree out after performed removal operation
	/// Fix the tree invariants after a performed removal from 'node', place at position 'node_pos'.
	/// The following operations may be performed: if node's invariants are not violated- nothing,
	/// if the node is underfull, but any of its siblings are able to lend an entry- do that, and else-
	/// merge 'node' with one of its siblings. If a merge occurred, the function calls itself recursively,
	/// fixing any underflows occurring upwards in the tree.
	///
	/// FIXME: Use new TreePath API
	[[maybe_unused]] void rebalance_after_remove(Nod &node, const Position node_pos, std::optional<std::size_t> node_idx_in_parent = {}, optional_cref<Key> key_to_remove = {}) {
		/// There is no need to check the upper levels if the current node is valid.
		/// And there is no more to check if the current node is the root node.
		if (!is_node_under(node) || node.is_root())
			return;

		/// If 'node' was root we would have already returned. Therefore it is safe to assume that
		/// 'node.parent()' contains a valid Position.
		Nod parent = Nod::from_page(m_pager->get(node.parent()));

		auto &parent_links = parent.branch().links;
		auto &parent_link_status = parent.branch().link_status;

		if (!node_idx_in_parent)
			node_idx_in_parent.emplace(std::find(parent_links.cbegin(), parent_links.cend(), node_pos) - parent_links.cbegin());
		if (*node_idx_in_parent >= parent_links.size())
			throw BadTreeRemove(fmt::format(" - [rebalance_after_remove] node_idx_in_parent (={}) is out of bounds for parent (@{}) and node (@{})", *node_idx_in_parent, node.parent(), node_pos));

		const bool has_left_sibling = *node_idx_in_parent > 0 && parent_link_status[*node_idx_in_parent - 1] == LinkStatus::Valid;
		const bool has_right_sibling = *node_idx_in_parent < parent.branch().links.size() - 1 && parent_link_status[*node_idx_in_parent + 1] == LinkStatus::Valid;

		enum class RelativeSibling { Left,
			                     Right };
		auto borrow_from_sibling = [&](RelativeSibling sibling_rel) {
			const auto sibling_idx = sibling_rel == RelativeSibling::Left ? *node_idx_in_parent - 1 : *node_idx_in_parent + 1;
			const Position sibling_pos = parent_links[sibling_idx];
			if (parent_link_status[sibling_idx] == LinkStatus::Valid)
				throw BadTreeRemove(fmt::format(" - link status of pos={} marks it as invalid\n", sibling_pos));
			Nod sibling = Nod::from_page(m_pager->get(sibling_pos));

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
				node.leaf().vals.insert(node.leaf().vals.cbegin() + borrowed_dest_idx, sibling.leaf().vals.at(borrowed_idx));
				node.leaf().keys.insert(node.leaf().keys.cbegin() + borrowed_dest_idx, sibling.leaf().keys.at(borrowed_idx));
				sibling.leaf().vals.erase(sibling.leaf().vals.cbegin() + borrowed_idx);
				sibling.leaf().keys.erase(sibling.leaf().keys.cbegin() + borrowed_idx);
			} else {
				// We are currently propagating a merge operation upwards in the tree.
			}
			const auto new_separator_key = [&] {
				if (sibling_rel == RelativeSibling::Left)
					return sibling.items().at(borrowed_idx - 1);
				return node.items().at(borrowed_dest_idx);
			}();
			parent.items()[*node_idx_in_parent] = new_separator_key;

			m_pager->place(node.parent(), parent.make_page());
			m_pager->place(node_pos, node.make_page());
			m_pager->place(sibling_pos, sibling.make_page());
		};

		if (has_left_sibling)
			borrow_from_sibling(RelativeSibling::Left);
		if (is_node_under(node) && has_right_sibling)
			borrow_from_sibling(RelativeSibling::Right);

		/// We tried borrowing a single element from the siblings and that successfully
		/// rebalanced the tree after the removal.
		if (!is_node_under(node))
			return;

		/// There is a chance that the last element in 'node' is the same as the 'separator_key'. Duplicates are unwanted.
		if (const auto separator_key = parent.branch().refs[*node_idx_in_parent]; separator_key != node.leaf().keys.back()) {
			/// If the separator_key is the same as the key we removed just before calling `rebalance_after_remove()` we would not like to keep duplicates, so skip adding it.
			if (key_to_remove.has_value() && separator_key != *key_to_remove)
				node.leaf().keys.push_back(separator_key);
		}
		parent.branch().refs.pop_back();

		auto make_merged_node_from = [&](const Nod &left, const Nod &right) {
			if (auto maybe_merged_node = left.fuse_with(right); maybe_merged_node) {
				const auto merged_node_pos = m_pager->alloc();

				// Replace previous 'left' link with new merged node
				parent_links[*node_idx_in_parent - 1] = merged_node_pos;
				parent_link_status[*node_idx_in_parent - 1] = LinkStatus::Valid;

				// Remove trailing link of previous 'right'. Expected the rest of the links to shift-left due to 'erase()'.
				parent_links.erase(parent_links.cbegin() + *node_idx_in_parent);
				parent_link_status.erase(parent_link_status.cbegin() + *node_idx_in_parent);

				parent.branch().refs[*node_idx_in_parent - 1] = maybe_merged_node->leaf().keys.back();
				m_pager->place(merged_node_pos, maybe_merged_node->make_page());
			}
		};

		if (has_left_sibling)
			make_merged_node_from(Nod::from_page(m_pager->get(*node_idx_in_parent - 1)), node);
		else if (has_right_sibling)
			make_merged_node_from(node, Nod::from_page(m_pager->get(*node_idx_in_parent + 1)));

		rebalance_after_remove(parent, node.parent());
	}

	/// Balance tree out after performed removal operation following the _relaxed_ strategy
	/// The algorithm is described in "Deletion Without Rebalancing in Multiway Search Trees", 2009
	/// and proved to be efficient for most use cases.
	void rebalance_after_remove_relaxed(TreePath &search_path) {
		while (!search_path.empty()) {
			const PosNod &path_of_curr = search_path.top();

			auto node = Nod::from_page(m_pager->get(path_of_curr.node_pos));
			/// Empty nodes should be removed. However, we don't want to delete the root node.
			/// Keep it empty for potential future insertions.
			if (!node.is_empty() || node.is_root())
				return;

			// Delete node
			m_pager->free(path_of_curr.node_pos);
			search_path.pop();

			/// Delete link from parent

			/// Sadly, since we are "iterating" through the tree path bottom-to-top there is no way to backup the parent node.
			/// This should be fine, expecting that the cache is large enough to fit _at least all branch nodes_.
			auto parent_node = Nod::from_page(m_pager->get(search_path.top().node_pos));

			auto &parent_links = parent_node.branch().links;

			/// Safety: 'path_of_curr.idx_in_parent' has value which is guaranteed by the fact that the current node is not root
			parent_links.erase(parent_links.cbegin() + path_of_curr.idx_in_parent.value());
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

	/// Create new <key, value> entry into the tree.
	/// Wraps insertion/replacement logic. If the key is already present into the tree, it takes action based
	/// on the 'action' flag passed. The returned value denotes whether a new entry was put into the tree.
	/// 'BadTreeInsert' may be thrown if an error occurs.
	/// It gets called by both 'insert()' and 'update()'.

	[[nodiscard]] InsertionReturnMark place_kv_entry(const Key &key, const Val &val, ActionOnKeyPresent action = ActionOnKeyPresent::AbandonChange, SplitBias split_bias = SplitBias::DistributeEvenly) {
		return place_kv_entry(Entry{.key = key, .val = val}, action, split_bias);
	}

	[[nodiscard]] InsertionReturnMark place_kv_entry(const Entry &entry, ActionOnKeyPresent action = ActionOnKeyPresent::AbandonChange, SplitBias split_bias = SplitBias::DistributeEvenly) {
		/// Locate position
		auto search_res = search(entry.key);

		if ((action == ActionOnKeyPresent::AbandonChange && search_res.key_is_present)
		    || (action == ActionOnKeyPresent::SubmitChange && !search_res.key_is_present))
			return InsertedNothing();

		auto &leaf_node = search_res.node.leaf();
		/// Insert new element
		leaf_node.keys.insert(leaf_node.keys.cbegin() + search_res.key_expected_pos, entry.key);
		leaf_node.vals.insert(leaf_node.vals.cbegin() + search_res.key_expected_pos, set_value(entry.val));
		m_pager->place(search_res.path.top().node_pos, search_res.node.make_page());

		/// Update stats
		++m_size;

		/// Rebalance
		rebalance_after_insert(search_res.path, split_bias);

		return InsertedEntry();
	}

	/// Places _many_ <key, value> entries inside the tree.
	/// Leaves the tree in an unbalanced shape. More efficient version of calling `place_kv_entry` many times.
	/// Returns return marks for each <key, value> Entry and a collection of all insertion trees that were created.
	/// The latter is used during rebalancing.
	/// The client code could take advantage of this, by buffering the insertion/update queries.
	[[nodiscard]] auto place_kv_entries(std::ranges::range auto &&bulk) {
		namespace rng = std::ranges;

		ManyInsertionReturnMarks insertion_marks;
		std::vector<InsertionTree<Key>> insertion_trees;

		for (auto simple_bulk_cbegin = rng::cbegin(bulk); simple_bulk_cbegin != rng::cend(bulk);) {
			auto search_result = search(simple_bulk_cbegin->key);
			PosNod path_to_leaf = consume_back<PosNod>(search_result.path);
			std::optional<Position> parent_pos = search_result.path.empty() ? std::nullopt : std::make_optional(search_result.path.top().node_pos);

			const auto leaf_vals_cbegin = search_result.node.leaf().vals.cbegin();
			const auto leaf_keys_cbegin = search_result.node.leaf().keys.cbegin();
			const auto leaf_keys_cend = search_result.node.leaf().keys.cend();

			const auto simple_bulk_cend = [&] {
				if (leaf_keys_cbegin == leaf_keys_cend) {
					/// In such case, the leaf is empty, and since that would mean that the tree is not properly balanced, this means that the subtree is empty as well.
					/// Therefore, the highkey (or the upper fence key) equals +∞, meaning that all entries of the bulk should be located in this leaf.
					return rng::cend(bulk);
				}
				const Key &leaf_highkey = *(leaf_keys_cend - 1);
				return std::find_if(simple_bulk_cbegin, rng::cend(bulk), [&leaf_highkey](const auto &entry) { return entry.key > leaf_highkey; });
			}();

			auto entry_of_key_it = [leaf_keys_cbegin, leaf_vals_cbegin, this](auto it) {
				return Entry{.key = *it, .val = get_value(*(leaf_vals_cbegin + std::distance(leaf_keys_cbegin, it)))};
			};

			auto iterate_over_simple_bulk = [simple_bulk_cit = simple_bulk_cbegin, leaf_keys_cit = leaf_keys_cbegin, simple_bulk_cend, leaf_keys_cend, &entry_of_key_it]() mutable -> cppcoro::generator<const Entry> {
				while (simple_bulk_cit != simple_bulk_cend && leaf_keys_cit != leaf_keys_cend)
					if (simple_bulk_cit->key < *leaf_keys_cit)
						co_yield *simple_bulk_cit++;
					else
						co_yield entry_of_key_it(leaf_keys_cit++);
				while (leaf_keys_cit != leaf_keys_cend)
					co_yield entry_of_key_it(leaf_keys_cit++);
				while (simple_bulk_cit != simple_bulk_cend)
					co_yield *simple_bulk_cit++;
			};

			insertion_trees.push_back(InsertionTree{
			        .path = search_result.path,
			        .tree = clone_only_blueprint(),
			        .lofence = simple_bulk_cbegin->key,
			        .hifence = simple_bulk_cend > simple_bulk_cbegin ? (simple_bulk_cend - 1)->key : simple_bulk_cbegin->key,
			        .leaf_pos = path_to_leaf.node_pos});
			auto &insertion_tree = insertion_trees.back();

			rng::for_each(iterate_over_simple_bulk(), [&insertion_tree, &insertion_marks, this](const auto &entry) {
				insertion_marks[entry.key] = insertion_tree.tree.place_kv_entry(typename Nod::Entry{.key = entry.key, .val = entry.val}, ActionOnKeyPresent::AbandonChange, SplitBias::LeanLeft);
			});
			/// Replace leaf with insertion tree root
			auto pos = [&] {
				// No parent, nor siblings => root element
				// Do not reuse old root pos and do not deallocate it, because the pager is
				// shared between the insertion trees and the actual Btree we are modifying,
				// thus already making use of the space located at m_rootpos.
				if (!parent_pos)
					return (m_rootpos = insertion_tree.tree.rootpos());

				// Replace link value in parent
				const auto new_pos = m_pager->alloc();
				auto parent = Nod::from_page(m_pager->get(*parent_pos));
				// Deref safety: We just asserted that the node has a parent
				parent.branch().links[*path_to_leaf.idx_in_parent] = new_pos;
				m_pager->place(*parent_pos, parent.make_page());
				// Optionally, replace link value in sibling
				if (*path_to_leaf.idx_in_parent > 0) {
					auto prev_sibling_pos = parent.branch().links[*path_to_leaf.idx_in_parent - 1];
					auto prev_sibling = Nod::from_page(m_pager->get(prev_sibling_pos));
					prev_sibling.set_next_node(new_pos);
					m_pager->place(prev_sibling_pos, prev_sibling.make_page());
				}
				return (insertion_tree.leaf_pos = new_pos);
			}();
			m_pager->place(pos, insertion_tree.tree.root().make_page());

			// Update current position in simple bulk
			simple_bulk_cbegin = simple_bulk_cend;

			// Return the last record of the path since it was popped earlier.
			// However, update its nodepos to the current 'rootpos()' of the insertion tree.
			path_to_leaf.node_pos = insertion_tree.tree.rootpos();
			insertion_tree.path.push(path_to_leaf);
		}

		return std::make_pair(std::move(insertion_marks), std::move(insertion_trees));
	}

public:
	///
	/// Properties access API
	///

	/// Get root node of tree
	/// Somewhat expensive operation if cache is not hot, since a disk read has to be made.
	[[nodiscard]] Nod root() { return Nod::from_page(m_pager->get(rootpos())); }

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
	[[nodiscard]] bool empty() noexcept { return size() == 0; }

	/// Get tree depth (max level of the tree)
	[[nodiscard]] std::size_t depth() { return m_depth; }

	/// Get limits of the size of leaf nodes (leaves contain [min; max] entries)
	[[nodiscard]] long min_num_records_leaf() const noexcept { return (m_num_records_leaf + 1) / 2; }
	[[nodiscard]] long max_num_records_leaf() const noexcept { return m_num_records_leaf; }

	/// Get limits of the size of branch nodes (branch contain [min; max] entries)
	[[nodiscard]] long min_num_records_branch() const noexcept { return (m_num_records_branch + 1) / 2; }
	[[nodiscard]] long max_num_records_branch() const noexcept { return m_num_records_branch; }

	/// Get the internal pager
	[[nodiscard]] PagerType &pager() noexcept { return *m_pager.get(); }

	[[nodiscard]] auto& ind_vector() {
		using namespace ::internal::storage;
		if constexpr (!Config::DYN_ENTRIES)
			throw BadIndVector(" - Not using DYN_ENTRIES option");
		static IndirectionVector<Config> ind_vector_;
		return ind_vector_;
	}


	///
	/// Dynamic entries
	///

	[[nodiscard]] RealVal get_value(Val val_or_slot) {
		if constexpr (Config::DYN_ENTRIES) {
			return ind_vector().get_from_slot(val_or_slot);
		}
		return val_or_slot;
	}

	[[nodiscard]] Val set_value(RealVal val) {
		if constexpr (Config::DYN_ENTRIES) {
			return ind_vector().set_to_slot(val, sizeof(RealVal));
		}
		return val;
	}

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

	/// Submit a set of <key, value> entries into the tree, i.e bulk insertion.
	/// Requires that the entries in 'bulk' are sorted in ascending order.
	/// Implementation is according to "Concurrency Control and I/O-Optimality in Bulk Insertion".
	/// FIXME: Adapt 'ActionOnKeyPresent' by adding "AskOnEach" and "ReplaceAllPresent" and "IgnoreAllPresent".

	std::unordered_map<Key, InsertionReturnMark> insert_many(std::ranges::range auto &&bulk) {
		namespace rng = std::ranges;

		if (rng::empty(bulk))
			return {};

		auto &&[insertion_marks, insertion_trees] = place_kv_entries(bulk);
		rebalance_after_bulk_insert(insertion_trees);

		return insertion_marks;
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
		const Val val;
	};
	struct RemovedNothing {};
	using RemovalReturnMark = std::variant<RemovedVal, RemovedNothing>;

	RemovalReturnMark remove(const Key &key) {
		auto search_res = search(key);

		if (!search_res.key_is_present)// key is not in the tree, nothing to remove
			return RemovedNothing();

		auto &node_leaf = search_res.node.leaf();
		const auto &node_path = search_res.path.top();

		const Val removed = get_value(node_leaf.vals.at(search_res.key_expected_pos));

		/// Erase element
		node_leaf.keys.erase(node_leaf.keys.cbegin() + search_res.key_expected_pos);
		if constexpr (Config::DYN_ENTRIES) {
			auto slot_id = node_leaf.vals.cbegin() + search_res.key_expected_pos;
			return ind_vector().remove_slot(slot_id);
		}
		node_leaf.vals.erase(node_leaf.vals.cbegin() + search_res.key_expected_pos);
		m_pager->place(node_path.node_pos, search_res.node.make_page());

		/// Update stats
		--m_size;

		/// Performs any rebalance operations if needed.
		if constexpr (Config::BTREE_RELAXED_REMOVES)
			rebalance_after_remove_relaxed(search_res.path);
		else {
			// rebalance_after_remove(curr, currpos, curr_idx_in_parent, ke}y);
		}

		return RemovedVal{.val = removed};
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
	/// If an error occurs during the tree traversal 'BadTreeSea
	/// rch' is thrown.
	constexpr std::optional<RealVal> get(const Key &key) {
		const auto search_result = search(key);
		if (!search_result.key_is_present)
			return {};

		const auto &val = search_result.node.leaf().vals[search_result.key_expected_pos];
		return get_value(val);
	}

	/// Check whether <key, val> entry described by the given key is present in the tree
	/// If an error occurs during the tree traversal 'BadTreeSearch' is thrown.
	constexpr bool contains(const Key &key) {
		return search(key).key_is_present;
	}

	/// Get the entry with the smallest key
	std::optional<Entry> get_min_entry() {
		const auto node_with_smallest_keys = get_corner_subtree(root(), CornerDetail::MIN);
		if (node_with_smallest_keys.num_filled() <= 0)
			return {};

		return std::make_optional<Entry>({.key = node_with_smallest_keys.leaf().keys.front(),
		                                  .val = get_value(node_with_smallest_keys.leaf().vals.front())});
	}

	/// Get the entry with the biggest key
	std::optional<Entry> get_max_entry() {
		const auto node_with_biggest_keys = get_corner_subtree(root(), CornerDetail::MAX);
		if (node_with_biggest_keys.num_filled() <= 0)
			return {};

		return std::make_optional<Entry>({.key = node_with_biggest_keys.leaf().keys.back(),
		                                  .val = get_value(node_with_biggest_keys.leaf().vals.back())});
	}

	/// Acquire all entries present in the tree
	/// This returns a generator over <key, val> pairs using the `next_node` member in the nodes.
	/// It does not traverse the whole tree, but only level 0.
	cppcoro::generator<const Entry &> get_all_entries() {
		Nod curr = get_corner_subtree(root(), CornerDetail::MIN);
		if (!curr.is_leaf())
			throw BadTreeSearch(" - returned branch corner node\n");

		while (true) {
			for (const auto &&[key, val] : iter::zip(curr.leaf().keys, curr.leaf().vals))
				co_yield Entry{.key = key, .val = get_value(val)};
			if (!curr.next_node())
				co_return;
			curr = Nod::from_page(m_pager->get(*curr.next_node()));
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
		if constexpr (requires { m_pager->load(); }) {
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

			m_pager->load();
		}
	}

	/// Store tree metadata to storage
	/// Stores tree's metadata inside files 'identifier' and 'identifier'-header.
	void save() {
		if constexpr (requires { m_pager->save(); }) {
			nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{header_name().data(), std::ios::trunc};
			if (!serializer.Write(header()))
				throw BadWrite();

			m_pager->save();
		}
	}

	/// Validity
	/// Check whether the tree object is valid
	/// Returns 'true' if it is alright and 'false' if not.
	[[nodiscard]] constexpr bool sanity_check() const {
		return min_num_records_leaf() >= 1
		        && min_num_records_branch() >= 1
		        && m_num_links_branch >= 2;
	}

public:
	explicit Btree(std::string_view identifier = "/tmp/eu-btree-default", ActionOnConstruction action_on_construction = ActionOnConstruction::Bare) : m_pager{std::make_shared<PagerType>(identifier)}, m_identifier{identifier} {
		using enum ActionOnConstruction;

		// clang-format off
		switch (action_on_construction) {
		  break; case Load: load();
		  break; case Bare: bare();
		  break; case InMemoryOnly: bare();
		}
		// clang-format on

		/// For debug build, additionally check whether the initialized instance represents a valid B-tree
		assert(sanity_check());
	}

	Btree(const Btree &) = default;
	Btree(Btree &&) noexcept = default;

	/// Copy-constructor which zeroes-out statistics
	Btree clone_only_blueprint() const noexcept {
		auto copy = Btree(*this);
		copy.m_size = copy.m_depth = 0;
		copy.bare();
		return copy;
	}

	Btree &operator=(const Btree &) = default;
	Btree &operator=(Btree &&) noexcept = default;

	virtual ~Btree() noexcept = default;

	auto operator<=>(const Btree &) const noexcept = default;

private:
	/// Shared among insertion trees and other copies of the tree
	std::shared_ptr<PagerType> m_pager;

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
