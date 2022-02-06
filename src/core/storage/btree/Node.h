#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/types/variant.h>
#include <nop/utility/buffer_reader.h>
#include <nop/utility/buffer_writer.h>
#include <nop/utility/die.h>

#include <core/storage/Pager.h>
#include <core/storage/btree/Config.h>

#include <fmt/core.h>

namespace internal::storage::btree {

template<BtreeConfig Config>
class Btree;

struct BadTreeAccess : std::runtime_error {
	explicit BadTreeAccess(std::string_view msg) : std::runtime_error{fmt::format("Eugene: Bad tree access {}", msg.data())} {}
};

/// Status of branch link stored in Branch::link_status
/// Each link in there is either a valid one, or an invalid one.
/// This may happen if a node was removed, than although it may
/// have siblings, this particular subtree does not exist anymore,
/// thus the link should be marked as invalid.
enum class LinkStatus : uint8_t { Valid,
	                          Inval };

template<BtreeConfig Config = DefaultConfig>
class Node {
	friend Btree<Config>;

	using Nod = Node<Config>;

	using Val = typename Config::Val;
	using Key = typename Config::Key;
	using Ref = typename Config::Ref;

public:
	///
	/// Node types
	///

	/// Data specific to internal nodes.
	/// Each such node contains a list of keys ('refs') and a list of its children nodes ('links). The list of
	/// keys is stored only for comparison purposes, it is not the actual key storage, but a "map" to the
	/// <key, val> entries which are placed in the leaves of the tree. Additionally each link is associated with
	/// status- check the definition of 'LinkStatus' for the details. This type is serializable (persistent) and
	/// comparable.
	struct Branch {
		std::vector<Ref> m_refs;
		std::vector<Position> m_links;
		std::vector<LinkStatus> m_link_status;

		constexpr Branch() = default;
		constexpr Branch(std::vector<Ref> &&refs, std::vector<Position> &&links, std::vector<LinkStatus> &&link_status)
		    : m_refs{std::move(refs)}, m_links{std::move(links)}, m_link_status{std::move(link_status)} {}

		auto operator<=>(const Branch &) const noexcept = default;
		NOP_STRUCTURE(Branch, m_refs, m_links, m_link_status);
	};

	/// Data specific to leaf nodes.
	/// Each such node contains a list of keys ('keys') and a list of values ('vals').
	/// This is whether the entry association is done (<key, val>).
	/// This type is serializable (persistent) and comparable.
	struct Leaf {
		std::vector<Key> m_keys;
		std::vector<Val> m_vals;

		constexpr Leaf() = default;
		constexpr Leaf(std::vector<Key> &&keys, std::vector<Val> &&vals)
		    : m_keys{std::move(keys)}, m_vals{std::move(vals)} {}

		auto operator<=>(const Leaf &) const noexcept = default;
		NOP_STRUCTURE(Leaf, m_keys, m_vals);
	};

	/// Each node contains either a Branch or Leaf specific data.
	using Metadata = nop::Variant<Branch, Leaf>;

	/// Metadata "constructor"
	template<typename NodeType, typename... T>
	constexpr static auto metadata_ctor(T &&...ctor_args) {
		return Metadata(NodeType{std::forward<T>(ctor_args)...});
	}

public:
	enum class RootStatus { IsRoot,
		                IsInternal };

	constexpr Node() = default;

	constexpr Node(Metadata &&metadata, Position parent_pos, RootStatus rs = RootStatus::IsInternal)
	    : m_metadata{std::move(metadata)},
	      m_is_root{rs == RootStatus::IsRoot},
	      m_parent_pos{parent_pos} {}

	constexpr Node(Node &&) noexcept = default;
	constexpr Node(const Node &) = default;

	constexpr Node &operator=(const Node &) = delete;
	constexpr Node &operator=(Node &&) noexcept = default;

	constexpr auto operator==(const Node &rhs) const noexcept {
		return is_leaf() == rhs.is_leaf()
		        && (is_leaf() ? leaf() == rhs.leaf() : branch() == rhs.branch())
		        && std::tie(m_is_root, m_parent_pos, m_next_node_pos) == std::tie(rhs.m_is_root, rhs.m_parent_pos, rhs.m_next_node_pos);
	}

	constexpr auto operator!=(const Node &rhs) const noexcept { return !operator==(rhs); }

public:
	///
	/// Operations API
	///

	/// Create a node from page
	[[nodiscard]] constexpr static Nod from_page(const Page &p) {
		nop::Deserializer<nop::BufferReader> deserializer{p.data(), PAGE_SIZE};
		Node node;
		deserializer.Read(&node);
		return node;
	}

	/// Create a page containing this' data
	[[nodiscard]] constexpr Page make_page() const noexcept {
		Page p;
		nop::Serializer<nop::BufferWriter> serializer{p.data(), PAGE_SIZE};
		serializer.Write(*this);
		return p;
	}

	/// Perform a split operation based on some branching factor 'm'.
	/// Returns a brand new node and the key which is not contained in
	/// neither of the nodes. It should be put in the parent's list.
	constexpr std::pair<Key, Nod> split(const std::size_t m) {
		const std::size_t pivot = (m + 1) / 2;
		if (is_branch()) {
			auto &b = branch();
			Node sibling{metadata_ctor<Branch>(
			                     break_at_index(b.m_refs, pivot),
			                     break_at_index(b.m_links, pivot),
			                     break_at_index(b.m_link_status, pivot)),
			             parent()};
			b.m_refs.shrink_to_fit();
			b.m_links.shrink_to_fit();
			b.m_link_status.shrink_to_fit();
			Key midkey = b.m_refs[pivot - 1];
			return std::make_pair<Key, Nod>(std::move(midkey), std::move(sibling));
		} else {
			auto &l = leaf();
			Node sibling{metadata_ctor<Leaf>(
			                     break_at_index(l.m_keys, pivot),
			                     break_at_index(l.m_vals, pivot)),
			             parent()};
			l.m_keys.shrink_to_fit();
			l.m_vals.shrink_to_fit();
			Key midkey = l.m_keys[pivot - 1];
			return std::make_pair<Key, Nod>(std::move(midkey), std::move(sibling));
		}
	}

	/// Create a new node which is a combination of *this and other.
	/// The created node is returned as a result and is guaranteed to be a valid node, which conforms to the
	/// btree requirements for a node.
	constexpr std::optional<Node> merge_with(const Node &other) const {
		// Merge is performed only of nodes which share a parent (are siblings)
		// Also a sanity check is done ensuring the nodes are at the same level.
		if (is_leaf() ^ other.is_leaf() && parent() == other.parent())
			return {};

		Node merged{*this};

		if (is_leaf()) {
			vector_extend(merged.leaf().m_keys, other.leaf().m_keys);
			vector_extend(merged.leaf().m_vals, other.leaf().m_vals);
		} else {
			vector_extend(merged.branch().m_refs, other.branch().m_refs);
			vector_extend(merged.branch().m_links, other.branch().m_links);
			vector_extend(merged.branch().m_link_status, other.branch().m_link_status);
		}

		return std::make_optional<Node>(std::move(merged));
	}

	///
	/// Properties
	///

	[[nodiscard]] constexpr bool is_leaf() const noexcept { return m_metadata.template is<Leaf>(); }

	[[nodiscard]] constexpr bool is_branch() const noexcept { return m_metadata.template is<Branch>(); }

	[[nodiscard]] Leaf &leaf() {
		if (is_leaf())
			return *m_metadata.template get<Leaf>();
		throw BadTreeAccess(fmt::format(" - branch accessed as leaf\n"));
	}

	[[nodiscard]] Branch &branch() {
		if (is_branch())
			return *m_metadata.template get<Branch>();
		throw BadTreeAccess(fmt::format(" - leaf accessed as branch\n"));
	}

	[[nodiscard]] const Leaf &leaf() const {
		if (is_leaf())
			return *m_metadata.template get<Leaf>();
		throw BadTreeAccess(fmt::format(" - branch accessed as leaf\n"));
	}

	[[nodiscard]] const Branch &branch() const {
		if (is_branch())
			return *m_metadata.template get<Branch>();
		throw BadTreeAccess(fmt::format(" - leaf accessed as branch\n"));
	}

	[[nodiscard]] constexpr std::optional<Position> next_node() const noexcept {
		/// Convert a nop::Optional<> to std::optional<>
		return m_next_node_pos ? std::make_optional<Position>(m_next_node_pos.get()) : std::nullopt;
	}

	[[nodiscard]] constexpr Position parent() const noexcept { return m_parent_pos; }

	[[nodiscard]] constexpr bool is_root() const noexcept { return m_is_root; }

	[[nodiscard]] constexpr long num_filled() const noexcept { return is_leaf() ? leaf().m_keys.size() : branch().m_refs.size(); }

	[[nodiscard]] constexpr const auto &items() const noexcept { return is_leaf() ? leaf().m_keys : branch().m_refs; }

	[[nodiscard]] constexpr auto &items() noexcept { return is_leaf() ? leaf().m_keys : branch().m_refs; }

	[[nodiscard]] constexpr bool is_full(long m) const noexcept { return num_filled() >= m; }

	[[nodiscard]] constexpr bool is_underfull(long m) const noexcept { return num_filled() < m / 2 && !m_is_root; }

	void set_root_status(RootStatus rs) noexcept { m_is_root = (rs == RootStatus::IsRoot); }

	void set_parent(Position pos) noexcept { m_parent_pos = pos; }

	void set_next_node(Position pos) noexcept { m_next_node_pos = pos; }

private:
	/// Data specific for the node's position - either Branch if internal, or Leaf- otherwise.
	Metadata m_metadata{};

	bool m_is_root{false};

	/// Keep track of parent node
	Position m_parent_pos{};

	/// Keep a list of all leaf nodes in the tree
	nop::Optional<Position> m_next_node_pos{};

	NOP_STRUCTURE(Node, m_metadata, m_is_root, m_parent_pos, m_next_node_pos);
};

}// namespace internal::storage::btree
