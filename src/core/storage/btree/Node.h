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

#include <core/Config.h>
#include <core/storage/Pager.h>

#include <fmt/core.h>

namespace internal::storage::btree {

template<EugeneConfig _>
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

/// Denotes how split operations distributes the entries of the overflowed node.
/// LeanLeft means that the left node is kept full, LeanRight- that the right node is kept full,
/// and Unbiased- that the entries are equally distributed among to the two siblings.
enum class SplitBias { LeanLeft,
	               LeanRight,
	               DistributeEvenly,
	               TakeLiterally };

/// Denotes how split operations deal with the midkey.
/// Normally, branch nodes remove the midkey from the resulting 2 nodes and propagate it upwards.
/// ExplodeOnly will not do that and the midkey is only a copy. - split(| 1 2 3 4 |) -> | 1 2 |, | 3 4 |.
/// ExcludeMid does what is considered the normal behaviour, described earlier.
enum class SplitType { ExplodeOnly,
	               ExcludeMid };

template<EugeneConfig Config = Config>
class Node {
	friend Btree<Config>;

	using Nod = Node<Config>;

	using Val = typename Config::Val;
	using RealVal = typename Config::RealVal;
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
		std::vector<Ref> refs;
		std::vector<Position> links;
		std::vector<LinkStatus> link_status;

		constexpr Branch() = default;
		constexpr Branch(std::vector<Ref> &&refs, std::vector<Position> &&links, std::vector<LinkStatus> &&link_status)
		    : refs{std::move(refs)}, links{std::move(links)}, link_status{std::move(link_status)} {}

		constexpr Branch(const Branch &) = default;
		constexpr Branch &operator=(const Branch &) = default;

		auto operator<=>(const Branch &) const noexcept = default;
		NOP_STRUCTURE(Branch, refs, links, link_status);
	};

	/// Data specific to leaf nodes.
	/// Each such node contains a list of keys ('keys') and a list of values ('vals').
	/// This is whether the entry association is done (<key, val>).
	/// This type is serializable (persistent) and comparable.

	struct Entry {
		Key key;
		RealVal val;

		[[nodiscard]] auto operator<=>(const Entry &) const noexcept = default;
	};

	struct Leaf {
		std::vector<Key> keys;
		std::vector<Val> vals;

		constexpr Leaf() = default;
		constexpr Leaf(std::vector<Key> &&keys, std::vector<Val> &&vals)
		    : keys{std::move(keys)}, vals{std::move(vals)} {}

		constexpr Leaf(const Leaf &) = default;
		constexpr Leaf &operator=(const Leaf &) = default;

		auto operator<=>(const Leaf &) const noexcept = default;
		NOP_STRUCTURE(Leaf, keys, vals);
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

	constexpr Node &operator=(const Node &) = default;
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
	[[nodiscard]] static Nod from_page(const Page &p) {
		if (static_cast<PageType>(p.front()) != PageType::Node)
		{}
			// throw BadRead("cannot create node from page");
			// throw BadRead();
		nop::Deserializer<nop::BufferReader> deserializer{p.data() + 1, PAGE_SIZE - 1};
		Node node;
		deserializer.Read(&node);
		return node;
	}

	/// Create a page containing this' data
	[[nodiscard]] constexpr Page make_page() const noexcept {
		Page p;
		p[0] = static_cast<uint8_t>(PageType::Node);
		nop::Serializer<nop::BufferWriter> serializer{p.data() + 1, PAGE_SIZE - 1};
		serializer.Write(*this);
		return p;
	}

	/// Perform a split operation based on some branching factor 'm'.
	/// Returns a brand new node and the key which is not contained in
	/// neither of the nodes. It should be put in the parent's list.

	constexpr std::pair<Key, Nod> split(const std::size_t max_num_records, const SplitBias bias, const SplitType type = SplitType::ExcludeMid) {
		Node sibling;
		Key midkey;
		const auto pivot = [max_num_records, bias, this]() -> std::size_t {
			// clang-format off
			switch (bias) {
				break; case SplitBias::LeanLeft: return max_num_records - 1;
				break; case SplitBias::LeanRight: return std::abs(num_filled() - static_cast<long>(max_num_records)) + 1;
				break; case SplitBias::DistributeEvenly: return num_filled() / 2;
				// In such case 'max_num_records' is not the maximum but the actual pivot precalculated.
				// Note: Using this option is discouraged in regular cases, since it may be an invalid index.
				break; case SplitBias::TakeLiterally: return max_num_records;
			}
			// clang-format on
			UNREACHABLE
		}();

		if (is_branch()) {
			auto &b = branch();
			midkey = b.refs[pivot];
			sibling = {metadata_ctor<Branch>(break_at_index(b.refs, pivot + 1), break_at_index(b.links, pivot + 1), break_at_index(b.link_status, pivot + 1)), parent()};
			if (type == SplitType::ExcludeMid)
				b.refs.pop_back();// Branch nodes do not copy mid-keys
		} else {
			auto &l = leaf();
			sibling = {metadata_ctor<Leaf>(break_at_index(l.keys, pivot), break_at_index(l.vals, pivot)), parent()};
			midkey = sibling.leaf().keys.front();
		}

		return std::make_pair<Key, Nod>(std::move(midkey), std::move(sibling));
	}

	/// Create a new node which is a combination of *this and other.
	/// The created node is returned as a result and is guaranteed to be a valid node, which conforms to the
	/// btree requirements for a node.
	[[maybe_unused]] constexpr Nod fuse_with(const Node &other) {
		Metadata m;
		if (is_leaf()) {
			auto l = Leaf();
			merge_many(leaf().keys, other.leaf().keys, [&](const bool use_self, const std::size_t idx) {
				if (use_self) {
					// fmt::print("Adding {}\n", *(leaf().keys.cbegin() + idx));
					l.keys.push_back(*(leaf().keys.cbegin() + idx));
					l.vals.push_back(*(leaf().vals.cbegin() + idx));
				} else {
					// fmt::print("Adding {}\n", *(other.leaf().keys.cbegin() + idx));
					l.keys.push_back(*(other.leaf().keys.cbegin() + idx));
					l.vals.push_back(*(other.leaf().vals.cbegin() + idx));
				}
			});
			m = l;
		} else if (is_branch()) {
			auto b = Branch();
			merge_many(branch().refs, other.branch().refs, [&](const bool use_self, const std::size_t idx) {
				if (use_self) {
					b.refs.push_back(*(branch().refs.cbegin() + idx));
					b.links.push_back(*(branch().links.cbegin() + idx));
					b.link_status.push_back(*(branch().link_status.cbegin() + idx));
					// fmt::print("Adding {} and link to @{}\n", b.refs.back(), b.links.back());
					if (idx == branch().refs.size() - 1 && branch().links.size() > branch().refs.size()) {
						// fmt::print("Last ref, adding right link and link to @{}\n", branch().links.back());
						b.links.push_back(branch().links.back());
						b.link_status.push_back(branch().link_status.back());
					}
				} else {
					b.refs.push_back(*(other.branch().refs.cbegin() + idx));
					b.links.push_back(*(other.branch().links.cbegin() + idx));
					b.link_status.push_back(*(other.branch().link_status.cbegin() + idx));
					// fmt::print("Adding {} and link to @{}\n", b.refs.back(), b.links.back());
					if (idx == other.branch().refs.size() - 1 && other.branch().links.size() > other.branch().refs.size()) {
						// fmt::print("Last ref, adding right link and link to @{}\n", other.branch().links.back());
						b.links.push_back(other.branch().links.back());
						b.link_status.push_back(other.branch().link_status.back());
					}
				}
			});

			m = b;
		}
		return Nod(std::move(m), 0, m_is_root ? RootStatus::IsRoot : RootStatus::IsInternal);
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

	[[nodiscard]] constexpr long num_filled() const noexcept { return is_leaf() ? leaf().keys.size() : branch().refs.size(); }

	[[nodiscard]] constexpr const auto &items() const noexcept { return is_leaf() ? leaf().keys : branch().refs; }

	[[nodiscard]] constexpr auto &items() noexcept { return is_leaf() ? leaf().keys : branch().refs; }

	[[nodiscard]] constexpr bool is_over(long m) const noexcept { return num_filled() > m; }

	[[nodiscard]] constexpr bool is_under(long m) const noexcept { return num_filled() < m / 2 && !m_is_root; }

	[[nodiscard]] constexpr bool is_empty() const noexcept { return num_filled() == 0; }

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
