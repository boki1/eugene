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

enum class LinkStatus : uint8_t { Valid,
	                          Inval };

template<BtreeConfig Config = DefaultConfig>
class Node {
	friend Btree<Config>;

	using Self = Node<Config>;

	using Val = typename Config::Val;
	using Key = typename Config::Key;
	using Ref = typename Config::Ref;

public:
	struct Branch {
		std::vector<Self::Ref> m_refs;
		std::vector<Position> m_links;
		std::vector<LinkStatus> m_link_status;

		constexpr Branch() = default;
		constexpr Branch(std::vector<Self::Ref> &&refs, std::vector<Position> &&links, std::vector<LinkStatus> &&link_status)
		    : m_refs{std::move(refs)}, m_links{std::move(links)}, m_link_status{std::move(link_status)} {}

		auto operator<=>(const Branch &) const noexcept = default;
		NOP_STRUCTURE(Branch, m_refs, m_links, m_link_status);
	};

	struct Leaf {
		std::vector<Self::Key> m_keys;
		std::vector<Self::Val> m_vals;

		constexpr Leaf() = default;
		constexpr Leaf(std::vector<Self::Key> &&keys, std::vector<Self::Val> &&vals)
		    : m_keys{std::move(keys)}, m_vals{std::move(vals)} {}

		auto operator<=>(const Leaf &) const noexcept = default;
		NOP_STRUCTURE(Leaf, m_keys, m_vals);
	};

	using Metadata = nop::Variant<Branch, Leaf>;

public:
	[[nodiscard]] constexpr bool is_leaf() const noexcept { return m_metadata.template is<Leaf>(); }
	[[nodiscard]] constexpr bool is_branch() const noexcept { return m_metadata.template is<Branch>(); }

	[[nodiscard]] Leaf &leaf() {
//		if (is_leaf())
			return *m_metadata.template get<Leaf>();
//		throw BadTreeAccess(fmt::format(" - branch accessed as leaf\n"));
	}

	[[nodiscard]] Branch &branch() {
//		if (is_branch())
			return *m_metadata.template get<Branch>();
//		throw BadTreeAccess(fmt::format(" - leaf accessed as branch\n"));
	}

	[[nodiscard]] const Leaf &leaf() const {
//		if (is_leaf())
			return *m_metadata.template get<Leaf>();
//		throw BadTreeAccess(fmt::format(" - branch accessed as leaf\n"));
	}

	[[nodiscard]] const Branch &branch() const {
//		if (is_branch())
			return *m_metadata.template get<Branch>();
//		throw BadTreeAccess(fmt::format(" - leaf accessed as branch\n"));
	}

	[[nodiscard]] constexpr Position parent() const noexcept { return m_parent_pos; }
	[[nodiscard]] constexpr bool is_root() const noexcept { return m_is_root; }

	[[nodiscard]] constexpr long num_filled() const noexcept { return is_leaf() ? leaf().m_keys.size() : branch().m_refs.size(); }
	[[nodiscard]] constexpr const auto &items() const noexcept { return is_leaf() ? leaf().m_keys : branch().m_refs; }
	[[nodiscard]] constexpr auto &items() noexcept { return is_leaf() ? leaf().m_keys : branch().m_refs; }

	[[nodiscard]] constexpr bool is_full(long m) const noexcept { return num_filled() >= m; }
	[[nodiscard]] constexpr bool is_underfull(long m) const noexcept { return num_filled() < m / 2 && !m_is_root; }

public:
	enum class RootStatus { IsRoot,
		                IsInternal };

	constexpr Node() : m_metadata{} {}

	constexpr Node(Metadata &&metadata, Position parent_pos, RootStatus rs = RootStatus::IsInternal)
	    : m_metadata{std::move(metadata)},
	      m_is_root{rs == RootStatus::IsRoot},
	      m_parent_pos{parent_pos} {}

private:
	constexpr Node(const Node &) = default;

public:
	constexpr Node &operator=(const Node &) = delete;

	constexpr Node(Node &&) noexcept = default;
	constexpr Node &operator=(Node &&) noexcept = default;

	constexpr Node clone() const noexcept {
		return Node{*this};
	}

	template<typename NodeType, typename... T>
	constexpr static auto meta_of(T &&...ctor_args) {
		return Metadata(NodeType{std::forward<T>(ctor_args)...});
	}

	constexpr auto operator==(const Node &rhs) const noexcept {
		if (is_leaf() ^ rhs.is_leaf())
			return false;
		return is_leaf() ? leaf() == rhs.leaf() : branch() == rhs.branch() && std::tie(m_is_root, m_parent_pos) == std::tie(rhs.m_is_root, rhs.m_parent_pos);
	}

	constexpr auto operator!=(const Node &rhs) const noexcept { return !operator==(rhs); }

	constexpr static Self from_page(const Page &p) {
		nop::Deserializer<nop::BufferReader> deserializer{p.data(), PAGE_SIZE};
		Node node;
		deserializer.Read(&node);
		return node;
	}

	[[nodiscard]] constexpr Page make_page() const noexcept {
		Page p;
		nop::Serializer<nop::BufferWriter> serializer{p.data(), PAGE_SIZE};
		serializer.Write(*this);
		return p;
	}

	constexpr std::pair<Self::Key, Self> split(const std::size_t m) {
		const std::size_t pivot = (m + 1) / 2;
		if (is_branch()) {
			auto &b = branch();
			Node sibling{meta_of<Branch>(
			                     break_at_index(b.m_refs, pivot),
			                     break_at_index(b.m_links, pivot),
			                     break_at_index(b.m_link_status, pivot)),
			             parent()};
			b.m_refs.shrink_to_fit();
			b.m_links.shrink_to_fit();
			b.m_link_status.shrink_to_fit();
			Self::Key midkey = b.m_refs[pivot - 1];
			return std::make_pair<Self::Key, Self>(std::move(midkey), std::move(sibling));
		} else {
			auto &l = leaf();
			Node sibling{meta_of<Leaf>(
			                     break_at_index(l.m_keys, pivot),
			                     break_at_index(l.m_vals, pivot)),
			             parent()};
			l.m_keys.shrink_to_fit();
			l.m_vals.shrink_to_fit();
			Self::Key midkey = l.m_keys[pivot - 1];
			return std::make_pair<Self::Key, Self>(std::move(midkey), std::move(sibling));
		}
	}

private:
	// Leaves elements [0; pivot] and returns a vector with (pivot; target.size())
	template<typename T>
	constexpr std::vector<T> break_at_index(std::vector<T> &target, uint32_t pivot) {
		assert(target.size() >= 2);

		std::vector<T> second;
		second.reserve(target.size() - pivot);

		std::move(target.begin() + pivot,
		          target.end(),
		          std::back_inserter(second));
		target.resize(pivot);
		return second;
	}

	/// Create a new node which is a combination of *this and other.
	/// The created node is returned as a result and is guaranteed to be a valid node, which conforms to the
	/// btree requirements for a node.
	constexpr std::optional<Node> merge_with(const Node &other) const {
		// Merge is performed only of nodes which share a parent (are siblings)
		// Also a sanity check is done ensuring the nodes are at the same level.
		if (is_leaf() ^ other.is_leaf() && parent() == other.parent())
			return {};

		Node merged = this->clone();

		/// Concatenate two std::vectors
		auto vec_extend = [](auto &vec1, auto &vec2) {
			vec1.reserve(vec1.size() + vec2.size());
			vec1.insert(vec1.end(), vec2.begin(), vec2.end());
		};

		if (is_leaf()) {
			vec_extend(merged.leaf().m_keys, other.leaf().m_keys);
			vec_extend(merged.leaf().m_vals, other.leaf().m_vals);
		} else {
			vec_extend(merged.branch().m_refs, other.branch().m_refs);
			vec_extend(merged.branch().m_links, other.branch().m_links);
			vec_extend(merged.branch().m_link_status, other.branch().m_link_status);
		}

		return std::make_optional<Node>(std::move(merged));
	}

public:
	void set_metadata(Metadata &&meta) noexcept { m_metadata = std::move(meta); }

	void set_root(bool flag = true) noexcept { m_is_root = flag; }

	void set_parent(Position pos) noexcept { m_parent_pos = pos; }

private:
	Metadata m_metadata;
	bool m_is_root{false};
	Position m_parent_pos{};

	NOP_STRUCTURE(Node, m_metadata, m_is_root, m_parent_pos);
};

}// namespace internal::storage::btree
