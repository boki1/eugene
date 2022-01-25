#pragma once

#include <algorithm>
#include <cassert>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>
#include <iostream>

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/types/variant.h>
#include <nop/utility/die.h>
#include <nop/utility/buffer_reader.h>
#include <nop/utility/buffer_writer.h>

#include <core/storage/Pager.h>
#include <core/storage/btree/Config.h>

namespace internal::storage::btree {

template<BtreeConfig Config>
class Btree;

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

		constexpr Branch() = default;
		constexpr Branch(std::vector<Self::Ref> &&refs, std::vector<Position> &&links)
		    : m_refs{std::move(refs)}, m_links{std::move(links)} {}

		auto operator<=>(const Branch &) const noexcept = default;
		NOP_STRUCTURE(Branch, m_refs, m_links);
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

	// TODO:
	// Consider adding some error handling here. As of now calling `leaf()` or `branch()` in either of their const
	// variants _REQUIRES_ that the value inside is actually the one which we ask for.
	[[nodiscard]] constexpr Leaf &leaf() { return *m_metadata.template get<Leaf>(); }
	[[nodiscard]] constexpr Branch &branch() { return *m_metadata.template get<Branch>(); }
	[[nodiscard]] constexpr const Leaf &leaf() const { return *m_metadata.template get<Leaf>(); }
	[[nodiscard]] constexpr const Branch &branch() const { return *m_metadata.template get<Branch>(); }

	[[nodiscard]] constexpr Position parent() const noexcept { return m_parent_pos; }
	[[nodiscard]] constexpr bool is_root() const noexcept { return m_is_root; }

	[[nodiscard]] constexpr long num_filled() const noexcept { return is_leaf() ? leaf().m_keys.size() : branch().m_refs.size(); }

	[[nodiscard]] constexpr bool is_full(long m) const noexcept { return num_filled() >= m; }
	[[nodiscard]] constexpr bool is_under(long m) const noexcept { return num_filled() < m / 2 && !m_is_root; }

public:
	constexpr Node() : m_metadata{} {}

	constexpr Node(Metadata &&metadata, Position parent_pos, bool is_root = false)
	    : m_metadata{std::move(metadata)},
	      m_is_root{is_root},
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
			                     break_at_index(b.m_links, pivot)), parent()};
			b.m_refs.shrink_to_fit();
			b.m_links.shrink_to_fit();
			Self::Key midkey = b.m_refs[pivot - 1];
			return std::make_pair<Self::Key, Self>(std::move(midkey), std::move(sibling));
		} else {
			auto &l = leaf();
			Node sibling{meta_of<Leaf>(
			                     break_at_index(l.m_keys, pivot),
			                     break_at_index(l.m_vals, pivot)), parent()};
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
