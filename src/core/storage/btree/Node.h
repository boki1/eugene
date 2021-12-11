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

#include <core/storage/Page.h>
#include <core/storage/Position.h>
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

	static constexpr int PIVOT = Config::BTREE_NODE_BREAK_POINT;

public:
	struct Branch {
		std::vector<Self::Ref> m_refs;
		std::vector<Position> m_links;

		Branch() = default;
		Branch(std::vector<Self::Ref> &&refs, std::vector<Position> &&links)
		    : m_refs{std::move(refs)}, m_links{std::move(links)} {}

		auto operator<=>(const Branch &) const noexcept = default;
		NOP_STRUCTURE(Branch, m_refs, m_links);
	};

	struct Leaf {
		std::vector<Self::Key> m_keys;
		std::vector<Self::Val> m_vals;

		Leaf() = default;
		Leaf(std::vector<Self::Key> &&keys, std::vector<Self::Val> &&vals)
		    : m_keys{std::move(keys)}, m_vals{std::move(vals)} {}

		auto operator<=>(const Leaf &) const noexcept = default;
		NOP_STRUCTURE(Leaf, m_keys, m_vals);
	};

	using Metadata = nop::Variant<Branch, Leaf>;

public:
	[[nodiscard]] bool is_leaf() const noexcept { return m_metadata.template is<Leaf>(); }
	[[nodiscard]] bool is_branch() const noexcept { return m_metadata.template is<Branch>(); }

	// TODO:
	// Consider adding some error handling here. As of now calling `leaf()` or `branch()` in either of their const
	// variants _REQUIRES_ that the value inside is actually the one which we ask for.
	[[nodiscard]] Leaf &leaf() { return *m_metadata.template get<Leaf>(); }
	[[nodiscard]] Branch &branch() { return *m_metadata.template get<Branch>(); }
	[[nodiscard]] const Leaf &leaf() const { return *m_metadata.template get<Leaf>(); }
	[[nodiscard]] const Branch &branch() const { return *m_metadata.template get<Branch>(); }

	[[nodiscard]] Position parent() const noexcept { return m_parent_pos; }
	[[nodiscard]] bool is_root() const noexcept { return m_is_root; }

	[[nodiscard]] long num_filled() const noexcept { return is_leaf() ? leaf().m_keys.size() : branch().m_refs.size(); }

	[[nodiscard]] bool is_full(long n) const noexcept { return num_filled() == 2 * n - 1 && !m_is_root; }
	[[nodiscard]] bool is_under(long n) const noexcept { return num_filled() < n - 1 && !m_is_root; }

public:
	Node() : m_metadata{} {}

	Node(Metadata &&metadata, Position parent_pos, bool is_root = false)
	    : m_metadata{std::move(metadata)},
	      m_is_root{is_root},
	      m_parent_pos{parent_pos} {}

private:
	Node(const Node &) = default;

public:
	Node &operator=(const Node &) = delete;

	Node(Node &&) noexcept = default;
	Node &operator=(Node &&) noexcept = default;

	Node clone() const noexcept {
		return Node{*this};
	}

	template<typename NodeType, typename... T>
	static auto meta_of(T &&...ctor_args) {
		return Metadata(NodeType{std::forward<T>(ctor_args)...});
	}

	auto operator==(const Node &rhs) const noexcept {
		if (is_leaf() ^ rhs.is_leaf())
			return false;
		return is_leaf() ? leaf() == rhs.leaf() : branch() == rhs.branch() && std::tie(m_is_root, m_parent_pos) == std::tie(rhs.m_is_root, rhs.m_parent_pos);
	}

	auto operator!=(const Node &rhs) const noexcept { return !operator==(rhs); }

	static Self from_page(const Page &p) {
		nop::Deserializer<nop::BufferReader> deserializer{p.raw(), Page::size()};
		Node node;
		deserializer.Read(&node) || nop::Die(std::cerr);
		return node;
	}

	[[nodiscard]] Page make_page() const noexcept {
		auto p = Page::empty();
		nop::Serializer<nop::BufferWriter> serializer{p.raw(), Page::size()};
		serializer.Write(*this) || nop::Die(std::cerr);
		return p;
	}

	std::pair<Self::Key, Self> split() {
		if (is_branch()) {
			auto &b = branch();
			Node sibling{meta_of<Branch>(
			                     break_at_index(b.m_refs, PIVOT),
			                     break_at_index(b.m_links, PIVOT)),
			             parent()};
			Self::Key midkey = b.m_refs[PIVOT];
			b.m_refs.resize(PIVOT);
			return std::make_pair<Self::Key, Self>(std::move(midkey), std::move(sibling));
		} else {
			auto &l = leaf();
			Node sibling{meta_of<Leaf>(
			                     break_at_index(l.m_keys, PIVOT),
			                     break_at_index(l.m_vals, PIVOT)),
			             parent()};
			Self::Key midkey = l.m_keys[PIVOT];
			l.m_keys.resize(PIVOT);
			return std::make_pair<Self::Key, Self>(std::move(midkey), std::move(sibling));
		}
	}

private:
	// Return (pivot; target.size()) and leave at original vector [0; pivot)
	template<typename T>
	std::vector<T> break_at_index(std::vector<T> &target, uint32_t pivot) {
		std::vector<T> second;
		second.reserve(target.size() - pivot - 1);
		std::move(target.begin() + pivot + 1,
		          target.end(),
		          std::back_inserter(second));
		target.resize(pivot + 1);
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
