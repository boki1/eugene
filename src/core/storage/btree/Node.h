#pragma once

#include <algorithm>
#include <cassert>
#include <nop/status.h>
#include <optional>
#include <vector>

#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/types/variant.h>
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

public:
	struct Branch {
		std::vector<Self::Ref> m_refs;
		std::vector<Position> m_links;

		auto operator<=>(const Branch &) const noexcept = default;
		NOP_STRUCTURE(Branch, m_refs, m_links);
	};

	struct Leaf {
		std::vector<Self::Key> m_keys;
		std::vector<Self::Val> m_vals;

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
	explicit Node(Metadata &&metadata, Position parent_pos = {}, bool is_root = false)
	    : m_metadata{std::move(metadata)},
	      m_is_root{is_root},
	      m_parent_pos{parent_pos} {}

	// Used only by serialization library
	Node()
	    : m_metadata{} {}

	auto operator==(const Node &rhs) const noexcept {
		if (is_leaf() ^ rhs.is_leaf())
			return false;
		return is_leaf() ? leaf() == rhs.leaf() : branch() == rhs.branch()
			&& std::tie(m_is_root, m_parent_pos) == std::tie(rhs.m_is_root, rhs.m_parent_pos);
	}

	static std::optional<Self> from_page(const Page &p) {
		nop::Deserializer<nop::BufferReader> deserializer{p.raw(), Page::size()};
		Node node;
		if (deserializer.Read(&node))
			return node;
		return {};
	}

	[[nodiscard]] std::optional<Page> make_page() const noexcept {
		auto p = Page::empty();
		nop::Serializer<nop::BufferWriter> serializer{p.raw(), Page::size()};
		if (serializer.Write(*this))
			return p;
		return {};
	}

	template<typename T>
	std::vector<T> break_at_index(std::vector<T> &target, uint32_t pivot) {
		std::vector<T> second;
		second.reserve(target.size() - 1);
		std::move(target.begin() + pivot, target.end(), std::back_inserter(second));
		return second;
	}

	std::pair<Self::Key, Self> split(uint32_t pivot) {
		if (is_branch()) {
			auto sibling_refs = break_at_index(branch().m_refs, pivot - 1);
			auto sibling_links = break_at_index(branch().m_links, pivot);
			Ref middle = sibling_refs.back();
			sibling_refs.pop_back();
			Node sibling{Branch(sibling_refs, sibling_links), parent()};
			return std::make_pair(middle, sibling);
		}
		assert(is_leaf());

		auto sibling_keys = break_at_index(leaf().m_keys, pivot);
		auto sibling_vals = break_at_index(leaf().m_vals, pivot);
		Key middle = sibling_keys.back();
		sibling_keys.pop_back();
		Node sibling{Leaf(sibling_keys, sibling_vals), parent()};
		return std::make_pair(middle, sibling);
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
