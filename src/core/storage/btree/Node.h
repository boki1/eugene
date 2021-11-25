#pragma once

#include <algorithm>
#include <cassert>
#include <optional>
#include <variant>
#include <vector>

#include <core/storage/Page.h>
#include <core/storage/Position.h>

#include <core/storage/btree/Config.h>

namespace internal::storage::btree {

template<BtreeConfig Config>
class Btree;

template<BtreeConfig Config>
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
	};

	struct Leaf {
		std::vector<Self::Key> m_keys;
		std::vector<Self::Val> m_vals;
	};

	using Metadata = std::variant<Branch, Leaf>;

private:
	[[nodiscard]] bool is_leaf() const noexcept { return std::holds_alternative<Leaf>(m_metadata); }
	[[nodiscard]] bool is_branch() const noexcept { return std::holds_alternative<Branch>(m_metadata); }

	[[nodiscard]] Leaf &leaf() { return std::get<Leaf>(m_metadata); }
	[[nodiscard]] Branch &branch() { return std::get<Branch>(m_metadata); }

	[[nodiscard]] const Leaf &leaf() const { return std::get<Leaf>(m_metadata); }
	[[nodiscard]] const Branch &branch() const { return std::get<Branch>(m_metadata); }

	[[nodiscard]] std::optional<Position> parent() const noexcept { return m_parent_pos; }
	[[nodiscard]] bool is_root() const noexcept { return m_is_root; }

	[[nodiscard]] long num_filled() const noexcept { return is_leaf() ? leaf().m_keys.size() : branch().m_refs.size(); }

	[[nodiscard]] bool is_full(long n) const noexcept { return num_filled() == 2 * n - 1 && !m_is_root; }
	[[nodiscard]] bool is_under(long n) const noexcept { return num_filled() < n - 1 && !m_is_root; }

public:
	Node(Metadata &&metadata, std::optional<Position> parent_pos = {}, bool is_root = false)
	    : m_metadata{std::move(metadata)},
	      m_is_root{is_root},
	      m_parent_pos{parent_pos} {}


	// TODO:
	static std::optional<Self> from_page(const Page &) {}

	// TODO:
	[[nodiscard]] Page make_page() const noexcept { return Page::empty(); }

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

	void set_parent(Position pos) noexcept { m_parent_pos.emplace(pos); }

private:
	Metadata m_metadata;
	bool m_is_root;
	std::optional<Position> m_parent_pos;
};

}// namespace internal::storage::btree
