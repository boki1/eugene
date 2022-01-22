#include <algorithm>
#include <fstream>
#include <optional>
#include <random>
#include <ranges>
#include <unordered_map>

#include "catch2/catch.hpp"

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <core/storage/Pager.h>
#include <core/storage/btree/Btree.h>

using namespace std::ranges::views;
using std::find;

using namespace internal::storage;
using namespace internal::storage::btree;

using Nod = Node<DefaultConfig>;
using Metadata = Nod::Metadata;
using Branch = Nod::Branch;
using Leaf = Nod::Leaf;

static Nod make_node() {
	static std::random_device dev;
	std::mt19937 rng(dev());
	std::uniform_int_distribution<std::mt19937::result_type> dist128(1, 128);

	std::vector<Position> a;
	std::vector<DefaultConfig::Key> b;
	for (std::size_t i = 0; i < dist128(rng); ++i)
		if (dist128(rng) % 2)
			a.push_back(dist128(rng));
		else
			b.push_back((int)Position(dist128(rng)));

	Metadata metadata;
	if (dist128(rng) % 2)
		metadata = Metadata(Branch(std::move(b), std::move(a)));
	else
		metadata = Metadata(Leaf(std::move(b), std::move(b)));

	Position p;
	if (auto pp = dist128(rng); pp < 75)
		p = pp;

	return Node(std::move(metadata), p, dist128(rng) % 2);
}

void truncate_file(std::string_view fname) {
	std::ofstream of{std::string{fname}, std::ios::trunc};
}

template <typename T, typename V>
bool contains(const T& collection, V item) {
	return std::find(collection.cbegin(), collection.cend(), item) != collection.cend();
}

TEST_CASE("Node serialization", "[btree]") {
	auto node1 = Nod(Metadata(Branch({1}, {1})), {}, false);
	auto node2 = Nod(Metadata(Leaf({2}, {2})), 13, true);

	auto node1_as_page = node1.make_page();
	auto node2_as_page = node2.make_page();

	auto node1_from_page = Nod::from_page(node1_as_page);
	auto node2_from_page = Nod::from_page(node2_as_page);

	REQUIRE(node1_from_page == node1);
	REQUIRE(node2_from_page == node2);
}

TEST_CASE("Persistent nodes", "[btree]") {
	truncate_file("/tmp/eu-persistent-nodes-pager");
	Pager pr("/tmp/eu-persistent-nodes-pager");

	auto node1_pos = pr.alloc();
	auto node1 = Nod(Metadata(Branch({1}, {1})), {}, false);
	auto node1_as_page = node1.make_page();
	pr.place(node1_pos, std::move(node1_as_page));
	auto node1_from_page = Nod::from_page(pr.get(node1_pos));
	REQUIRE(node1_from_page == node1);
	REQUIRE(node1_from_page.is_root() == false);

	auto node2_pos = pr.alloc();
	auto node2 = Nod(Metadata(Leaf({2}, {2})), 13, true);
	auto node2_as_page = node2.make_page();
	pr.place(node2_pos, std::move(node2_as_page));
	auto node2_from_page = Nod::from_page(pr.get(node2_pos));
	REQUIRE(node2_from_page == node2);
	REQUIRE(node2_from_page.is_root() == true);
	REQUIRE(node2_from_page.parent() == Position(13));
}

TEST_CASE("Paging with many random nodes", "[btree]") {
	truncate_file("/tmp/eu-many-persistent-nodes-pager");
	Pager pr("/tmp/eu-many-persistent-nodes-pager");

	std::unordered_map<Position, Nod> nodes;
	for (std::size_t i = 0; i < 128; ++i) {
		auto node = make_node();
		auto node_pos = pr.alloc();
		nodes[node_pos] = node.clone();
		auto node_as_page = node.make_page();
		pr.place(node_pos, std::move(node_as_page));
		auto node_from_page = Nod::from_page(pr.get(node_pos));
		REQUIRE(node_from_page == node);
	}

	auto random_node = [&] {
		static std::random_device dev;
		static std::mt19937 rng(dev());
		std::uniform_int_distribution<std::mt19937::result_type> dist(0, nodes.size() - 1);

		auto random_it = std::next(std::begin(nodes), dist(rng));
		return std::tie(random_it->first, random_it->second);
	};

	for (std::size_t i = 0; i < nodes.size(); ++i) {
		auto [pos, node] = random_node();
		auto node_from_page = Nod::from_page(pr.get(pos));
		REQUIRE(node_from_page == node);
	}
}

TEST_CASE("Split full nodes", "[btree]") {
	auto bn = Nod(Metadata(Branch({}, {})), {}, false); auto &b = bn.branch();
	auto ln = Nod(Metadata(Leaf({}, {})), 13, true); auto &l = ln.leaf();

	constexpr auto limit_branch = 512;
	constexpr auto limit_leaf = 512;

	// Fill
	for (int i = 0; i < limit_branch; ++i) {
		b.m_refs.push_back(i);
		b.m_links.emplace_back(i);
	}
	b.m_links.emplace_back(limit_branch);

	for (int i = 0; i < limit_leaf; ++i) {
		l.m_keys.push_back(i);
		l.m_vals.push_back(i);
	}

	// Break apart
	auto [branch_midkey, branch_sib_node] = bn.split(limit_branch);
	auto [leaf_midkey, leaf_sib_node] = ln.split(limit_leaf);
	REQUIRE(branch_midkey == limit_branch / 2 - 1);
	REQUIRE(leaf_midkey == limit_leaf / 2 - 1);

	auto &branch_sib = branch_sib_node.branch();
	auto &leaf_sib = leaf_sib_node.leaf();

	for (decltype(branch_midkey) i = 0; i < branch_midkey; ++i) {
		REQUIRE(contains(b.m_refs, i));
		REQUIRE(contains(b.m_links, static_cast<long>(i)));
		REQUIRE(!contains(branch_sib.m_refs, i));
		REQUIRE(!contains(branch_sib.m_links, static_cast<long>(i)));
	}
	REQUIRE(contains(b.m_links, static_cast<long>(branch_midkey)));
	REQUIRE(!contains(branch_sib.m_links, static_cast<long>(branch_midkey)));

	for (decltype(leaf_midkey) i = 0; i <= leaf_midkey; ++i) {
		REQUIRE(contains(l.m_keys, i));
		REQUIRE(contains(l.m_vals, i));
		REQUIRE(!contains(leaf_sib.m_keys, i));
		REQUIRE(!contains(leaf_sib.m_vals, i));
	}

	// The middle key should remain in the "left" leaf although it is suppossed to get a
	// ref inside the new parent, this is the actual spot where the val is stored.
	REQUIRE(contains(leaf_sib.m_keys, (limit_leaf + 1) / 2));
	REQUIRE(contains(leaf_sib.m_vals, (limit_leaf + 1) / 2));

	for (uint32_t i = branch_midkey + 1; i < limit_branch; ++i) {
		REQUIRE(!contains(b.m_refs, i));
		REQUIRE(!contains(b.m_links, static_cast<long>(i)));
		REQUIRE(contains(branch_sib.m_refs, i));
		REQUIRE(contains(branch_sib.m_links, static_cast<long>(i)));
	}
	REQUIRE(!contains(b.m_links, static_cast<long>(limit_branch)));
	REQUIRE(contains(branch_sib.m_links, static_cast<long>(limit_branch)));

	for (uint32_t i = leaf_midkey + 1; i < limit_leaf; ++i) {
		REQUIRE(!contains(l.m_keys, i));
		REQUIRE(!contains(l.m_vals, i));
		REQUIRE(contains(leaf_sib.m_keys, i));
		REQUIRE(contains(leaf_sib.m_vals, i));
	}
}
