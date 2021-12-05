#include <fstream>
#include <optional>
#include <random>
#include <ranges>
#include <unordered_map>

#include "catch2/catch.hpp"

#include <core/storage/Page.h>
#include <core/storage/Position.h>
#include <core/storage/btree/Btree.h>

using namespace std::ranges::views;

using namespace internal::storage;
using namespace internal::storage::btree;

using Nod = Node<Config>;
using Metadata = Nod::Metadata;
using Branch = Nod::Branch;
using Leaf = Nod::Leaf;

static Nod make_node() {
	static std::random_device dev;
	std::mt19937 rng(dev());
	std::uniform_int_distribution<std::mt19937::result_type> dist128(1, 128);

	std::vector<Position> a;
	std::vector<uint32_t> b;
	for (std::size_t i = 0; i < dist128(rng); ++i)
		if (dist128(rng) % 2)
			a.push_back(dist128(rng));
		else
			b.push_back(Position(dist128(rng)));

	Metadata metadata;
	if (dist128(rng) % 2)
		metadata = Metadata(Branch(b, a));
	else
		metadata = Metadata(Leaf(b, b));

	Position p;
	if (auto pp = dist128(rng); pp < 75)
		p.set(pp);

	return Node(std::move(metadata), p, dist128(rng) % 2);
}

TEST_CASE("Node serialization", "[btree]") {
	auto node1 = Nod(Metadata(Branch({1}, {1})), {}, false);
	auto node2 = Nod(Metadata(Leaf({2}, {2})), 13, true);

	auto node1_as_page = node1.make_page().value();
	auto node2_as_page = node2.make_page().value();

	auto node1_from_page = Nod::from_page(node1_as_page).value();
	auto node2_from_page = Nod::from_page(node2_as_page).value();

	REQUIRE(node1_from_page == node1);
	REQUIRE(node2_from_page == node2);
}

TEST_CASE("Persitent nodes", "[btree]") {
	Pager pr("/tmp/eu-persistent-nodes-pager");
	Page page;

	auto node1_pos = pr.alloc();
	auto node1 = Nod(Metadata(Branch({1}, {1})), {}, false);
	auto node1_as_page = node1.make_page().value();
	pr.sync(node1_as_page, node1_pos);
	auto node1_from_page = Nod::from_page(pr.fetch(page, node1_pos)).value();
	REQUIRE(node1_from_page == node1);
	REQUIRE(node1_from_page.is_root() == false);
	REQUIRE(node1_from_page.parent().is_set() == false);

	auto node2_pos = pr.alloc();
	auto node2 = Nod(Metadata(Leaf({2}, {2})), 13, true);
	auto node2_as_page = node2.make_page().value();
	pr.sync(node2_as_page, node2_pos);
	auto node2_from_page = Nod::from_page(pr.fetch(page, node2_pos)).value();
	REQUIRE(node2_from_page == node2);
	REQUIRE(node2_from_page.is_root() == true);
	REQUIRE(node2_from_page.parent() == Position(13));
}

TEST_CASE("Paging with many random nodes", "[btree]") {
	Pager pr("/tmp/eu-many-persistent-nodes-pager");
	Page page;

	std::unordered_map<Position, Nod> nodes;
	for (std::size_t i = 0; i < 128; ++i) {
		auto node = make_node();
		auto node_pos = pr.alloc();
		nodes[node_pos] = node;
		auto node_as_page = node.make_page().value();
		pr.sync(node_as_page, node_pos);
		auto node_from_page = Nod::from_page(pr.fetch(page, node_pos)).value();
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
		auto node_from_page = Nod::from_page(pr.fetch(page, pos)).value();
		REQUIRE(node_from_page == node);
	}
}
