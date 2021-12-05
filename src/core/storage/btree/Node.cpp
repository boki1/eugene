#include <ranges>
#include <optional>

#include "catch2/catch.hpp"

#include <core/storage/Position.h>
#include <core/storage/Page.h>
#include <core/storage/btree/Btree.h>

using namespace std::ranges::views;

using namespace internal::storage;
using namespace internal::storage::btree;

using Nod = Node<Config>;
using Metadata = Nod::Metadata;
using Branch = Nod::Branch;
using Leaf = Nod::Leaf;

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

