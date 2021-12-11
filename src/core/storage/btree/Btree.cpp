#include "catch2/catch.hpp"

#include <core/storage/Storage.h>
#include <core/storage/btree/Btree.h>

#include <iostream>
#include <ranges>
using namespace std::ranges::views;

using namespace internal::storage::btree;

TEST_CASE("Btree operations", "[btree]") {
	Btree bpt("/tmp/eu-btree-pgcache_name");
	REQUIRE(bpt.contains(42) == false);
	REQUIRE(bpt.get(42).has_value() == false);

	bpt.put(42, 1);
	REQUIRE(bpt.root().leaf().m_keys[0] == 42);
	REQUIRE(bpt.root().leaf().m_vals[0] == 1);

	REQUIRE(bpt.get(42).value() == 1);
}
