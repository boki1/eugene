#include "catch2/catch.hpp"

#include <core/storage/Storage.h>
#include <core/storage/btree/Btree.h>

#include <iostream>
#include <ranges>
using namespace std::ranges::views;

using namespace internal::storage::btree;

TEST_CASE("Btree operations", "[btree]") {
	[[maybe_unused]] Btree bpt("/tmp/eu-btree-pgcache_name"); }
