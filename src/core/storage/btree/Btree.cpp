#include <iostream>
#include <map>

#include <catch2/catch.hpp>
#include <fmt/core.h>

#include <core/storage/Storage.h>
#include <core/storage/btree/Btree.h>
using namespace internal::storage::btree;

auto item() {
	static std::random_device dev;
	static std::mt19937 rng(dev());
	static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000000);
	return dist(rng);
}

TEST_CASE("Btree operations", "[btree]") {
	Btree bpt("/tmp/eu-btree-pgcache_name");
	REQUIRE(bpt.contains(42) == false);
	REQUIRE(bpt.get(42).has_value() == false);

	bpt.put(42, 1);
	REQUIRE(bpt.root().leaf().m_keys[0] == 42);
	REQUIRE(bpt.root().leaf().m_vals[0] == 1);

	REQUIRE(bpt.get(42).value() == 1);

	std::map<Config::Key, Config::Val> backup;
	while (backup.size() != 200000) {
		auto key = item();
		auto val = key;//item();
		bpt.put(key, val);
		backup.emplace(key, val);
	}

	int i = 1; for (const auto &[key, val] : backup) {
		REQUIRE(bpt.get(key).value() == val);
		fmt::print("-- {} -> Key '{}' found mapped to '{}'\n", i++, key, val);
	}
}
