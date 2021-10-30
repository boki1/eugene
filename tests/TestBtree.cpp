#include <external/catch2/Catch2.h>

#include <internal/storage/Storage.h>
#include <internal/storage/btree/Btree.h>

#include <iostream>
#include <ranges>
using namespace std::ranges::views;

using namespace internal::btree;
using config_ = DefaultBtreeConfig;
using bt_t = Btree<config_>;
using node_t = bt_t::Node;
using it_t = bt_t::Iterator;

TEST_CASE("Btree init", "[btree]") { [[maybe_unused]] Btree bpt{}; }

TEST_CASE("Btree find", "[btree]") {
	bt_t bpt{};
	bpt.prepare_root_for_inmem();
	auto it1 = bpt.find(13u);
	REQUIRE(!it1.has_value());
}

TEST_CASE("Btree insert", "[btree]") {
	bt_t bpt;
	bpt.prepare_root_for_inmem();


	REQUIRE(node_t::num_links_per_branch() - 1 == node_t::num_records_per_node());

	const uint32_t recs = node_t::num_records_per_node();
	for (const uint32_t index : iota(0) | take(recs)) {
		const config_::Key key = index;
		const config_::Val val = index + 1;
		REQUIRE(!bpt.get(key).has_value());
		const auto it = bpt.insert(key, val);
		const auto end_it = bpt.end();
		REQUIRE(it != bpt.end());
		const auto opt = bpt.get(key);
		REQUIRE(opt.has_value());
		REQUIRE(opt.value() == val);
	}
}

TEST_CASE("Btree iterator", "[btree]") {
	Btree bpt{};
	bpt.prepare_root_for_inmem();
	REQUIRE(bpt.begin() == bpt.end());
	REQUIRE(bpt.begin()->is_leftmost());
	REQUIRE(bpt.begin()->is_rightmost());
	REQUIRE(bpt.begin()->is_root());

	bpt.insert(13u, 19u);
	REQUIRE(bpt.begin() != bpt.end());

	auto do_checks = [&](node_t &thing) {
		REQUIRE(thing.numfilled() == 1);
		REQUIRE(thing.at(0) == 13u);
		REQUIRE(thing.is_leaf());
		REQUIRE(thing.leaf().vals_.at(0) == 19u);
	};

	for (auto &thing : bpt)
		do_checks(thing);

	for (auto rit = bpt.end(); rit != bpt.begin(); --rit)
		do_checks(*rit);
}
