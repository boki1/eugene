#include <external/catch2/Catch2.h>

#include <internal/btree/Btree.h>
#include <internal/storage/Storage.h>

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
  auto it1 = bpt.find(13u);
  REQUIRE(!it1.has_value());
}

TEST_CASE("Btree insert", "[btree]") {
  bt_t bpt;
  REQUIRE(node_t::records_() > 10);
  REQUIRE(node_t::links_() > 11);
  REQUIRE(node_t::links_() - 1 == node_t::records_());

  for (const uint32_t index : iota(0) | take(node_t::records_() - 1)) {
    const config_::Key key = index;
    const config_::Val val = index + 1;

    REQUIRE(!bpt.get(key).has_value());
    const auto it = bpt.insert(key, val);
    const auto end_it = bpt.end();
    REQUIRE(it != bpt.end());
    REQUIRE(it->at(index) == key);
    REQUIRE(it.index() == key);
    REQUIRE(it.key() == std::make_optional(key));
    REQUIRE(it.val() == std::make_optional(val));
    const auto opt = bpt.get(key);
    REQUIRE(opt.has_value());
    REQUIRE(opt.value() == val);
  }
}

TEST_CASE("Btree iterator", "[btree]") {
  [[maybe_unused]] Btree bpt{};
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
