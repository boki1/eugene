#include <internal/btree/Btree.h>
#include <internal/btree/BtreePrinter.h>

#include <iostream>
#include <ranges>
using namespace std::ranges::views;

using namespace internal::btree;
using namespace internal::btree::util;

using config_ = DefaultBtreeConfig;
using bt_t = Btree<config_>;
using btprinter_t = BtreeYAMLPrinter<config_>;
using node_t = bt_t::Node;
using it_t = bt_t::Iterator;

// Btree Print Single Layer
int main() {
  bt_t bpt;

//  for (const uint32_t index : iota(0) | take(node_t::records_())) {
  for (const uint32_t index : iota(0, 10)) {
    const config_::Key key = index;
    const config_::Val val = index + 1;

    [[maybe_unused]] const auto it = bpt.insert(key, val);
  }

  btprinter_t printer{bpt};
  printer.print();
}
