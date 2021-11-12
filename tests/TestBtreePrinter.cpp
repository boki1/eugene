#include <core/storage/btree/Btree.h>
#include <core/storage/btree/BtreePrinter.h>

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
	bpt.prepare_root_for_inmem();

	for (const uint32_t index : iota(0) | take(node_t::num_records_per_node())) {
		const config_::Key key = index;
		const config_::Val val = index + 1;

		[[maybe_unused]] const auto it = bpt.insert(key, val);
		if (!bpt.get(key)) {
			std::cerr << "failed when looking for inserted key = " << key << '\n';
			return -1;
		}
	}

	btprinter_t printer{bpt};
	printer.print();
}
