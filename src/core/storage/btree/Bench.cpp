#include <random>

#include <benchmark/benchmark.h>

#include <core/Util.h>
#include <core/storage/btree/Btree.h>

using namespace internal::storage::btree;
using namespace internal;

static void BM_BtreeInsertion(benchmark::State &state) {
	Btree bpt("/tmp/eu-btree-bench");
	const size_t num_insertions = state.range(0);

	for (auto i : state) {
		(void) i;
		for (std::size_t j = 0; j < num_insertions; ++j) {
			bpt.insert(random_item<DefaultConfig::Key>(), random_item<DefaultConfig::Key>());
		}
	}
}

BENCHMARK(BM_BtreeInsertion)
	->Unit(benchmark::kMillisecond)
	->Arg(100)
	->Arg(500)
	->Arg(1000)
	->Arg(5000)
	->Arg(10000)
	->Arg(50000)
	->Arg(100000)
	->Arg(500000)
	->Arg(1000000)
	->Arg(5000000)
	->Arg(10000000);

BENCHMARK_MAIN();
