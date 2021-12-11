#include <random>

#include <benchmark/benchmark.h>

#include <core/storage/Storage.h>
#include <core/storage/btree/Btree.h>
using namespace internal::storage::btree;

auto item() {
	static std::random_device dev;
	static std::mt19937 rng(dev());
	static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000000);
	return dist(rng);
}

static void BM_BtreeInsertion(benchmark::State &state) {
	Btree bpt("/tmp/eu-btree-bench");
	const size_t num_insertions = state.range(0);

	for (auto i : state) {
		(void) i;
		for (std::size_t j = 0; j < num_insertions; ++j) {
			bpt.put(item(), item());
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
