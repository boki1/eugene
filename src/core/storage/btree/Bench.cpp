#include <benchmark/benchmark.h>

#include <core/Util.h>
#include <core/storage/btree/Btree.h>

using namespace internal::storage::btree;
using namespace internal;

// FIXME:
// Will get implemented once merged with 'btree-crud' branch

// Dummy benchmark
static void BM_BtreeInsertion(benchmark::State &state) {
        for ([[maybe_unused]] auto _ : state) {
	}
}

BENCHMARK(BM_BtreeInsertion)
	->Unit(benchmark::kMillisecond)
	->Arg(100);

BENCHMARK_MAIN();
