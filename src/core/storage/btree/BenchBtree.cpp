#include <benchmark/benchmark.h>

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
