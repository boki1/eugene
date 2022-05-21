#include <core/storage/compression/benchmarks/Shared.h>

/// @brief Compression benchmark
///
/// \param st - google benchmark state object that contains the
/// benchmark parameters
static void BM_Compression(benchmark::State &st) {
	for (auto _ : st) {
		st.PauseTiming();
		std::ofstream ofs(file_name.begin());

		ofs << generate_random_string_sequence(st.range(0));

		ofs.close();
		st.ResumeTiming();

		compression::Compressor{
			std::vector<std::string>(1, file_name.begin()),
			compressed_file_name
		}();

		st.PauseTiming();
		clean({file_name.begin(), compressed_file_name.begin()});
		st.ResumeTiming();
	}
}

//	1*1024*1024*1024 for 1GB ".txt" file -> ~105 seconds
//  1*1024*1024 for 1MB ".txt" file -> ~110ms
//  64 for 1KB ".txt" file
BENCHMARK(BM_Compression)
	->Unit(benchmark::kMillisecond)
	->Iterations(10)
	->Arg(1 * 1024 * 1024);

BENCHMARK_MAIN();