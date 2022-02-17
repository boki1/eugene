#include <core/storage/compression/benchmarks/Shared.h>

/// @brief Compression benchmark
///
/// \param st - google benchmark state object that contains the
/// benchmark parameters
static void BM_Compression(benchmark::State &st) {
	for (auto _ : st) {
		st.PauseTiming();
		std::ofstream ofs(file_name.begin());
		for (int j = 0; j < st.range(0); ++j)
			ofs << "some text here \n";
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

//	67108864 for 1GB ".txt" file
//  65536 for 1MB ".txt" file
//  64 for 1KB ".txt" file
BENCHMARK(BM_Compression)
	->Unit(benchmark::kMillisecond)
	->Iterations(10)
	->Arg(65536);

BENCHMARK_MAIN();