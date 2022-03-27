#include <core/storage/compression/benchmarks/Shared.h>

/// @brief Decompression benchmark
///
/// \param st - google benchmark state object that contains the
///// benchmark parameters
static void BM_Decompression(benchmark::State &st) {
	for (auto _ : st) {
		st.PauseTiming();
		std::ofstream ofs(file_name.begin());
		for (int j = 0; j < st.range(0); ++j)
			ofs << "some text here \n";
		ofs.close();

		compression::Compressor compress{
			std::vector<std::string>(1, file_name.begin()),
			compressed_file_name
		};
		compress();
		st.ResumeTiming();

		decompression::Decompressor decompress{compressed_file_name.begin()};
		decompress();

		st.PauseTiming();
		clean({file_name.begin(), compressed_file_name.begin()});
		st.ResumeTiming();
	}
}

//	67108864 for 1GB ".txt" file
//  65536 for 1MB ".txt" file
//  64 for 1KB ".txt" file
BENCHMARK(BM_Decompression)
	->Unit(benchmark::kMillisecond)
	->Iterations(10)
	->Arg(65536);

BENCHMARK_MAIN();