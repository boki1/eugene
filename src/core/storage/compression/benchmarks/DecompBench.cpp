#include <core/storage/compression/benchmarks/Shared.h>

/// @brief Decompression benchmark
///
/// \param st - google benchmark state object that contains the
///// benchmark parameters
static void BM_Decompression(benchmark::State &st) {
	for (auto _ : st) {
		st.PauseTiming();
		std::ofstream ofs(file_name.begin());

		ofs << generate_random_string_sequence(st.range(0));

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

//	1*1024*1024*1024 for 1GB ".txt" file -> ~71s
//  1*1024*1024 for 1MB ".txt" file -> ~110ms
//  64 for 1KB ".txt" file
BENCHMARK(BM_Decompression)
	->Unit(benchmark::kMillisecond)
	->Iterations(10)
	->Arg(1 * 1024 * 1024);

BENCHMARK_MAIN();