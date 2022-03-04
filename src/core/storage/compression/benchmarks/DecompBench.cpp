#include <core/storage/compression/benchmarks/Shared.h>

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
		clean({{"file_name", file_name.begin()},
		       {"compressed_file_name", compressed_file_name.begin()}});
		st.ResumeTiming();
	}
}

BENCHMARK(BM_Decompression)
	->Unit(benchmark::kMillisecond)
	->Arg(16);

BENCHMARK_MAIN();