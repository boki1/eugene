#include <core/storage/compression/benchmarks/Shared.h>

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
		clean({{"file_name", file_name.begin()},
		       {"compressed_file_name", compressed_file_name.begin()}});
		st.ResumeTiming();
	}
}

//	67108864 for 1GB ".txt" file
BENCHMARK(BM_Compression)
	->Unit(benchmark::kMillisecond)
	->Arg(16);

BENCHMARK_MAIN();