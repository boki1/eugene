#include <algorithm>

#include <benchmark/benchmark.h>

#include <core/storage/compression/Compressor.h>
#include <core/storage/compression/Decompressor.h>

namespace fs = std::filesystem;

//	67108864 for 1GB ".txt" file
constexpr int file_size = (32);
constexpr std::string_view file_name = "test.txt";
constexpr std::string_view compressed_file_name = "compressed";

bool clean(const std::map<std::string, std::string> &files) {
	return std::ranges::all_of(files.cbegin(), files.cend(),
	                           [](const auto &pair) {
	                             return fs::remove_all(pair.second);
	                           });
}

class BenchDecompressor : public benchmark::Fixture {
public:
	void SetUp(const benchmark::State &) override {
		std::ofstream ofs(file_name.begin());
		for (int j = 0; j < file_size; ++j)
			ofs << "some text here \n";
		ofs.close();

		compression::Compressor compress{
			std::vector<std::string>(1, file_name.begin()),
			compressed_file_name
		};
		compress();
	}

	void TearDown(const benchmark::State &) override {
		clean({{file_name.begin(), file_name},
		       {compressed_file_name.begin(), compressed_file_name}});
	}
};

BENCHMARK_DEFINE_F(BenchDecompressor, CompressorTest)(benchmark::State &) {
	decompression::Decompressor decompress{compressed_file_name.begin()};
	decompress();
	clean({{"file_name", file_name.begin()}});
}

BENCHMARK_REGISTER_F(BenchDecompressor, CompressorTest)
->Unit(benchmark::kMillisecond);



BENCHMARK_MAIN();