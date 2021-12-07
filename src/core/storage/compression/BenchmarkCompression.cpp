#include <benchmark/cppbenchmark.h>
#include <core/storage/compression/Compressor.h>
#include <core/storage/compression/Decompressor.h>

namespace fs = std::filesystem;

//	67108864 for 1GB txt file
constexpr int file_size = (32);
constexpr std::string_view file_name = "test.txt";
constexpr std::string_view compressed_file_name = "compressed";

bool clean(const std::map<std::string, std::string> &files) {
	return std::ranges::all_of(files.cbegin(), files.cend(),
	                           [](const auto &pair) {
		                           return fs::remove_all(pair.second);
	                           });
}

class CompressorFixture : public virtual CppBenchmark::Fixture {
protected:
	void Initialize(CppBenchmark::Context &) override {
		std::ofstream ofs(file_name.begin());
		for (int j = 0; j < file_size; ++j)
			ofs << "some text here \n";
		ofs.close();
	}
	void Cleanup(CppBenchmark::Context &) override {
		clean({{"file_name", file_name.begin()},
		       {"compressed_file_name", compressed_file_name.begin()}});
	}
};

class DecompressorFixture {
protected:
	DecompressorFixture() {
		std::ofstream ofs(file_name.begin());
		for (int j = 0; j < file_size; ++j)
			ofs << "some text here \n";
		ofs.close();

		compression::Compressor compress{
			std::vector<std::string>(1, file_name.begin()),
			compressed_file_name};
		compress();
	}
	~DecompressorFixture() {
		clean({{"file_name", file_name.begin()},
		       {"compressed_file_name", compressed_file_name.begin()}});
	}
};

BENCHMARK_FIXTURE(DecompressorFixture, "decompressor") {
	decompression::Decompressor decompress{compressed_file_name.begin()};
	decompress();
	clean({{"file_name", file_name.begin()}});
}
BENCHMARK_FIXTURE(CompressorFixture, "compressor") {
	compression::Compressor compress{
		std::vector<std::string>(1, file_name.begin()),
		compressed_file_name};
	compress();
}

BENCHMARK_MAIN()
