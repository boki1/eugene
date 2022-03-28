#include <core/storage/compression/tests/Shared.h>

/// \brief In this test - in the file are written 16 chars
/// so if you want specific number of bytes(N)
/// you give value of file_size with the formula N / 16
TEST_CASE("Compressor specific_file_size_test", "[specific_file_size_test]") {
	const int file_size = 32;
	const std::string &file_name = "test.txt";
	const std::string &compressed_file_name = "compressed";

	const std::string_view text_in_file = "some text here.\n";

	std::ofstream ofs(file_name);
	for (int j = 0; j < file_size; ++j)
		ofs << text_in_file;
	ofs.close();

	REQUIRE(exists(file_name));

	compression::Compressor compress{
		std::vector<std::string>(1, file_name),
		compressed_file_name
	};
	compress();
	REQUIRE(exists(compressed_file_name));

	check_initial_compressed_size(file_name, compressed_file_name);
	REQUIRE(clean({{"file_name", file_name},
	               {"compressed_file_name", compressed_file_name}}));
}