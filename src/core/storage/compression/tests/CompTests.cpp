#include <core/storage/compression/tests/Shared.h>

/// \brief In this test - in the file is written 1KB
TEST_CASE("Compressor specific_file_size_test", "[specific_file_size_test]") {
	const int file_size = 1 * 1024; //!< 1KB
	const std::string &file_name = "test.txt";
	const std::string &compressed_file_name = "compressed";

	const std::string text_in_file = generate_random_string_sequence(file_size);

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