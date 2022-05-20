#include <core/storage/compression/tests/Shared.h>

void folder_test(std::map<std::string, std::string> &params,
                 const int text_size,
                 const std::string_view &text_in_file) {
	for (const auto &item : params)
		REQUIRE(!exists(item.second));

	REQUIRE(create_testing_directory(params["test_dir_name"],
	                                 text_size,
	                                 text_in_file));
	REQUIRE(exists(params["test_dir_name"]));

	compression::Compressor compress{
		std::vector<std::string>(1, params["test_dir_name"]),
		params["compressed_name"]
	};
	compress();
	REQUIRE(exists(params["compressed_name"]));

	fs::rename(params["test_dir_name"], params["changed_to_initial_dir"]);
	REQUIRE(exists(params["changed_to_initial_dir"]));

	decompression::Decompressor decompress{params["compressed_name"]};
	decompress();
	REQUIRE(exists(params["test_dir_name"]));

	REQUIRE(compare_folders("InitialDir", "ForTesting"));

	check_initial_compressed_size(params["test_dir_name"], params["compressed_name"]);
	REQUIRE(clean(params));
}

TEST_CASE("CompDecomp comp_decomp", "[compressor_decompressor]") {
	std::map<std::string, std::string> params;
	params["test_dir_name"] = "ForTesting";
	params["changed_to_initial_dir"] = "InitialDir";
	params["compressed_name"] = "Test";

	const std::string text_in_file = generate_random_string_sequence(34);

	const int number_of_test_run = 5;

//	starts from 1 because if it starts from 0, the file size will be too small
	for (int i = 1; i <= number_of_test_run; ++i)
		folder_test(params,
		            (int) pow(10, i),
		            text_in_file);
}