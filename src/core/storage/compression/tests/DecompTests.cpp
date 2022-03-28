#include <core/storage/compression/tests/Shared.h>

TEST_CASE("CompressorDecompressor partial_decompress", "[partial_decompress]") {
	std::map<std::string, std::string> params;
	params["test_dir_name"] = "ForTesting";
	params["changed_to_initial_dir"] = "InitialDir";
	params["compressed_name"] = "Test";

	const std::string_view partial_decompress_text = "this is some text in the new file\n";
	const int partial_decompress_text_size = 100;

	std::string_view partial_decompress_name = "1";


	REQUIRE(create_testing_directory(params["test_dir_name"],
	                                 partial_decompress_text_size,
									 partial_decompress_text));
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
	decompress(partial_decompress_name);

	REQUIRE(exists(params["test_dir_name"]));
	REQUIRE(compare_folders("InitialDir/1", "ForTesting/1"));

	std::cout << "#############################################################" << std::endl;
	std::cout << "Partial decompress of initial folder: " << params["test_dir_name"] << "/"
	          << partial_decompress_name << std::endl;
	std::cout << "Success" << std::endl;
	std::cout << "#############################################################\n\n";
	REQUIRE(clean(params));
}