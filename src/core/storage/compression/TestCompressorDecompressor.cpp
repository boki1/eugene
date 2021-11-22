#include <core/storage/compression/Compressor.h>
#include <core/storage/compression/Decompressor.h>
#include <third-party/catch2/Catch2.h>
#include <third-party/expected/Expected.h>

bool create_testing_directory(const std::string &new_structure, const int text_size) {
	for (int i = 0; i < 3; ++i) {
		std::string filesystem_structure = new_structure;
		if (i > 0)
			filesystem_structure += "/" + std::to_string(i);

		fs::path path{filesystem_structure};
		path /= "file.txt";
		if (!fs::create_directories(path.parent_path()))
			return false;

		std::ofstream ofs(path);
		for (int j = 0; j < text_size; ++j)
			ofs << "this is some text in the new file\n";
		ofs.close();
	}
	return true;
}

bool exists(const fs::path &p, fs::file_status s = fs::file_status{}) {
	return fs::status_known(s) ? fs::exists(s) : fs::exists(p);
}

bool compare_folders(const std::string &first, const std::string &second) {
	auto initial = fs::recursive_directory_iterator(first);
	for (auto &compressed: fs::recursive_directory_iterator(second)) {
		using compr = compression::storage::detail::CompressorInternal;
		std::string compressed_np = compressed.path();
		std::string initial_np = compressed.path();

		if (compressed_np != initial_np)
			return false;

		if (!compressed.is_directory() && !initial->is_directory()) {
			if (compr::return_file_info(compressed_np) != compr::return_file_info(initial_np))
				return false;
		}
		initial++;
	}
	return true;
}

bool clean(const std::map<std::string, std::string> &files) {
	return std::ranges::all_of(files.cbegin(), files.cend(),
	                           [](const auto &pair) {
		                           return fs::remove_all(pair.second);
	                           });
}

void check_initial_compressed_size(const std::string &initial_file_folder, const std::string &compressed_file) {
	unsigned long initial_size;
	std::cout << "directory: " << fs::is_directory(initial_file_folder) << std::endl;
	if (fs::is_directory(initial_file_folder))
		initial_size = std::accumulate(
			fs::recursive_directory_iterator(initial_file_folder.c_str()),
			fs::recursive_directory_iterator(), 0,
			[](auto sz, auto entry) { return is_directory(entry) ? sz : sz + file_size(entry); });
	else
		initial_size = fs::file_size(initial_file_folder);

	unsigned long long int compressed_size = fs::file_size(compressed_file);

	REQUIRE(compressed_size < initial_size);
	std::cout << std::endl << std::endl << "#############################################################" << std::endl;
	std::cout << "Passed with initial size: " << initial_size << " and compressed size: " << compressed_size
	          << std::endl;
	std::cout << "#############################################################" << std::endl << std::endl << std::endl;
}

void folder_test(std::map<std::string, std::string> &params, const int text_size) {
	for (const auto &item: params)
		REQUIRE(!exists(item.second));

	REQUIRE(create_testing_directory(params["test_dir_name"], text_size));
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

TEST_CASE("CompressorDecompressor compress_decompress", "[compressor_decompressor]")
{
	std::map<std::string, std::string> params;
	params["test_dir_name"] = "ForTesting";
	params["changed_to_initial_dir"] = "InitialDir";
	params["compressed_name"] = "Test";

	for (int i = 1; i < 5; ++i)
		folder_test(params, (int) pow(10, i));
}

/// \brief In this test - in the file are written 16 chars
/// so if you want specific number of bytes(N)
/// you give value of file_size with the formula N / 16
TEST_CASE("CompressorDecompressor specific_file_size_test", "[specific_file_size_test]")
{
	const int file_size = 32;
	const std::string &file_name = "test.txt";
	const std::string &compressed_file_name = "compressed";

	std::ofstream ofs(file_name);
	for (int j = 0; j < file_size; ++j)
		ofs << "some text here \n";
	ofs.close();

	REQUIRE(exists(file_name));

	compression::Compressor compress{
		std::vector<std::string>(1, file_name),
		compressed_file_name
	};
	compress();
	REQUIRE(exists(compressed_file_name));

	check_initial_compressed_size(file_name, compressed_file_name);
	REQUIRE(clean(std::map<std::string, std::string>{
		{"file_name", file_name},
		{"compressed_file_name", compressed_file_name}
	}));
}

TEST_CASE("CompressorDecompressor partial_decompress", "[partial_decompress]")
{
	std::map<std::string, std::string> params;
    params["test_dir_name"] = "ForTesting";
    params["changed_to_initial_dir"] = "InitialDir";
    params["compressed_name"] = "Test";

	REQUIRE(create_testing_directory(params["test_dir_name"], 100));
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
	decompress("1");
	REQUIRE(exists(params["test_dir_name"]));

	compare_folders("InitialDir/1", "ForTesting/1");
	REQUIRE(clean(params));
}