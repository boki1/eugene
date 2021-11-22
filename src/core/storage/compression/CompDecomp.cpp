#include <core/storage/compression/Compressor.h>
#include <core/storage/compression/Decompressor.h>
namespace fs = std::filesystem;

bool exists(const fs::path &p, fs::file_status s = fs::file_status{}) {
	return fs::status_known(s) ? fs::exists(s) : fs::exists(p);
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

	std::cout << std::endl
	          << std::endl
	          << "#############################################################" << std::endl;
	std::cout << "Passed with initial size: " << initial_size << " and compressed size: " << compressed_size
	          << std::endl;
	std::cout << "#############################################################" << std::endl
	          << std::endl
	          << std::endl;
}

bool clean(const std::map<std::string, std::string> &files) {
	return std::ranges::all_of(files.cbegin(), files.cend(),
	                           [](const auto &pair) {
		                           return fs::remove_all(pair.second);
	                           });
}

int main() {
	const int file_size = (67108864);
	const std::string &file_name = "test.txt";
	const std::string &compressed_file_name = "compressed";

	std::ofstream ofs(file_name);
	for (int j = 0; j < file_size; ++j)
		ofs << "some text here \n";
	ofs.close();

	auto start = std::chrono::high_resolution_clock::now();

	compression::Compressor compress{
	        std::vector<std::string>(1, file_name),
	        compressed_file_name};
	compress();

	auto stop = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::seconds>(stop - start);
	std::cout << "Compression time: " << duration.count() << " s" << std::endl;

	clean({{"", file_name}});

	start = std::chrono::high_resolution_clock::now();

	decompression::Decompressor decompress{compressed_file_name};
	decompress();

	stop = std::chrono::high_resolution_clock::now();
	duration = std::chrono::duration_cast<std::chrono::seconds>(stop - start);
	std::cout << "Decompression time: " << duration.count() << " s" << std::endl;

	clean({{"file_name", file_name},
	       {"compressed_file_name", compressed_file_name}});
}