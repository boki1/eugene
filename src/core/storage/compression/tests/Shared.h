#pragma once

#include <catch2/catch.hpp>

#include <iostream>
#include <map>

#include <core/storage/compression/Compressor.h>
#include <core/storage/compression/Decompressor.h>

bool clean(const std::map<std::string, std::string> &files) {
	return std::ranges::all_of(files.cbegin(), files.cend(),
	                           [](const auto &pair) {
	                             return fs::remove_all(pair.second);
	                           });
}

bool create_testing_directory(const std::string_view &new_structure, const int text_size,
                              const std::string_view &text) {
	for (int i = 0; i < 3; ++i) {
		std::string filesystem_structure = new_structure.begin();
		if (i > 0)
			filesystem_structure += "/" + std::to_string(i);

		fs::path path{filesystem_structure};
		path /= "file.txt";
		if (!fs::create_directories(path.parent_path()))
			return false;

		std::ofstream ofs(path);
		for (int j = 0; j < text_size; ++j)
			ofs << text;
		ofs.close();
	}
	return true;
}

bool exists(const fs::path &p, fs::file_status s = fs::file_status{}) {
	return fs::status_known(s) ? fs::exists(s) : fs::exists(p);
}

bool compare_folders(const std::string &first, const std::string &second) {
	auto initial = fs::recursive_directory_iterator(first);
	for (auto &compressed : fs::recursive_directory_iterator(second)) {
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

void check_initial_compressed_size(const std::string &initial_file_folder, const std::string &compressed_file) {
	using compr = compression::storage::detail::CompressorInternal;
	unsigned long initial_size = compr::get_file_folder_size(initial_file_folder);

	unsigned long long int compressed_size = compr::get_file_folder_size(compressed_file);

	REQUIRE(compressed_size < initial_size);
	std::cout << "#############################################################" << std::endl;

	std::cout << initial_file_folder << " file/folder passed with initial size: " << initial_size << std::endl;

	std::cout << compressed_file << " file/folder passed with compressed size: "
	          << compressed_size << std::endl;

	std::cout << "#############################################################"
	          << std::endl << std::endl;
}

static std::string generate_random_string_sequence(unsigned int range) {
	std::string sequence;
	for (unsigned int j = 0; j < range; ++j) {
		char c = static_cast<char>(random() % 30);
		sequence += c;
	}
	return sequence;
}