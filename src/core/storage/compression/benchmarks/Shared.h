#pragma once
#include <algorithm>

#include <benchmark/benchmark.h>

#include <core/storage/compression/Compressor.h>
#include <core/storage/compression/Decompressor.h>


constexpr std::string_view file_name = "test.txt";
constexpr std::string_view compressed_file_name = "compressed";

static bool clean(const std::map<std::string, std::string> &files) {
	return std::ranges::all_of(files.cbegin(), files.cend(),
	                           [](const auto &pair) {
	                             return fs::remove_all(pair.second);
	                           });
}