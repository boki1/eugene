#pragma once
#include <algorithm>

#include <benchmark/benchmark.h>

#include <core/storage/compression/Compressor.h>
#include <core/storage/compression/Decompressor.h>

constexpr std::string_view file_name = "test.txt"; //!< File name to be used for
//!< compression/decompression benchmarking.

constexpr std::string_view compressed_file_name = "compressed"; //!< File name to be used for compressed file.

/// @brief Static function that will clean up the files
/// created by the compression/decompression benchmarks.
///
/// @param files - vector of files to be deleted.
static bool clean(const std::vector<std::string_view> &files) {
	return std::ranges::all_of(files.cbegin(), files.cend(),
	                           [](const auto &item) {
	                             return fs::remove_all(item);
	                           });
}