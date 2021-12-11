#pragma once

#include <concepts>
#include <cstdint>

#include <core/SizeMetrics.h>

namespace internal::storage::btree {

struct Config {
	using Key = uint32_t;
	using Val = uint32_t;
	using Ref = uint32_t;

	static inline constexpr int NUM_RECORDS = 128;
	static inline constexpr int BTREE_NODE_BREAK_POINT = (NUM_RECORDS - 1) / 2;
	static inline constexpr int PAGE_CACHE_SIZE = 1_MB;
	static inline constexpr bool APPLY_COMPRESSION = true;
	static inline constexpr int BTREE_NODE_SIZE = 4_KB;
};

template<typename C>
concept BtreeConfig = std::is_base_of_v<Config, C>;

using DefaultConfig = Config;

}// namespace internal::storage::btree
