#pragma once

#include <concepts>
#include <cstdint>

#include <core/storage/Page.h>
#include <core/storage/Position.h>
#include <core/SizeMetrics.h>

namespace internal::storage::btree {

struct Config {
	using Key = int;
	using Val = int;
	using Ref = int;

	static inline constexpr int PAGE_CACHE_SIZE = 1_MB;
	static inline constexpr bool APPLY_COMPRESSION = true;
};

template<typename C>
concept BtreeConfig = std::is_base_of_v<Config, C>;

using DefaultConfig = Config;

}// namespace internal::storage::btree
