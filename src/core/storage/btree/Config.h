#pragma once

#include <concepts>
#include <cstdint>

#include <nop/structure.h>

#include <core/SizeMetrics.h>
#include <core/storage/Pager.h>

namespace internal::storage::btree {

/// Btree configuration
/// Contains information about the entry types (<key, val> types), pager algorithms, sizes, etc.
/// It is expected that the used derives from this class and defines its own configuration setup for the specific use case.
struct Config {
	using Key = int;
	using Val = int;
	using Ref = int;

	using PageAllocatorPolicy = StackSpaceAllocator;
	using PageEvictionPolicy = LRUCache;

	static inline constexpr int PAGE_CACHE_SIZE = 1_MB;
	static inline constexpr bool APPLY_COMPRESSION = true;

	static inline constexpr int BRANCHING_FACTOR_LEAF = 0;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = 0;
};

template<typename C>
concept BtreeConfig = std::is_base_of_v<Config, C>;

using DefaultConfig = Config;

}// namespace internal::storage::btree
