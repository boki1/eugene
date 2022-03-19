#pragma once

#include <concepts>
#include <cstdint>

#include <core/Util.h>
#include <core/storage/Pager.h>

namespace internal {

/// Eugene configuration
/// Contains information about the entry types (<key, val> types), pager algorithms, sizes, etc.
/// It is expected that the used derives from this class and defines its own configuration setup for the specific use case.
struct Config {
	using Key = int;
	using Val = int;
	using Ref = int;

	using PageAllocatorPolicy = storage::FreeListAllocator;
	using PageEvictionPolicy = storage::LRUCache;
	using PagerType = storage::Pager<PageAllocatorPolicy, PageEvictionPolicy>;

	static inline constexpr int PAGE_CACHE_SIZE = 1_MB;
	static inline constexpr bool APPLY_COMPRESSION = true;

	static inline constexpr int BRANCHING_FACTOR_LEAF = 0;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = 0;

	static inline constexpr bool PERSISTENT = true;

	static inline constexpr bool BTREE_RELAXED_REMOVES = true;

	static inline constexpr bool DYN_ENTRIES = false;
	using RealVal = Val;
	using RealValSizeEvaluator_type = std::size_t (*)(const void *);
	static inline constexpr RealValSizeEvaluator_type RealValSizeEvaluator = nullptr;
};

template<typename C>
concept EugeneConfig = std::is_base_of_v<Config, C>;

}// namespace internal
