#pragma once

#include <concepts>
#include <cstdint>

#include <nop/structure.h>

#include <core/Util.h>
#include <core/storage/Pager.h>

namespace internal::storage::btree {

/// Btree configuration
/// Contains information about the entry types (<key, val> types), pager algorithms, sizes, etc.
/// It is expected that the used derives from this class and defines its own configuration setup for the specific use case.
struct Config {
	using Key = int;
	using Val = int;
	using Ref = int;

	using PageAllocatorPolicy = FreeListAllocator;
	using PageEvictionPolicy = LRUCache;
	using PagerType = Pager<PageAllocatorPolicy, PageEvictionPolicy>;

	static inline constexpr int PAGE_CACHE_SIZE = 1_MB;
	static inline constexpr bool APPLY_COMPRESSION = true;

	static inline constexpr int BRANCHING_FACTOR_LEAF = 0;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = 0;

	/// Marks whether relaxed rebalancing should be performed on remove operations
	static inline constexpr bool BTREE_RELAXED_REMOVES = true;
};

#define BTREE_OF_ORDER(m)\
	static inline constexpr int BRANCHING_FACTOR_LEAF = (m);\
	static inline constexpr int BRANCHING_FACTOR_BRANCH = (m)

template<typename C>
concept BtreeConfig = std::is_base_of_v<Config, C>;

using DefaultConfig = Config;

}// namespace internal::storage::btree
