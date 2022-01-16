#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <list>
#include <optional>
#include <fstream>
#include <string_view>
#include <unordered_map>

#include <cppcoro/generator.hpp>

#include <core/SizeMetrics.h>
#include <core/Util.h>

namespace internal::storage {

constexpr static std::size_t PAGE_SIZE = 4_KB;
constexpr static std::size_t PAGECACHE_SIZE = 1_MB;

using Position = unsigned long int;
using Page = std::array<std::uint8_t, PAGE_SIZE>;

/// Page with position
struct PagePos {
	Page page;
	Position pos;
};

class StackSpaceAllocator {
	Position m_cursor = 0;

public:
	[[nodiscard]] constexpr Position alloc() {
		auto tmp = m_cursor;
		m_cursor += PAGE_SIZE;
		return tmp;
	}
};

using CacheEvictionResult = std::optional<PagePos>;

template<typename Policy>
class PageCache {
	friend Policy;

	[[nodiscard]] constexpr auto evict() { return Policy::evict(*this); }

	struct CacheEntry {
		Page m_page;
		std::list<Position>::const_iterator m_cit;
		bool m_dirty;
	};

public:
	constexpr explicit PageCache(std::size_t limit = PAGECACHE_SIZE / PAGE_SIZE) : m_limit{limit} {
		assert(m_limit >= 1);
	}

	[[nodiscard]] constexpr optional_ref<Page> get(Position pos) {
		if (!m_index.contains(pos))
			return {};

		auto it = m_index.find(pos);
		// assert(it != m_index.end());

		m_tracker.splice(m_tracker.cend(), m_tracker, it->second.m_cit);
		return it->second.m_page;
	}

	[[nodiscard]] constexpr CacheEvictionResult place(Position pos, Page &&page) {
		// assert(m_index.size() <= m_limit);

		CacheEvictionResult evict_res;

		const auto it = m_index.find(pos);
		if (it == m_index.cend()) {
			if (m_tracker.size() >= m_limit)
				evict_res = evict();
			m_tracker.push_back(pos);
		} else {
			/// Move it to the end of `m_tracker`.
			m_tracker.splice(m_tracker.cend(), m_tracker, it->second.m_cit);

			/// TODO: Perhaps perform some fast hashing to check whether the page was actually modified
			/// in order to not make unneeded disk IO
		}

		m_index[pos] = CacheEntry{
		        .m_page = std::move(page),
		        .m_cit = m_tracker.cend(),
		        .m_dirty = true};

		return evict_res;
	}

	[[nodiscard]] cppcoro::generator<CacheEvictionResult> flush() {
		while (!m_tracker.empty())
			co_yield evict();
	}

private:
	const std::size_t m_limit;
	std::unordered_map<Position, CacheEntry> m_index;
	std::list<Position> m_tracker;
};

struct LRUCache {
	[[nodiscard]] static CacheEvictionResult evict(PageCache<LRUCache> &cache) {
		CacheEvictionResult res;
		const Position pos = cache.m_tracker.front();
		const auto &cached = cache.m_index.at(pos);
		if (cached.m_dirty)
			res = PagePos{.page = cached.m_page, .pos = pos};
		cache.m_index.erase(pos);
		cache.m_tracker.pop_front();
		return res;
	}
};

class Pager {
	using AllocatorPolicy = StackSpaceAllocator;
	friend AllocatorPolicy;

	using CacheEvictionPolicy = LRUCache;

private:
	[[nodiscard]] Page read(Position pos) {
		Page page;
		m_disk.seekp(pos);
		m_disk.read(reinterpret_cast<char *>(&page), PAGE_SIZE);
		return page;
	}

	void write(Position pos, const Page &page) {
		m_disk.seekp(pos);
		m_disk.write(reinterpret_cast<const char *>(page.data()), PAGE_SIZE);
	}

public:
	explicit Pager(std::string_view identifier)
	    : m_identifier{identifier},
	      m_disk{identifier.data()} {}

	[[nodiscard]] constexpr Position alloc() {
		return m_allocator.alloc();
	}

	Page get(Position pos) {
		if (auto p = m_cache.get(pos); p)
			return p->get();
		Page p = read(pos);
		if (auto evict_res = m_cache.place(pos, Page(p)); evict_res)
			write(evict_res->pos, evict_res->page);
		return p;
	}

	void place(Position pos, Page &&page) {
		if (auto evict_res = m_cache.place(pos, std::move(page)); evict_res)
			write(evict_res->pos, evict_res->page);
	}

	void flush_cache() {
		for (auto evict_res: m_cache.flush())
			if (evict_res)
				write(evict_res->pos, evict_res->page);
	}

private:
	AllocatorPolicy m_allocator;
	PageCache<CacheEvictionPolicy> m_cache;
	std::string_view m_identifier;
	std::fstream m_disk;
};

}// namespace internal::storage
