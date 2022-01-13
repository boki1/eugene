#pragma once

#include <concepts>
#include <list>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>

#include <core/SizeMetrics.h>
#include <core/storage/Page.h>
#include <core/storage/Position.h>

namespace internal::storage {

//! Buffer pool structure.
//! Implemented as a write-behind LRU cache.
//! Controls access to pages using an internal pager.
class PageCache {

	struct CacheEntry {
		Page m_page;
		std::list<Position>::const_iterator m_cit;
	};

	void evict() {
		const auto &pos = m_tracker.back();
		const auto &page = m_index.at(pos).m_page;
		if (page.dirty())
			m_pager.sync(page, pos);
		m_index.erase(pos);
		m_tracker.pop_back();
	}

public:
	template<typename String>
	explicit PageCache(String &&pager_fname, uint32_t size = 1_MB)
	    : m_limit{static_cast<std::size_t>(size / Page::size())},
	      m_pager{std::forward<String>(pager_fname)} {
		assert(m_limit > 0);
	}

	/*
	 * Acquire a reference to a page loaded inside the buffer pool.
	 * If the desired page is not originally present in the index,
	 * it is added using the put_page() mechanism.
	 */
	Page &get_page(Position page_pos) {
		assert(page_pos.is_set());

		if (!m_index.contains(page_pos)) {
			static auto p = Page();
			put_page(page_pos, std::move(m_pager.fetch(p, page_pos)));
		}

		auto it = m_index.find(page_pos);
		assert(it != m_index.end());

		m_tracker.splice(m_tracker.cend(), m_tracker, it->second.m_cit);
		return it->second.m_page;
	}

	/*
	 * Place *owned* page inside the buffer pool.
	 * If not space is currently available, the
	 * least recently used page is evicted.
	 */
	void put_page(Position page_pos, Page &&page) {
		assert(page_pos.is_set());
		assert(m_index.size() <= m_limit);

		const auto it = m_index.find(page_pos);
		if (it == m_index.cend()) {
			// Page is not present in cache.
			if (full())
				evict();
			m_tracker.push_back(page_pos);
		} else {
			// Page is present.
			// Move it to the end of `m_tracker`.
			m_tracker.splice(m_tracker.cend(), m_tracker, it->second.m_cit);
		}

		m_index[page_pos] = CacheEntry{
		        .m_page = page,
		        .m_cit = m_tracker.cend()};
	}

	void flush_all() {
		while (!empty())
			evict();
	}

	[[nodiscard]] Position get_new_pos() noexcept { return m_pager.alloc(); }

	[[nodiscard]] bool full() const noexcept { return m_index.size() >= m_limit; }

	[[nodiscard]] bool empty() const noexcept { return m_index.empty(); }

	[[nodiscard]] uint32_t static constexpr min_size() noexcept { return Page::size(); }

private:
	//! Maximum number of elements that may be stored in the cache at once
	const std::size_t m_limit;

	//! Position-to-Page mapping
	std::unordered_map<Position, CacheEntry> m_index;

	//! Usage tracker of the entries inside `m_index`
	std::list<Position> m_tracker;

	//!
	Pager m_pager;
};

}// namespace internal::storage
