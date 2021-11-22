#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include <third-party/expected/Expected.h>

#include <core/storage/Page.h>
#include <core/storage/Position.h>

static constexpr uint32_t operator""_MB(const unsigned long long x) {
	return x * 1 << 20;
}

namespace internal::storage {

//! Buffer pool structure.
//! Implemented as a write-behind LRU cache.
//! Controls access to pages using an internal pager.
class PageCache {
	using PosPage = std::pair<Position, Page>;

private:
	void evict_lru() {
		// This cache implementation is write-behind so this is the stage at which
		// dirty pages are synced with the disc
		auto &[pos, page] = m_pool.back();
		if (page.dirty())
			m_pager.sync(page, pos);

		m_index.erase(pos);
		m_pool.pop_back();
	}

public:
	template<typename String>
	explicit PageCache(String &&pager_fname, uint32_t size = 1_MB)
	    : m_size{size},
	      m_limit{size / sizeof(PosPage)},
	      m_pager{std::forward<String>(pager_fname)} {
		assert(size >= sizeof(PosPage));
		assert(m_limit > 0);
	}

	/*
	 * Acquire a reference to a page loaded inside the buffer pool.
	 * If the desired page is not originally present in the index,
	 * it is added using the put_page() mechanism.
	 */
	Page &get_page(Position page_pos) {
		auto it = m_index.find(page_pos);
		if (it == m_index.end()) {
			Page tmp_clone = m_pager.fetch(m_tmp, page_pos);
			put_page(page_pos, std::move(tmp_clone));
			it = m_index.find(page_pos);
			assert(it != m_index.end());
		}
		m_pool.splice(m_pool.begin(), m_pool, it->second);
		return it->second->second;
	}

	/*
	 * Place *owned* page inside the buffer pool.
	 * If not space is currently available, the
	 * least recently used page is evicted.
	 */
	void put_page(Position page_pos, Page &&page) {
		if (auto it = m_index.find(page_pos); it != m_index.end()) {
			m_index[page_pos]->second = std::move(page);
			return;
		}

		if (full())
			evict_lru();

		// From this point on the cache *owns* this page
		m_pool.emplace_front(page_pos, std::move(page));
		m_index.emplace(page_pos, m_pool.begin());
	}

	void flush_all() {
		while (!empty())
			evict_lru();
	}

	[[nodiscard]] Position get_new_pos() noexcept { return m_pager.alloc(); }

	[[nodiscard]] bool full() const noexcept { return m_index.size() == m_limit; }

	[[nodiscard]] bool empty() const noexcept { return m_index.size() == 0; }

	[[nodiscard]] uint32_t static constexpr min_size() noexcept { return sizeof(PosPage); }

private:
	const std::size_t m_size;
	const std::size_t m_limit;

	std::list<PosPage> m_pool;
	std::unordered_map<Position, decltype(m_pool)::iterator> m_index;

	Page m_tmp{{0}};

	Pager m_pager;
};

}// namespace internal::storage
