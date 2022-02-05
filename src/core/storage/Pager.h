#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <list>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <deque>

#include <cppcoro/generator.hpp>

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/types/variant.h>
#include <nop/base/vector.h>
#include <nop/utility/die.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>

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

/// Marks whether any additional action should be performed when constructing a Pager instance
enum class ActionOnConstruction : uint8_t {
	Load,
	DoNotLoad
};

//!
//! Exceptions thrown in the pager module
//!

/// Bad allocation
/// This could be the result of 1) incorrectly executed allocation algorithm, 2) calling alloc/free on allocators which
/// do not support the specific operation.
struct BadAlloc : std::runtime_error {
	explicit BadAlloc() : std::runtime_error{"Eugene: Bad allocation"} {}
};

/// Bad position
/// This error implies that the position passed as an argument to one of the methods was _invalid_. The vailidity of a
/// position is specific to the execution context, but it general it signals that the position is out the operating
/// range of the pager/cache/allocator or it does not point to a page boundary.
struct BadPosition : std::runtime_error {
	explicit BadPosition(const Position pos) : std::runtime_error{fmt::format("Eugene: Bad position {:#04x}", pos)} {}
};

/// Bad read
/// The read operation failed.
struct BadRead : std::runtime_error {
	explicit BadRead() : std::runtime_error{"Eugene: Bad read"} {}
};

/// Bad write
/// The write operation failed.
struct BadWrite : std::runtime_error {
	explicit BadWrite() : std::runtime_error{"Eugene: Bad write"} {}
};

/// Stack-based allocator
/// The operating range grows based on a cursor position which points to the next free page.
/// This allocator does not support freeing of pages. It is perfect for a tree in which only insertions and lookups will
/// be performed, since it does not come with any overhead.
/// Allocation is Θ(1). Freeing is not supported. Checking whether a page has been allocated is Θ(1).
class StackSpaceAllocator {
	Position m_cursor = 0;
	NOP_STRUCTURE(StackSpaceAllocator, m_cursor);

public:
	[[nodiscard]] constexpr Position alloc() {
		auto tmp = m_cursor;
		m_cursor += PAGE_SIZE;
		return tmp;
	}

	void free(const Position) {
		throw BadAlloc{};
	}

	/// If the page is below the cursor, it is considered as allocated since there is no "official" way of freeing
	/// pages using this allocator.
	/// Note: The implementation does track whether the pos is actually identifing a valid page.
	constexpr bool has_allocated(const Position pos) const noexcept {
		return pos < m_cursor;
	}

	[[nodiscard]] constexpr Position cursor() const noexcept { return m_cursor; }
};

/// Free list allocator
/// Contains a list filled with the positions which are currently available.
/// Allocation in performed in Θ(1) - just peek at the first entry and remove it.
/// Freeing in Θ(n), where n is the number of items currently stored in the freelist. This is due to the fact that all
/// positions are stored sorted.
/// Checking whether a page has been allocated is Θ(n).
/// The drawbacks of this method is the space it uses.
///
/// TODO: Optimize space usage. Maybe something like a range selector: From 0x0 to 0x100000 is free, rather than keeping
/// every page in this range.
class FreeListAllocator {
    std::vector<Position> m_freelist;
	NOP_STRUCTURE(FreeListAllocator, m_freelist);

public:
    explicit FreeListAllocator(std::size_t num_pages = 256) {
        std::generate_n(std::back_inserter(m_freelist), num_pages, [pg = num_pages - 1]() mutable { return pg-- * PAGE_SIZE; });
    }

    [[nodiscard]] Position alloc() {
        const Position pos = m_freelist.back();
        m_freelist.pop_back();
        return pos;
    }

    void free(const Position pos) {
        if (pos % PAGE_SIZE != 0)
            throw BadPosition(pos);
		const auto it = std::find_if(m_freelist.begin(), m_freelist.end(), [&](const Position curr) {
			return curr <= pos;
		});
        if (*it == pos && it < m_freelist.end())
            throw BadPosition(pos);
        m_freelist.insert(m_freelist.begin() + std::distance(m_freelist.begin(), it), pos);
    }

    [[nodiscard]] const auto &freelist() const noexcept { return m_freelist; }
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

template<typename AllocatorPolicy = StackSpaceAllocator,
         typename CacheEvictionPolicy = LRUCache>
class Pager final {
	friend AllocatorPolicy;

	[[nodiscard]] static constexpr bool at_page_boundary(const Position pos) { return pos % PAGE_SIZE == 0; }

public:
	[[nodiscard]] std::size_t size() noexcept { return std::filesystem::file_size(m_identifier); }

	[[nodiscard]] const AllocatorPolicy &allocator() const noexcept { return m_allocator; }

private:
	/// Read a page off a given position
	/// Position is expected to be 1) associated with a page and, 2) the page which is allocated.
	/// If that is not adhered to, BadRead is thrown.
	[[nodiscard]] Page read(Position pos) {
		if (!at_page_boundary(pos) || !m_allocator.has_allocated(pos))
			throw BadRead{};

		Page page;
		m_disk.seekp(pos);
		m_disk.read(reinterpret_cast<char *>(&page), PAGE_SIZE);
		return page;
	}

	/// Write a page to disk
	/// Optionally, the position that the page should be put at is given. If no such is provided, a new one is
	/// allocated. If, however, one is provided _but_ it is invalid (i.e not associated with a page boundary), a
	/// BadWrite is thrown.
	void write(const Page &page, Position pos) {
		if (!at_page_boundary(pos))
			throw BadWrite{};

		m_disk.seekp(pos);
		m_disk.write(reinterpret_cast<const char *>(page.data()), PAGE_SIZE);
	}

	/// The standard does not allow to call a member function as a default parameter value, thus the `write` function
	/// is overloaded rather than using default parameter values.
	void write(const Page &page) {
		return write(page, alloc());
	}

public:
	/// The Pager class conforms to the Rule of Three

	template <typename ...Args>
	explicit Pager(std::string_view identifier, const ActionOnConstruction action = ActionOnConstruction::DoNotLoad, Args&& ...args)
	    : m_allocator{std::forward<Args>(args)...},
		  m_identifier{identifier},
	      m_disk{identifier.data()} {
		if (action == ActionOnConstruction::Load)
			load();
	}

	constexpr Pager(const Pager &pager) = default;

	constexpr auto operator<=>(const Pager &) const noexcept = default;

	/// Allocate a new page and return its position.
	/// The AllocatorPolicy of the class is used as an algorithm.
	[[nodiscard]] Position alloc() {
		return m_allocator.alloc();
	}

	/// Free a page identified by its position.
	/// The AllocatorPolicy of the class is used as an algorithm.
	/// Note: Some allocators do not implement a `free` method are require that its definition is not called. BadAlloc
	/// is thrown otherwise.
	void free(const Position pos) {
		return m_allocator.free(pos);
	}

	/// Acquire the page, placed at a given position.
	/// May fail if either `read` or `write` throw an error.
	[[nodiscard]] Page get(Position pos) {
		if (auto p = m_cache.get(pos); p)
			return p->get();
		Page p = read(pos);
		if (auto evict_res = m_cache.place(pos, Page(p)); evict_res)
			write(evict_res->page, evict_res->pos);
		return p;
	}

	/// Placed a page at a given position.
	/// May fail if `write` throws an error.
	void place(Position pos, Page &&page) {
		if (auto evict_res = m_cache.place(pos, std::move(page)); evict_res)
			write(evict_res->page, evict_res->pos);
	}

	void save() {
		// Store allocator state
		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{fmt::format("{}-alloc", m_identifier.data()), std::ios::trunc | std::ios::out};
		if (!serializer.Write(m_allocator))
			throw BadWrite();

		// Flush cache
		for (auto evict_res : m_cache.flush())
			if (evict_res)
				write(evict_res->page, evict_res->pos);
	}

	void load() {
		// Load allocator state
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{fmt::format("{}-alloc", m_identifier.data())};
		if (!deserializer.Read(&m_allocator))
			throw BadRead();
	}

private:
	AllocatorPolicy m_allocator;
	PageCache<CacheEvictionPolicy> m_cache;
	std::string_view m_identifier;
	std::fstream m_disk;
};

}// namespace internal::storage
