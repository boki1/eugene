#pragma once

#include <array>
#include <bit>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <list>
#include <optional>
#include <string_view>
#include <unordered_map>
namespace fs = std::filesystem;

#include <cppcoro/generator.hpp>

#include <nop/base/vector.h>
#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/types/variant.h>
#include <nop/utility/die.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>

#include <core/Util.h>

namespace internal::storage {

constexpr static std::size_t PAGE_SIZE = 4_KB;
/// Each 4B have a bit denoting whether they are used or not.
constexpr static std::size_t PAGE_ALLOC_SCALE = 4_B;
/// Num B used for the header.
/// 1B for type: Slot or Node.
enum class PageType : uint8_t { Node,
	                        Slots };

/// Start of allocation metadata
constexpr static std::size_t PAGE_ALLOC_METADATA = sizeof(PageType);
/// The rest is for allocation information.
constexpr static std::size_t PAGE_HEADER_SIZE = PAGE_ALLOC_METADATA + PAGE_SIZE / PAGE_ALLOC_SCALE / CHAR_BIT;

constexpr static std::size_t PAGECACHE_SIZE = 1_MB;

constexpr static std::size_t PAGECACHE_SIZE_UNLIMITED = 0;

constexpr static std::size_t DEFAULT_NUM_PAGES = 256;

using Position = unsigned long int;

/// Page layout
using Page = std::array<std::uint8_t, PAGE_SIZE>;

constexpr Page SlotPage() {
	Page p;
	p[0] = static_cast<uint8_t>(PageType::Slots);
	return p;
}

constexpr Page NodePage() {
	Page p;
	p[0] = static_cast<uint8_t>(PageType::Node);
	return p;
}

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
	explicit BadAlloc(std::string msg = "") : std::runtime_error{fmt::format("Eugene: Bad allocation - {}", msg)} {}
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
	explicit BadRead(std::string msg = "") : std::runtime_error{fmt::format("Eugene: Bad read - {}", msg)} {}
};

/// Bad write
/// The write operation failed.
struct BadWrite : std::runtime_error {
	explicit BadWrite(std::string msg = "") : std::runtime_error{fmt::format("Eugene: Bad write - {}", msg)} {}
};

/// Stack-based allocator
/// The operating range grows based on a cursor position which points to the next free page.
/// This allocator does not support freeing of pages. It is perfect for a tree in which only insertions and lookups will
/// be performed, since it does not come with any overhead.
/// Allocation is Θ(1). Freeing is not supported. Checking whether a page has been allocated is Θ(1).
class StackSpaceAllocator {
	Position m_cursor = 0;
	const std::size_t m_limit_num_pages;
	NOP_STRUCTURE(StackSpaceAllocator, m_cursor);

public:
	explicit StackSpaceAllocator(const std::size_t limit_num_pages = DEFAULT_NUM_PAGES) : m_limit_num_pages{limit_num_pages} {}
	StackSpaceAllocator(const StackSpaceAllocator &) = default;
	StackSpaceAllocator &operator=(const StackSpaceAllocator &);

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
	[[nodiscard]] constexpr bool has_allocated(const Position pos) const noexcept {
		return pos < m_cursor;
	}

	[[nodiscard]] cppcoro::generator<Position> next_allocated_page() const noexcept {
		for (auto i = 0ul; i < m_cursor; i += PAGE_SIZE)
			co_yield i;
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
class FreeListAllocator {
	std::vector<Position> m_freelist;
	std::size_t m_next_page{0};
	std::size_t m_limit_num_pages;
	NOP_STRUCTURE(FreeListAllocator, m_freelist, m_next_page, m_limit_num_pages);

public:
	explicit FreeListAllocator(std::size_t limit_num_pages = DEFAULT_NUM_PAGES) : m_limit_num_pages{limit_num_pages} {}

	FreeListAllocator(const FreeListAllocator &) = default;
	FreeListAllocator &operator=(const FreeListAllocator &) = default;

	/// Allocate space using the freelist allocator
	/// Tries to release the first accessible page, using the freelist.
	/// If it is empty increments cursor allocator until the end is reached.
	[[nodiscard]] Position alloc() {
		if (!m_freelist.empty()) {
			const Position pos = m_freelist.back();
			m_freelist.pop_back();
			return pos;
		}

		if (m_next_page >= m_limit_num_pages)
			throw BadAlloc(fmt::format(" - FreeListAllocator out of space. Already returned limit amount of pages (={})\n", m_limit_num_pages));

		return m_next_page++ * PAGE_SIZE;
	}

	void free(const Position pos) {
		if (pos % PAGE_SIZE != 0)
			throw BadPosition(pos);

		if (pos / PAGE_SIZE == m_next_page - 1) {
			--m_next_page;
			return;
		}

		const auto it = std::find_if(m_freelist.begin(), m_freelist.end(), [&](const Position curr) {
			return curr <= pos;
		});
		if (it < m_freelist.end() && *it == pos)
			throw BadPosition(pos);
		m_freelist.insert(m_freelist.begin() + std::distance(m_freelist.begin(), it), pos);
	}

	[[nodiscard]] constexpr bool has_allocated(const Position pos) const {
		if (collection_contains(m_freelist, pos))
			return false;
		if (pos >= m_next_page * PAGE_SIZE)
			return false;
		return true;
	}

	[[nodiscard]] cppcoro::generator<Position> next_allocated_page() const noexcept {
		fmt::print("pager alloc: next_page @{}\n", m_next_page);
		for (Position i = 0; i < m_next_page * PAGE_SIZE; i += PAGE_SIZE) {
			if (has_allocated(i)) {
				co_yield i;
			}
		}
	}

	[[nodiscard]] const auto &freelist() const noexcept { return m_freelist; }
	[[nodiscard]] const auto next() const noexcept { return m_next_page; }
	[[nodiscard]] const auto limit() const noexcept { return m_limit_num_pages; }
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
	constexpr explicit PageCache(std::size_t limit = PAGECACHE_SIZE / PAGE_SIZE)
	    : m_limit{limit > 0 ? limit : std::numeric_limits<decltype(m_limit)>::max()} {}

	[[nodiscard]] optional_ref<Page> get(Position pos) {
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
		}

		m_index[pos] = CacheEntry{
		        .m_page = page,
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

/// Evict the least-recently used page from cache
/// If it has been modified, store it first.
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

/// Cache which never evicts. Used for the InMemoryPager since the pages
/// are supposed to be contained entirely in memory and the PageCache is
/// used for this purpose.
struct NeverEvictCache {
	[[nodiscard]] static CacheEvictionResult evict(PageCache<NeverEvictCache> &) {
		DO_NOTHING
		return {};
	}
};

template<typename AllocatorPolicy = FreeListAllocator,
         typename CacheEvictionPolicy = LRUCache>
class GenericPager {
	friend AllocatorPolicy;

public:
	template<typename... AllocatorArgs>
	constexpr explicit GenericPager(std::size_t limit_num_pages = PAGECACHE_SIZE / PAGE_SIZE, AllocatorArgs &&...allocator_args) : m_allocator{limit_num_pages, std::forward<AllocatorArgs>(allocator_args)...},
	                                                                                                                               m_cache{limit_num_pages} {}
	GenericPager(const GenericPager &) = default;

	GenericPager &operator=(const GenericPager &) = default;

	virtual ~GenericPager() noexcept = default;

	auto operator<=>(const GenericPager &) const noexcept = default;

	///
	/// Allocation API
	///

	[[nodiscard]] virtual Position alloc() = 0;

	virtual void free(Position pos) = 0;

	///
	/// Page operations API
	///

	[[nodiscard]] virtual Page get(const Position pos) = 0;

	virtual void place(const Position pos, Page &&page) = 0;

	///
	/// Properties
	///

	[[nodiscard]] virtual constexpr const AllocatorPolicy &allocator() const noexcept { return m_allocator; }

	[[nodiscard]] virtual constexpr const PageCache<CacheEvictionPolicy> &cache() const noexcept { return m_cache; }

protected:
	AllocatorPolicy m_allocator;
	PageCache<CacheEvictionPolicy> m_cache;
};

///
/// Persistent pager interface
class IPersistentPager {

public:
	IPersistentPager() = default;
	IPersistentPager(const IPersistentPager &) = default;
	IPersistentPager &operator=(const IPersistentPager &) = default;

	virtual void save() = 0;
	virtual void load() = 0;
};

///
/// Allows inner allocations
///
class ISupportingInnerOperations {
public:
	ISupportingInnerOperations() = default;
	ISupportingInnerOperations(const ISupportingInnerOperations &) = default;
	ISupportingInnerOperations &operator=(const ISupportingInnerOperations &) = default;

	virtual Position alloc_inner(std::size_t) = 0;
	virtual void free_inner(Position, std::size_t) = 0;
	virtual std::vector<uint8_t> get_inner(Position, std::size_t) = 0;
	virtual void place_inner(Position, const std::vector<uint8_t> &) = 0;
	virtual std::size_t max_bytes_inner_used() noexcept = 0;
};

template<typename AllocatorPolicy = FreeListAllocator,
         typename CacheEvictionPolicy = LRUCache>
class Pager : public GenericPager<AllocatorPolicy, CacheEvictionPolicy>,
              public IPersistentPager,
              public ISupportingInnerOperations {
	using Super = GenericPager<AllocatorPolicy, CacheEvictionPolicy>;

	[[nodiscard]] static constexpr bool at_page_boundary(const Position pos) { return pos % PAGE_SIZE == 0; }

public:
	/// The Pager class conforms to the Rule of Three

	template<typename... Args>
	explicit Pager(std::string_view identifier, const ActionOnConstruction action = ActionOnConstruction::DoNotLoad, std::size_t limit_page_cache_size = PAGECACHE_SIZE / PAGE_SIZE, Args &&...args)
	    : Super{limit_page_cache_size, std::forward<Args>(args)...}, m_identifier{identifier} {
		if (action == ActionOnConstruction::Load)
			load();
		else if (!fs::exists(identifier.data()))
			/// Create an empty storage iff we should not load and the storage does not yet exist.
			m_disk.open(identifier.data(), std::ios::trunc | std::ios::in | std::ios::out);
		else
			m_disk.open(identifier.data(), std::ios::in | std::ios::out);
	}

	virtual ~Pager() noexcept {
		/// Close storage manually since it is not using RAII
		m_disk.close();
	}

	Pager(const Pager &p) : Super(p) {}
	Pager &operator=(const Pager &) = default;

	auto operator<=>(const Pager &) const noexcept = default;

	///
	/// Allocation API
	///

	/// Allocate a new page and return its position.
	/// The AllocatorPolicy of the class is used as an algorithm.
	[[nodiscard]] Position alloc() override {
		return this->m_allocator.alloc();
	}

	/// Free a page identified by its position.
	/// The AllocatorPolicy of the class is used as an algorithm.
	/// Note: Some allocators do not implement a `free` method are require that its definition is not called. BadAlloc
	/// is thrown otherwise.
	void free(const Position pos) override {
		return this->m_allocator.free(pos);
	}

	///
	/// Page operations
	///

	/// Read a page off a given position
	/// Position is expected to be 1) associated with a page and, 2) the page which is allocated.
	/// If that is not adhered to, BadRead is thrown.
	[[nodiscard]] Page read(Position pos) {
		if (!at_page_boundary(pos) || !this->m_allocator.has_allocated(pos))
			throw BadRead("pos is either not associated with a page or is not allocated");

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

	/// Acquire the page, placed at a given position.
	/// May fail if either `read` or `write` throw an error.
	[[nodiscard]] Page get(Position pos) override {
		if (auto p = this->m_cache.get(pos); p)
			return p->get();
		Page p = read(pos);
		if (auto evict_res = this->m_cache.place(pos, Page(p)); evict_res)
			write(evict_res->page, evict_res->pos);
		return p;
	}

	/// Placed a page at a given position.
	/// May fail if `write` throws an error.
	void place(Position pos, Page &&page) override {
		if (auto evict_res = this->m_cache.place(pos, Page(page)); evict_res)
			write(evict_res->page, evict_res->pos);
	}

	///
	/// Persistence API
	///

	void save() override {
		// Store allocator state
		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{fmt::format("{}-alloc", m_identifier.data()), std::ios::trunc | std::ios::out};
		if (!serializer.Write(this->m_allocator))
			throw BadWrite("- serializer failed writing pager allocator");

		// Flush cache
		for (auto evict_res : this->m_cache.flush())
			if (evict_res)
				write(evict_res->page, evict_res->pos);
	}

	void load() override {
		// Load allocator state
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{fmt::format("{}-alloc", m_identifier.data())};
		if (!deserializer.Read(&this->m_allocator))
			throw BadRead("- deserializer failed reading pager allocator");
	}

	///
	/// Inner operations
	///

private:
	[[nodiscard]] constexpr Position chunk_to_position(Position page_pos, unsigned chunk_num) {
		return PAGE_HEADER_SIZE + page_pos + chunk_num * PAGE_ALLOC_SCALE;
	}

	[[nodiscard]] constexpr unsigned position_to_chunk(Position pos_in_page) {
		return (pos_in_page - PAGE_HEADER_SIZE) % PAGE_SIZE / PAGE_ALLOC_SCALE;
	}

	constexpr void chunkbit(Page &p, unsigned chunk_num, bool flag) {
		uint8_t &byte = p[PAGE_ALLOC_METADATA + chunk_num / CHAR_BIT];
		uint8_t mask = 1 << (chunk_num % CHAR_BIT);
		if (flag)
			byte |= mask;
		else
			byte &= ~mask;
	}

	cppcoro::generator<std::pair<unsigned, bool>> chunkbit_iter(const Page &p) {
		auto chunk_num = 0;
		for (auto i = PAGE_ALLOC_METADATA; i < PAGE_HEADER_SIZE; ++i, ++chunk_num) {
			const auto val = p.at(i);
			for (auto bit_num = 0; bit_num < CHAR_BIT; ++bit_num)
				co_yield std::make_pair(chunk_num * CHAR_BIT + bit_num, val & (1 << bit_num));
		}
	}

public:
	std::size_t max_bytes_inner_used() noexcept override {
		std::size_t chunks = 0;
		for (Position page_pos : this->m_allocator.next_allocated_page()) {
			auto page = get(page_pos);
			fmt::print("Inspecting page @{}\n", page_pos);
			for (auto i = PAGE_ALLOC_METADATA; i < PAGE_HEADER_SIZE; ++i) {
				chunks += std::popcount(page.at(i));
				if (auto popc = std::popcount(page.at(i)); popc > 0)
					fmt::print("    has {} used chunkbits\n", popc);
			}
		}
		return chunks * PAGE_ALLOC_SCALE;
	}

	/// Allocate 'sz' number of bytes space inside consecutive pages
	/// Uses the allocation metadata in the header of the page.
	/// Utilizes linear probing during search.
	/// If a valid sz value is passed, then the allocation always succeeds.
	Position alloc_inner(std::size_t sz) override {
		if (sz == 0)
			throw BadAlloc("cannot alloc_inner with size 0");

		const auto target_chunks = round_upwards(sz, PAGE_ALLOC_SCALE);
		auto curr_chunks = 0ul;
		std::map<Position, Page> marked_pages;
		std::optional<Position> prev_page_pos;
		auto start_pos = 0ul;

		auto reset = [&] {
			curr_chunks = 0;
			marked_pages.clear();
			// fmt::print("resetting\n");
		};

		auto alloc_in_page = [&](Page &page, const Position page_pos) {
			for (auto [chunk_num, bitval] : chunkbit_iter(page)) {
				if (bitval) {
					reset();
					continue;
				}
				fmt::print("match\n");
				if (curr_chunks == 0)
					start_pos = chunk_to_position(page_pos, chunk_num);
				if (++curr_chunks >= target_chunks)
					break;
			}

			if (curr_chunks > 0) {
				marked_pages.emplace(page_pos, std::move(page));
				fmt::print("emplace page @{} with start_pos = {}\n", page_pos, start_pos);
			}
		};

		/// Try to fill in a page that has already been started but is not yet full.
		for (Position page_pos : this->m_allocator.next_allocated_page()) {
			auto page = get(page_pos);
			if (page.front() != static_cast<uint8_t>(PageType::Slots)) {
				reset();
				continue;
			}
			if (prev_page_pos && prev_page_pos.value() != page_pos - PAGE_SIZE)
				reset();

			alloc_in_page(page, page_pos);
			if (curr_chunks >= target_chunks)
				break;
		}

		/// If that was not enough, allocate new pages to use.
		while (curr_chunks < target_chunks) {
			auto new_page = SlotPage();
			auto new_page_pos = this->m_allocator.alloc();
			alloc_in_page(new_page, new_page_pos);
		}

		/// We should be fine now, just assure that.
		assert(curr_chunks == target_chunks);

		/// Mark used pages and save.
		for (auto &[mppos, mp] : marked_pages) {
			for (auto [chunk_num, _] : chunkbit_iter(mp)) {
				fmt::print("chunk #{}\n", chunk_num);
				auto chpos = chunk_to_position(mppos, chunk_num);
				if (chpos >= start_pos && chpos < start_pos + sz)
					chunkbit(mp, chunk_num, true);
			}
			place(mppos, std::move(mp));
		}

		return start_pos;
	}

	void free_inner(Position pos, std::size_t sz) override {
		// The position of the page whose metadata we are currently modifying
		auto pgpos = (pos / PAGE_SIZE) * PAGE_SIZE;
		auto target_chunks = round_upwards(sz, PAGE_ALLOC_SCALE);
		fmt::print("freeing {} chunks\n", target_chunks);

		while (target_chunks > 0) {
			auto pg = get(pgpos);
			for (auto [chunk_num, _] : chunkbit_iter(pg)) {
				auto chpos = chunk_to_position(pgpos, chunk_num);
				fmt::print("at chunk #{} (@{})\n", chunk_num, chpos);
				if (chpos < pos)
					continue;
				chunkbit(pg, chunk_num, false);
				if (--target_chunks <= 0)
					break;
			}
			place(pgpos, std::move(pg));
			pgpos += PAGE_SIZE;// Go to next page
		}
	}

	std::vector<uint8_t> get_inner(Position pos, std::size_t sz) override {
		if (!this->m_allocator.has_allocated(pos))
			throw BadRead("cannot inner read from unallocated page");
		auto page = get(pos);
		assert(page.front() == static_cast<uint8_t>(PageType::Slots));
		auto start_pos = pos % PAGE_SIZE;
		if (start_pos + sz > PAGE_SIZE)
			throw BadRead("trying to inner read out of page bounds");

		std::vector<uint8_t> data;
		data.reserve(sz);

		std::copy(page.cbegin() + start_pos, page.cbegin() + start_pos + sz, std::back_inserter(data));
		return data;
	}

	void place_inner(Position pos, const std::vector<uint8_t> &bytes_to_store) override {
		if (!this->m_allocator.has_allocated(pos))
			throw BadRead("cannot inner read from unallocated page");
		auto page = get(pos);
		assert(page.front() == static_cast<uint8_t>(PageType::Slots));
		auto start_pos = pos % PAGE_SIZE;
		if (start_pos + bytes_to_store.size() > PAGE_SIZE)
			throw BadWrite("trying to inner write out of page bounds");

		std::copy_n(bytes_to_store.cbegin(), bytes_to_store.size(), page.begin() + start_pos);
		place(pos, std::move(page));
	}

	[[nodiscard]] std::string_view identifier() const noexcept { return m_identifier; }

private:
	std::string_view m_identifier;
	std::fstream m_disk;
};

template<typename AllocatorPolicy = FreeListAllocator>
class InMemoryPager : protected GenericPager<AllocatorPolicy, NeverEvictCache> {
	using Super = GenericPager<AllocatorPolicy, NeverEvictCache>;

public:
	/// The InMemoryPager class conforms to the Rule of Three

	template<typename... Args>
	explicit InMemoryPager(std::string_view identifier, std::size_t limit_page_cache_size = PAGECACHE_SIZE_UNLIMITED) : Super{limit_page_cache_size}, m_identifier{identifier} {}

	~InMemoryPager() noexcept override = default;

	constexpr InMemoryPager(const InMemoryPager &pager) = default;

	constexpr auto operator<=>(const InMemoryPager &) const noexcept = default;

	///
	/// Allocation API
	///

	/// Allocate a new page and return its position.
	/// The AllocatorPolicy of the class is used as an algorithm.
	[[nodiscard]] Position alloc() override {
		return this->m_allocator.alloc();
	}

	/// Free a page identified by its position.
	/// The AllocatorPolicy of the class is used as an algorithm.
	/// Note: Some allocators do not implement a `free` method are require that its definition is not called. BadAlloc
	/// is thrown otherwise.
	void free(const Position) override {
		DO_NOTHING
	}

	/// Acquire the page, placed at a given position.
	/// May fail if either `read` or `write` throw an error.
	[[nodiscard]] Page get(Position pos) override {
		auto p = this->m_cache.get(pos);

		// InMemoryPager stores all pages in the cache.
		assert(p.has_value());
		return *p;
	}

	/// Placed a page at a given position.
	/// May fail if `write` throws an error.
	void place(Position pos, Page &&page) override {
		if (auto evict_res = this->m_cache.place(pos, page); evict_res)
			UNREACHABLE
	}

public:
	[[nodiscard]] std::string_view identifier() const noexcept { return m_identifier; }

private:
	std::string_view m_identifier;
};

}// namespace internal::storage
