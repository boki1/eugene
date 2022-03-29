#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <vector>
#include <thread>

#include <catch2/catch.hpp>

#include <core/storage/Pager.h>

using namespace internal::storage;

TEST_CASE("Prepare storage files") {
	/// Dummy test case
	std::ofstream{"/tmp/eu-pager", std::ios::trunc};
	std::ofstream{"/tmp/eu-persistent-pager-stackallocater", std::ios::trunc};
	std::ofstream{"/tmp/eu-pager-persistent-pager-freelistalloc", std::ios::trunc};
	std::ofstream{"/tmp/eu-pager-con", std::ios::trunc};
}

TEST_CASE("Page", "[pager]") {
	Pager pr("/tmp/eu-pager");
	Page p;

	std::fill(p.begin(), p.end(), 42);
	pr.place(0, Page(p));
	REQUIRE(p == pr.get(0));

	Page q;
	std::fill(q.begin(), q.end(), 13);
	pr.place(PAGE_SIZE, Page(q));
	REQUIRE(q == pr.get(PAGE_SIZE));
}

TEST_CASE("Persistent pager", "[pager]") {
	SECTION("Using stack allocator") {
		Pager<StackSpaceAllocator> pr_stack1("/tmp/eu-persistent-pager-stackallocater");

		for (int i = 0; i < 10; ++i)
			[[maybe_unused]] Position p = pr_stack1.alloc();
		REQUIRE(pr_stack1.allocator().cursor() == 10 * PAGE_SIZE);
		pr_stack1.save();

		Pager<StackSpaceAllocator> pr_stack2("/tmp/eu-persistent-pager-stackallocater", ActionOnConstruction::Load);
		REQUIRE(pr_stack2.allocator().cursor() == 10 * PAGE_SIZE);

		Pager<StackSpaceAllocator> pr_stack3("/tmp/eu-persistent-pager-stackallocater", ActionOnConstruction::DoNotLoad);
		REQUIRE(pr_stack3.allocator().cursor() == 0);
		pr_stack3.load();
		REQUIRE(pr_stack3.allocator().cursor() == 10 * PAGE_SIZE);
	}

	SECTION("Using freelist allocator") {
		Pager<FreeListAllocator, LRUCache> pr("/tmp/eu-pager-persistent-pager-freelistalloc", ActionOnConstruction::DoNotLoad, 10ul);
		REQUIRE(pr.allocator().freelist().empty());
		REQUIRE(pr.allocator().next() == 0ul);
		REQUIRE(pr.allocator().limit() == 10ul);

		for (int i = 0; i < 10; ++i)
			REQUIRE(pr.alloc() == i * PAGE_SIZE);
		REQUIRE(pr.allocator().freelist().empty());
		REQUIRE(pr.allocator().next() == 10ul);

		for (int i = 0; i < 10; i += 2)
			REQUIRE_NOTHROW(pr.free(i * PAGE_SIZE));
		REQUIRE(pr.allocator().freelist() == std::vector<Position>{32768, 24576, 16384, 8192, 0});
		pr.save();

		Pager<FreeListAllocator, LRUCache> pr_2("/tmp/eu-pager-persistent-pager-freelistalloc", ActionOnConstruction::Load);
		REQUIRE(pr_2.allocator().freelist() == std::vector<Position>{32768, 24576, 16384, 8192, 0});

		Pager<FreeListAllocator, LRUCache> pr_3("/tmp/eu-pager-persistent-pager-freelistalloc", ActionOnConstruction::DoNotLoad, 10ul);
		REQUIRE(pr_3.allocator().freelist().empty());
		REQUIRE(pr_3.allocator().next() == 0ul);
		REQUIRE(pr_3.allocator().limit() == 10ul);
		pr_3.load();
		REQUIRE(pr_3.allocator().freelist() == std::vector<Position>{32768, 24576, 16384, 8192, 0});
	}
}

TEST_CASE("Page stack allocator", "[pager]") {
	Pager<StackSpaceAllocator> pr("/tmp/eu-pager-stack-alloc");
	for (int i = 0; i < 11; ++i) {
		REQUIRE(pr.alloc() == i * PAGE_SIZE);
		REQUIRE_THROWS_AS(pr.free(0), BadAlloc);
	}
}

TEST_CASE("Page free list", "[pager]") {
	Pager<FreeListAllocator, LRUCache> pr("/tmp/eu-pager-freelist-alloc", ActionOnConstruction::DoNotLoad, 10ul);
	REQUIRE(pr.allocator().freelist().empty());

	for (int i = 0; i < 10; ++i)
		REQUIRE(pr.alloc() == i * PAGE_SIZE);
	REQUIRE(pr.allocator().freelist().empty());

	for (int i = 0; i < 10; i += 2) {
		REQUIRE_NOTHROW(pr.free(i * PAGE_SIZE));
	}

	REQUIRE(pr.allocator().freelist() == std::vector<Position>{32768, 24576, 16384, 8192, 0});
}

TEST_CASE("Page cache with LRU policy", "[pager]") {
	PageCache<LRUCache> cache(4);
	Page p;

	for (int i = 0; i < 4; ++i) {
		std::fill(p.begin(), p.end(), i);
		REQUIRE(!cache.place(i * PAGE_SIZE, Page(p)));
		REQUIRE(p == cache.get(i * PAGE_SIZE)->get());
	}

	REQUIRE(!cache.get(42).has_value());

	std::fill(p.begin(), p.end(), 42);
	auto evict_res1 = cache.place(4 * PAGE_SIZE, Page(p));
	REQUIRE(evict_res1.has_value());
	REQUIRE(cache.get(4 * PAGE_SIZE)->get() == p);
	REQUIRE(std::find(evict_res1->page.cbegin(), evict_res1->page.cend(), 0) != evict_res1->page.cend());
	REQUIRE(evict_res1->pos == 0 * PAGE_SIZE);

	std::fill(p.begin(), p.end(), 13);
	auto evict_res2 = cache.place(5 * PAGE_SIZE, Page(p));
	REQUIRE(evict_res2.has_value());
	REQUIRE(cache.get(5 * PAGE_SIZE)->get() == p);
	REQUIRE(std::find(evict_res2->page.cbegin(), evict_res2->page.cend(), 1) != evict_res2->page.cend());
	REQUIRE(evict_res2->pos == 1 * PAGE_SIZE);
}

TEST_CASE("Pager inner operations") {
	using PagerType = Pager<FreeListAllocator, LRUCache>;
	SECTION("Allocation and deallocation") {
		PagerType pt{"/tmp/eu-pager-inner-allocations"};

		auto pos10 = pt.alloc_inner(10);
		REQUIRE(pos10 == PAGE_HEADER_SIZE);
		REQUIRE(pt.max_bytes_inner_used() == 12);

		auto pos20 = pt.alloc_inner(20);
		REQUIRE(pos20 == PAGE_HEADER_SIZE + 12);
		REQUIRE(pt.max_bytes_inner_used() == 32);

		auto pos5000 = pt.alloc_inner(5000);
		REQUIRE(pos5000 == PAGE_HEADER_SIZE + 32);
		// REQUIRE(pt.max_bytes_inner_used() == 5032);

		pt.free_inner(pos10, 10);
		// REQUIRE(pt.max_bytes_inner_used() == 5020);

		auto pos10_2 = pt.alloc_inner(10);
		REQUIRE(pos10 == pos10_2);
		pt.free_inner(pos5000, 5000);

		// REQUIRE(pt.max_bytes_inner_used() == 32);
	}

	SECTION("Enplacing and retrieving of data") {
		PagerType pt{"/tmp/eu-pager-inner-place-and-get"};

		auto pos10 = pt.alloc_inner(10);
		std::vector<uint8_t> expected10(10, 10);
		pt.place_inner(pos10, expected10);
		auto actual10 = pt.get_inner(pos10, 10);
		REQUIRE(std::equal(expected10.cbegin(), expected10.cend(), actual10.cbegin()));

		auto pos20 = pt.alloc_inner(20);
		std::vector<uint8_t> expected20(20, 20);
		pt.place_inner(pos20, expected20);
		auto actual20 = pt.get_inner(pos20, 20);
		fmt::print("expected20 = '{}'\n", fmt::join(expected20, "; "));
		fmt::print("actual20   = '{}'\n", fmt::join(actual20, "; "));
		REQUIRE(std::equal(expected20.cbegin(), expected20.cend(), actual20.cbegin()));

		auto pos5000 = pt.alloc_inner(5000);
		std::vector<uint8_t> expected5000(5000, 50);
		pt.place_inner(pos5000, expected5000);
		auto actual5000 = pt.get_inner(pos5000, 5000);
		REQUIRE(std::equal(expected5000.cbegin(), expected5000.cend(), actual5000.cbegin()));

		pt.free_inner(pos10, 10);
		auto pos10_2 = pt.alloc_inner(10);
		std::vector<uint8_t> expected10_2(10, 11);
		pt.place_inner(pos10_2, expected10_2);
		auto actual10_2 = pt.get_inner(pos10_2, 10);
		REQUIRE(std::equal(expected10_2.cbegin(), expected10_2.cend(), actual10_2.cbegin()));
		REQUIRE(pos10_2 == pos10);

		std::vector<uint8_t> expected2222(2222, 22);
		pt.place_inner(pos5000, expected2222);
		auto actual2222 = pt.get_inner(pos5000, 2222);
		REQUIRE(std::equal(expected2222.cbegin(), expected2222.cend(), actual2222.cbegin()));
	}
}

TEST_CASE("Pager concurrency") {
	std::array<std::thread, 10> threads;
	using PagerType = Pager<FreeListAllocator, LRUCache>;

	PagerType pr("/tmp/eu-pager-con");
	auto oper = [&] {
		Page p;
		std::fill(p.begin(), p.end(), 42);
		pr.place(0, Page(p));
		REQUIRE(p == pr.get(0));
		Page q;
		std::fill(q.begin(), q.end(), 13);
		pr.place(PAGE_SIZE, Page(q));
		REQUIRE(q == pr.get(PAGE_SIZE));

		[[maybe_unused]] auto pos10 = pr.alloc_inner(10);
		std::vector<uint8_t> expected10(10, 10);
		pr.place_inner(pos10, expected10);
		auto actual10 = pr.get_inner(pos10, 10);
		REQUIRE(std::equal(expected10.cbegin(), expected10.cend(), actual10.cbegin()));

		auto pos20 = pr.alloc_inner(20);
		std::vector<uint8_t> expected20(20, 20);
		pr.place_inner(pos20, expected20);
		auto actual20 = pr.get_inner(pos20, 20);
		REQUIRE(std::equal(expected20.cbegin(), expected20.cend(), actual20.cbegin()));
		fmt::print("thread end\n");
	};

	for (auto &t : threads)
		t = std::thread(oper);

	for (auto &t : threads)
		t.join();
}
