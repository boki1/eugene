#include <algorithm>
#include <fstream>
#include <array>
#include <filesystem>

#include <catch2/catch.hpp>

#include <core/storage/Pager.h>

void truncate_file(std::string_view fname) {
	std::ofstream of{std::string{fname}, std::ios::trunc};
}

using namespace internal::storage;

TEST_CASE("Page", "[pager]") {
	truncate_file("/tmp/eu-pager");
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
	truncate_file("/tmp/eu-persistent-pager-stackallocater");
	Pager pr_stack1("/tmp/eu-persistent-pager-stackallocater");

	for (int i = 0; i < 10; ++i)
		[[maybe_unused]] Position p = pr_stack1.alloc();
	REQUIRE(pr_stack1.allocator().cursor() == 10 * PAGE_SIZE);
	pr_stack1.save();

	Pager pr_stack2("/tmp/eu-persistent-pager-stackallocater", ActionOnConstruction::Load);
	REQUIRE(pr_stack2.allocator().cursor() == 10 * PAGE_SIZE);

	Pager pr_stack3("/tmp/eu-persistent-pager-stackallocater", ActionOnConstruction::DoNotLoad);
	REQUIRE(pr_stack3.allocator().cursor() == 0);
	pr_stack3.load();
	REQUIRE(pr_stack3.allocator().cursor() == 10 * PAGE_SIZE);
}

TEST_CASE("Page stack allocator", "[pager]") {
	Pager pr("/tmp/eu-pager-stack-alloc");
	for (int i = 0; i < 11; ++i) {
		REQUIRE(pr.alloc() == i * PAGE_SIZE);
		REQUIRE_THROWS_AS(pr.free(0), BadAlloc);
	}
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
