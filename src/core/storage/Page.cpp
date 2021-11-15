#include <algorithm>

#include <third-party/catch2/Catch2.h>

#include <core/storage/Page.h>

using namespace internal::storage;

TEST_CASE("Page", "[page]") {
	Pager pr("/tmp/page-test-disk");

	std::array<uint8_t, PAGE_SIZE> p1_data;
	std::iota(p1_data.begin(), p1_data.end(), 1);
	Page p1(std::move(p1_data));
	REQUIRE(pr.sync(p1) == 0l);

	std::array<uint8_t, PAGE_SIZE> p2_data;
	std::iota(p2_data.begin(), p2_data.end(), 2);
	Page p2(std::move(p2_data));
	REQUIRE(pr.sync(p2) == (long)PAGE_SIZE);

	REQUIRE(pr.fetch(p2, 0) == p1);
}
