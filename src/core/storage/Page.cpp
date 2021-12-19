#include <algorithm>

#include "catch2/catch.hpp"

#include <core/storage/Page.h>

void truncate_file(std::string_view fname) {
	std::ofstream of{std::string{fname}, std::ios::trunc};
}

using namespace internal::storage;

TEST_CASE("Page", "[page]") {
	truncate_file("/tmp/eu-pager");
	Pager pr("/tmp/eu-pager");

	std::array<uint8_t, Page::size()> p1_data;
	std::iota(p1_data.begin(), p1_data.end(), 1);
	Page p1(std::move(p1_data));
	REQUIRE(pr.sync(p1) == 0l);

	auto p2_pos = pr.alloc();
	REQUIRE((long) p2_pos == Page::size());

	std::array<uint8_t, Page::size()> p3_data;
	std::iota(p3_data.begin(), p3_data.end(), 3);
	Page p3(std::move(p3_data));
	REQUIRE(pr.sync(p3) == Position(2l * Page::size()));

	std::array<uint8_t, Page::size()> p2_data;
	std::iota(p2_data.begin(), p2_data.end(), 2);
	Page p2(std::move(p2_data));
	REQUIRE(pr.sync(p2, p2_pos) == (long) p2_pos);

	pr.fetch(p3, 0);
	REQUIRE(p3 == p1);
	pr.fetch(p3, p2_pos);
	REQUIRE(p3 == p2);
}

TEST_CASE("Page operations", "[page]") {
	std::array<uint8_t, Page::size()> p_data;
	std::fill(p_data.begin(), p_data.end(), 1);
	Page p(std::move(p_data));

	auto [d1_begin, d1_end] = p.read(10, 5).value();
	std::array<uint8_t, 5> d1_expected { 1 };
	std::fill(d1_expected.begin(), d1_expected.end(), 1);
	REQUIRE(std::distance(d1_begin, d1_end) == 5);
	REQUIRE(std::equal(d1_begin, d1_end, d1_expected.cbegin()));

	for (std::size_t i = 0; i < Page::size(); ++i) {
		auto d2 = p.read(i).value();
		REQUIRE(d2 == 1);
	}

	std::array<uint8_t, 10> d3;
	std::iota(d3.begin(), d3.end(), 1);
	p.write(100, d3.cbegin(), d3.cend());
	auto [d3_begin, d3_end] = p.read(100, 10).value();
	REQUIRE(std::equal(d3_begin, d3_end, d3.cbegin()));

	REQUIRE(!p.read(Page::size(), 1).has_value());
}
