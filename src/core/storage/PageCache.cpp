#include <algorithm>
#include <array>
#include <fstream>
#include <filesystem>

#include <catch2/catch.hpp>

#include <core/storage/PageCache.h>
#include <core/storage/Page.h>
#include <core/storage/Position.h>

using namespace internal::storage;

static constexpr std::string_view test_output_fname = "/tmp/eu-bufpool-test1";
static constexpr std::size_t test_num_pages = 3;

void truncate_file(std::string_view fname) {
	std::ofstream of{std::string{fname}, std::ios::trunc};
}

TEST_CASE("buffer-pool-ops", "[bufpool]") {
	truncate_file(test_output_fname);
	PageCache pgcache(test_output_fname, PageCache::min_size());
	REQUIRE(!pgcache.full());

	std::array<uint8_t, Page::size()> data;
	auto make_dirty_page = [&, val = 0]() mutable {
		auto p = Page::empty();
		std::fill(data.begin(), data.end(), val++);
		p.write(0, data.cbegin(), data.cend());

		auto pos = pgcache.get_new_pos();
		// Don't copy in order to be able to compare read page later
		pgcache.put_page(pos, Page(p));
		REQUIRE(pgcache.get_page(pos) == p);

		auto &cpage = pgcache.get_page(pos);
		std::fill(data.begin(), data.end(), val++);
		cpage.write(0, data.cbegin(), data.cend());
		pgcache.put_page(pos, Page(cpage));
		REQUIRE(cpage == pgcache.get_page(pos));
	};

	for (size_t i = 0; i < test_num_pages; ++i) {
		make_dirty_page();
		REQUIRE(pgcache.full());
	}

	pgcache.flush_all();

	char *ptr = reinterpret_cast<char *>(const_cast<unsigned char *>(data.data()));

	REQUIRE(std::filesystem::file_size("/tmp/eu-bufpool-test1") == test_num_pages * Page::size());

	std::fstream fbufpool("/tmp/eu-bufpool-test1", std::ios::in);
	fbufpool.seekg(std::fstream::beg);

	auto value_of_page = [](int i) {
		return i * 2 + 1;
	};

	for (std::size_t i = 0; i < test_num_pages; ++i) {
		fbufpool.seekg(i * Page::size());
		fbufpool.read(ptr, Page::size());
		for (std::size_t j = 0; j < Page::size(); ++j)
			REQUIRE(ptr[j] == value_of_page(i));
	}
}
