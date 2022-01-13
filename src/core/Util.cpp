#include <limits>

#include <catch2/catch.hpp>

#include <core/Util.h>

using namespace internal;

TEST_CASE("Primitive binsearch", "[util]") {
	auto square3 = [](long big) {
		return binsearch_primitive(static_cast<decltype(big)>(0), 128l, [=](long curr, long, long) {
			return curr * curr * curr - big;
		});
	};

	auto square2 = [](long big) {
		return binsearch_primitive(static_cast<decltype(big)>(0), 256l, [=](long curr, long, long) {
			return curr * curr - big;
		});
	};

	REQUIRE(square3(27).value() == 3);
	REQUIRE(square3(343).value() == 7);
	REQUIRE(square3(4913).value() == 17);
	REQUIRE(square3(1953125).value() == 125);
	REQUIRE(square3(1000000).value() == 100);

	REQUIRE(square2(10000).value() == 100);
	REQUIRE(square2(729).value() == 27);
	REQUIRE(square2(9).value() == 3);
	REQUIRE(square2(49).value() == 7);
	REQUIRE(square2(65536).value() == 256);

	// REQUIRE(binsearch_primitive(1, 1, [](long,long,long) { return -1; }));
}
