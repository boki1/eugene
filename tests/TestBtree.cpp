#include <external/catch2/Catch2.h>

#include <internal/storage/Storage.h>
#include <internal/btree/Btree.h>

using namespace internal::btree;

TEST_CASE("Btree init", "[btree]") {
	Btree bpt{};
}

TEST_CASE("Btree find", "[btree]") {
	Btree bpt{};
	auto it1 = bpt.find(13u);
}

TEST_CASE("Btree insert", "[btree]") {
	Btree bpt{};
	// auto it1 = bpt.insert(13u, 12u);
}

