#include <external/catch2/Catch2.h>

#include <internal/storage/Storage.h>
#include <internal/btree/Btree.h>

using namespace internal::btree;

TEST_CASE("Btree init", "[btree]") {
	Btree bpt{};
}

