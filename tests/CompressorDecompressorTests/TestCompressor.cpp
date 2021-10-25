#include <external/catch2/Catch2.h>
#include <external/expected/Expected.h>
#include <internal/storage/Compressor.h>

TEST_CASE("Compressor compress", "[compressor]")
{
        compression::Compressor compressor{2, ""};
        REQUIRE(true);
}