#include <internal/storage/Decompressor.h>

int main(const int argc, const char *argv[])
{
        decompression::Decompressor decompress{argv[1]};
        decompress();
}