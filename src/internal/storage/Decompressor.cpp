#include "Decompressor.h"

int main(const int argc, const char *argv[])
{
        Decompressor decompress{argv[1]};
        decompress();
}