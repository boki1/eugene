#include "Compression.h"

int main(const int argc, const char *argv[])
{
        Compressor compress{argc, argv};
        compress();
}