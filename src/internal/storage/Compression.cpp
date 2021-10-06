#include <Compression.h>

int main(const int argc, const char *argv[])
{
        Compressor compress = Compressor(argc, argv);
        compress();
}