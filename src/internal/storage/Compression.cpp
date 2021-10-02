#include <Compression.h>

int main(const int argc, const char *argv[])
{
        Compress compress = Compress(argc, argv);
        compress.compress();
}