#include <internal/storage/Compressor.h>

int main(const int argc, const char *argv[])
{
        compression::Compressor compress{argc, argv, "test"};
        compress();
}