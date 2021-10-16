add_executable(compressor Compression.cpp Compression.h)
target_link_libraries(compressor PRIVATE catch2)

add_executable(decompressor Decompressor.cpp Decompressor.h)
target_link_libraries(decompressor PRIVATE catch2)

