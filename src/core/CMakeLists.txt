set(BTREE_SOURCES btree/Btree.h btree/BtreePrinter.h)

set(COMPR_SOURCE compression/Compressor.h compression/Decompressor.h)

set(STORAGE_SOURCES Storage.h "${BTREE_SOURCE}" "${COMPR_SOURCE}")
list(TRANSFORM STORAGE_SOURCES PREPEND "storage/")

add_library(storage "${STORAGE_SOURCES}")
add_eugene_internal_lib(storage)

set_target_properties(storage PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(storage PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(storage PRIVATE expected)

# Unit tests
if(DEFINED ENV{EUGENE_BUILD_TESTS})
  message("-- Building core unit tests")

  add_library(test_main Main.cpp)
  target_link_libraries(test_main catch2)

  add_executable(test_compressor_decompressor storage/compression/Compressor.cpp)
  target_link_libraries(test_compressor_decompressor PUBLIC test_main)
  add_test(NAME TestCompressorDecompressor COMMAND test_compressor_decompressor)

  add_executable(test_btree storage/btree/Btree.cpp)
  target_link_libraries(test_btree PUBLIC test_main storage)
  add_test(NAME TestBtree COMMAND test_btree)
endif()
