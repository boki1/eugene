set(BTREE_SOURCES btree/Btree.h btree/BtreePrinter.h)

set(COMPR_SOURCE compression/Compressor.h compression/Decompressor.h)

set(STORAGE_SOURCES Storage.h "${BTREE_SOURCE}" "${COMPR_SOURCE}")
list(TRANSFORM STORAGE_SOURCES PREPEND "storage/")

add_library(storage "${STORAGE_SOURCES}")
add_eugene_internal_lib(storage)

set_target_properties(storage PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(storage PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(storage PRIVATE expected)
