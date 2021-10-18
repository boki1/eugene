set(BTREE_DIR "btree/")
set(BTREE_SOURCES Btree.h)
list(TRANSFORM BTREE_SOURCES PREPEND ${BTREE_DIR})

add_library(btree ${BTREE_SOURCES})
set_target_properties(btree PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(btree PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(btree PRIVATE pthread)

get_property(EUGENE_INTERNAL_LIBS GLOBAL PROPERTY EUGENE_INTERNAL_LIBS_PROPERTY)
add_internal_lib(btree)

###############
set(STORAGE_DIR "storage/")
set(STORAGE_SOURCES Storage.h)
list(TRANSFORM STORAGE_SOURCES PREPEND ${STORAGE_DIR})


add_library(storage "${STORAGE_SOURCES}")
set_target_properties(storage PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(storage PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
add_internal_lib(storage)


# For Compression
set(COMPRESSION_SOURCES Compressor.h)
list(TRANSFORM COMPRESSION_SOURCES PREPEND ${STORAGE_DIR})

add_library(compression "${COMPRESSION_SOURCES}")
set_target_properties(compression PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(compression PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
add_internal_lib(compression)

add_executable(compressor storage/Compressor.cpp)


# For Decompression
set(DECOMPRESSION_SOURCES Decompressor.h)
list(TRANSFORM DECOMPRESSION_SOURCES PREPEND ${STORAGE_DIR})

add_library(decompression "${DECOMPRESSION_SOURCES}")
set_target_properties(decompression PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(decompression PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
add_internal_lib(decompression)

add_executable(decompressor storage/Decompressor.cpp)