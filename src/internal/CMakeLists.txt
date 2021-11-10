set(BTREE_DIR "btree/")
set(BTREE_SOURCES Btree.h)
list(TRANSFORM BTREE_SOURCES PREPEND ${BTREE_DIR})

add_library(btree ${BTREE_SOURCES})
set_target_properties(btree PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(btree PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(btree PRIVATE pthread)

get_property(EUGENE_INTERNAL_LIBS GLOBAL PROPERTY EUGENE_INTERNAL_LIBS_PROPERTY)
add_eugene_internal_lib(btree)


set(BTREE_SOURCES
        btree/Btree.h
        btree/BtreePrinter.h)

set(STORAGE_SOURCES Storage.h "${BTREE_SOURCE}")
list(TRANSFORM STORAGE_SOURCES PREPEND "storage/")

add_library(storage "${STORAGE_SOURCES}")
add_eugene_internal_lib(storage)

set_target_properties(storage PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(storage PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(storage PRIVATE pthread expected)
