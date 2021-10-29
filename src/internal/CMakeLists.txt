set(BTREE_DIR "btree/")
set(BTREE_SOURCES Btree.h BtreePrinter.h)
list(TRANSFORM BTREE_SOURCES PREPEND ${BTREE_DIR})

add_library(btree ${BTREE_SOURCES})
set_target_properties(btree PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(btree PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(btree PRIVATE pthread expected)

get_property(EUGENE_INTERNAL_LIBS GLOBAL PROPERTY EUGENE_INTERNAL_LIBS_PROPERTY)
add_internal_lib(btree)

set(STORAGE_DIR "storage/")
set(STORAGE_SOURCES Storage.h)
list(TRANSFORM STORAGE_SOURCES PREPEND ${STORAGE_DIR})

add_library(storage "${STORAGE_SOURCES}")
set_target_properties(storage PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(storage PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
add_internal_lib(storage)
