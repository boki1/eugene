set(EUGENE_SOURCES Index.h Index.cpp)
add_library(eugene "${EUGENE_SOURCES}")
set_target_properties(eugene PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(eugene PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/..")

get_property(EUGENE_INTERNAL_LIBS GLOBAL
             PROPERTY EUGENE_INTERNAL_LIBS_GLOBAL_LIST)
message(STATUS "Building internal libs: ${EUGENE_INTERNAL_LIBS}")
target_link_libraries(eugene PRIVATE "${EUGENE_INTERNAL_LIBS}")
