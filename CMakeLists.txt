cmake_minimum_required(VERSION 3.19)
project(bt)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build")

# Gets populated by internal/ subdir
define_property(GLOBAL PROPERTY EUGENE_INTERNAL_LIBS_GLOBAL_LIST BRIEF_DOCS "internal libs of eugene" FULL_DOCS "internal libs of eugene")
set_property(GLOBAL PROPERTY EUGENE_INTERNAL_LIBS_GLOBAL_LIST "")

macro(add_internal_lib new_lib)
    set_property(GLOBAL APPEND PROPERTY EUGENE_INTERNAL_LIBS_GLOBAL_LIST "${new_lib}")
endmacro(add_internal_lib)

# Keep before eugene since EUGENE_INTERNAL_LIBS needs to be filled
add_subdirectory(src/internal)

add_subdirectory(src/eugene)

add_subdirectory(cients)
