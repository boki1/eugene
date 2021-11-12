cmake_minimum_required(VERSION 3.19)
project(eugene)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build")

add_compile_options(
	-Wall
	-Wno-comment
	-Wextra
	-Werror
	-pedantic
	-ggdb3
	-fdiagnostics-color
	-fconcepts-diagnostics-depth=2)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

enable_testing()
include(FetchContent)

# Gets populated by internal/ subdirs
define_property(GLOBAL PROPERTY EUGENE_INTERNAL_LIBS_GLOBAL_LIST BRIEF_DOCS "internal libs of eugene" FULL_DOCS "internal libs of eugene")
set_property(GLOBAL PROPERTY EUGENE_INTERNAL_LIBS_GLOBAL_LIST "")

macro(add_eugene_internal_lib new_lib)
    set_property(GLOBAL APPEND PROPERTY EUGENE_INTERNAL_LIBS_GLOBAL_LIST "${new_lib}")
endmacro(add_eugene_internal_lib)

add_subdirectory(src/third-party)

# Keep before eugene since EUGENE_INTERNAL_LIBS needs to be filled
add_subdirectory(src/core)

add_subdirectory(src/eugene-api)

add_subdirectory(clients)

if (NOT DEFINED SKIP_BUILD_TESTS)
	set(SKIP_BUILD_TESTS "False")
endif()

if ("${SKIP_BUILD_TESTS}" STREQUAL "False")
	message("-- Building tests")
	add_subdirectory(tests)
endif()
