cmake_minimum_required(VERSION 3.19)
project(eugene)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(
  -Wall
  -Wno-comment
  -Wextra
  -Werror
  -pedantic
  -ggdb3
  -fdiagnostics-color
  -fconcepts-diagnostics-depth=2)

enable_testing()
include(FetchContent)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

# find_package(fmt)
# find_package(spdlog)
# find_package(outcome)
# find_package(ms-gsl)
# find_package(catch2)

set(EUGENE_CORE_SRC
	Position.h PageCache.h Page.h PageCache.cpp Page.cpp
	Storage.h
	btree/Btree.h btree/BtreePrinter.h btree/Btree.cpp
	compression/Compressor.h compression/Decompressor.h compression/TestCompression.cpp)
list(TRANSFORM EUGENE_CORE_SRC PREPEND "src/core/storage/")

add_library(eugene_core "${EUGENE_CORE_SRC}")
set_target_properties(eugene_core PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(eugene_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(eugene_core ${CONAN_LIBS})
#target_link_libraries(eugene_core
#						CONAN_PKG::outcome
#						CONAN_PKG::fmt
#						CONAN_PKG::ms-gsl)

if(DEFINED ENV{EUGENE_BUILD_TESTS})
  message("-- Building core unit tests")

  add_library(test_main Main.cpp)
  target_link_libraries(test_main ${CONAN_LIBS})

  add_executable(test_compression storage/compression/TestCompression.cpp)
  target_link_libraries(test_compression PUBLIC test_main)
  add_test(NAME TestCompression COMMAND test_compression)

  add_executable(test_btree storage/btree/Btree.cpp)
  target_link_libraries(test_btree PUBLIC test_main storage)
  add_test(NAME TestBtree COMMAND test_btree)

  add_executable(test_page storage/Page.h storage/Page.cpp)
  target_link_libraries(test_page PUBLIC test_main)
  add_test(NAME TestPage COMMAND test_page)

  add_executable(test_pgcache storage/PageCache.h storage/PageCache.cpp)
  target_link_libraries(test_pgcache PUBLIC test_main)
  add_test(NAME TestPageCache COMMAND test_pgcache)
endif()
