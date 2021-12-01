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
conan_basic_setup(TARGETS)

# #############################################################################
# Storage setup
set(EUGENE_STORAGE src/core/storage)
set(LibEugeneCore_SRC
        Position.h PageCache.h Page.h PageCache.cpp Page.cpp
        Storage.h Logger.h
        btree/Btree.h btree/BtreePrinter.h btree/Btree.cpp
        compression/Compressor.h compression/Decompressor.h
        compression/TestCompression.cpp)
list(TRANSFORM LibEugeneCore_SRC PREPEND "${EUGENE_STORAGE}/")

add_library(LibEugeneCore "${LibEugeneCore_SRC}")
set_target_properties(LibEugeneCore PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(LibEugeneCore PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(LibEugeneCore
        CONAN_PKG::fmt
        CONAN_PKG::ms-gsl
        CONAN_PKG::outcome
        CONAN_PKG::catch2
        CONAN_PKG::spdlog
        CONAN_PKG::cppbenchmark)


if (DEFINED ENV{EUGENE_BUILD_TESTS})
    macro(eugene_test component path_prefix)
        add_executable(Test${component} ${path_prefix}/${component}.cpp)
        target_link_libraries(Test${component} TestMain LibEugeneCore)
        add_test(NAME Test${component}
                COMMAND Test${component})
        message("   -- Test about '${component}' component added from \"${path_prefix}/${component}.cpp\"")
    endmacro()

    message("-- Building core unit tests")

    add_library(TestMain src/core/Main.cpp)
    target_link_libraries(TestMain LibEugeneCore)

    eugene_test(TestCompression ${EUGENE_STORAGE}/compression)
    eugene_test(Btree ${EUGENE_STORAGE}/btree)
    eugene_test(Page ${EUGENE_STORAGE})
    eugene_test(PageCache ${EUGENE_STORAGE})

    # add executable for compression/benchmarks/BenchmarkCompressor.cpp
    add_executable(benchmark ${EUGENE_STORAGE}/compression/BenchmarkCompression.cpp)
    target_link_libraries(benchmark PUBLIC LibEugeneCore)
endif ()
