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

set(LibEugeneBtree_SRC
	storage/btree/Btree.h
	storage/btree/Btree.cpp
	storage/btree/Bench.cpp
	storage/btree/Node.h
	storage/btree/BtreePrinter.h
	storage/btree/Config.h)

set (LibEugenePager_SRC
	storage/Position.h
	storage/PageCache.h
	storage/PageCache.cpp
	storage/Page.h
	storage/Page.cpp)

set(LibEugeneCompression_SRC
	storage/compression/Compressor.h
	storage/compression/Decompressor.h
	storage/compression/Compression.cpp)

set(LibEugeneCore_SRC
		Util.h SizeMetrics.h Logger.h
		${LibEugeneBtree_SRC}
		${LibEugenePager_SRC}
		${LibEugeneCompression_SRC})
list(TRANSFORM LibEugeneCore_SRC PREPEND "src/core/")

add_library(LibEugeneCore "${LibEugeneCore_SRC}")
set_target_properties(LibEugeneCore PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(LibEugeneCore PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(LibEugeneCore
        CONAN_PKG::fmt
        CONAN_PKG::ms-gsl
        CONAN_PKG::outcome
        CONAN_PKG::catch2
        CONAN_PKG::spdlog
        CONAN_PKG::libnop
        CONAN_PKG::benchmark
        CONAN_PKG::catch2)

if (DEFINED ENV{EUGENE_BUILD_TESTS})
    macro(eugene_test component path_prefix)
        add_executable(Test${component} ${path_prefix}/${component}.cpp)
		    target_link_libraries(Test${component} TestMain LibEugeneCore)
        add_test(NAME Test${component} COMMAND Test${component})
        message("   -- Test about '${component}' component added from \"${path_prefix}/${component}.cpp\"")
    endmacro()

    message("-- Building core unit tests")

    add_library(TestMain src/core/Main.cpp)
    target_link_libraries(TestMain CONAN_PKG::catch2)

    eugene_test(Compression src/core/storage/compression)
    eugene_test(Btree src/core/storage/btree)
	  eugene_test(Node src/core/storage/btree)
    eugene_test(Page src/core/storage)
    eugene_test(PageCache src/core/storage)
endif()

if (DEFINED ENV{EUGENE_BUILD_BENCHMARKS})
    add_executable(BtreeBench src/core/storage/btree/Bench.cpp)
    target_link_libraries(BtreeBench LibEugeneCore)

    add_executable(benchmark src/core/storage/compression/BenchmarkCompression.cpp)
    target_link_libraries(benchmark PUBLIC LibEugeneCore)

    message("-- Building benchmarks")
endif()

add_executable(main src/standalone/json-parse/main.cpp)
target_link_libraries(main
        CONAN_PKG::nlohmann_json)