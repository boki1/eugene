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
        -fdiagnostics-color)

enable_testing()
include(FetchContent)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

set(LibEugeneBtree_SRC
        storage/btree/Btree.h
        storage/btree/Btree.cpp
        storage/btree/BenchBtree.cpp
        storage/btree/Node.h
        storage/btree/BtreePrinter.h
        storage/btree/Config.h)

set(LibEugenePager_SRC
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
        Util.h Util.cpp SizeMetrics.h Logger.h
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
        CONAN_PKG::catch2
        CONAN_PKG::spdlog
        CONAN_PKG::libnop
        CONAN_PKG::benchmark
        CONAN_PKG::catch2
        CONAN_PKG::cppbenchmark)

if (DEFINED EU_BUILD_TESTS)
	message(STATUS "EU_BUILD_TESTS = " ${EU_BUILD_TESTS})
	if ("${EU_BUILD_TESTS}" STREQUAL "yes")
		macro(eugene_test component path_prefix)
			add_executable(Test${component} ${path_prefix}/${component}.cpp)
			target_link_libraries(Test${component} TestMain LibEugeneCore)
			add_test(NAME Test${component} COMMAND Test${component})
			message(STATUS "\tTest about '${component}' component added from \"${path_prefix}/${component}.cpp\"")
		endmacro()

		message(STATUS "Building core unit tests")

		add_library(TestMain src/core/Main.cpp)
		target_link_libraries(TestMain CONAN_PKG::catch2)

		eugene_test(Compression src/core/storage/compression)
		eugene_test(Btree src/core/storage/btree)
		eugene_test(Node src/core/storage/btree)
		eugene_test(Page src/core/storage)
		eugene_test(PageCache src/core/storage)
		eugene_test(Util src/core)
	endif()
endif ()


if (DEFINED EU_BUILD_BENCHMARKS)
	message(STATUS "EU_BUILD_BENCHMARKS = " ${EU_BUILD_BENCHMARKS})
	if ("${EU_BUILD_BENCHMARKS}" STREQUAL "yes")
		macro(eugene_bench component path_prefix)
			set(bench_name Bench${component})
			add_executable(${bench_name} ${path_prefix}/${bench_name}.cpp)
			target_link_libraries(${bench_name} LibEugeneCore CONAN_PKG::benchmark)
			message(STATUS "\tBenchmark about '${component}' component added from \"${path_prefix}/${bench_name}.cpp\"")
		endmacro()

		message(STATUS "Building core benchmarks")

		#eugene_bench(Compression src/core/storage/compression)
		eugene_bench(Btree src/core/storage/btree)
	endif()
endif ()
