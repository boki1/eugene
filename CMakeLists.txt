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
	storage/btree/Bench.cpp
	storage/btree/Node.h
	storage/btree/BtreePrinter.h
	storage/btree/Config.h)

set(LibEugenePager_SRC
        storage/Pager.h
        storage/Pager.cpp)

set(LibEugeneCompression_SRC
	storage/compression/Compressor.h
	storage/compression/Decompressor.h)

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
		CONAN_PKG::andreasbuhr-cppcoro
        CONAN_PKG::benchmark
        CONAN_PKG::catch2
        CONAN_PKG::cppbenchmark)


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

	eugene_test(Btree src/core/storage/btree)
	eugene_test(Node src/core/storage/btree)
	eugene_test(Pager src/core/storage)
	eugene_test(Util src/core)

	add_library(LibTestsShared src/core/storage/compression/tests/Shared.h)
	set_target_properties(LibTestsShared PROPERTIES LINKER_LANGUAGE CXX)
	target_include_directories(LibTestsShared PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src/core/storage/compression/tests")

	eugene_test(CompDecompTests src/core/storage/compression/tests)
	eugene_test(CompTests src/core/storage/compression/tests)
	eugene_test(DecompTests src/core/storage/compression/tests)
endif()


if (DEFINED ENV{EUGENE_BUILD_BENCHMARKS})
	add_executable(BtreeBench src/core/storage/btree/Bench.cpp)
	target_link_libraries(BtreeBench PUBLIC LibEugeneCore)


	add_library(LibCompShared src/core/storage/compression/benchmarks/Shared.h)
	set_target_properties(LibCompShared PROPERTIES LINKER_LANGUAGE CXX)
	target_include_directories(LibCompShared PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src/core/storage/compression/benchmarks")

	add_executable(bench_comp src/core/storage/compression/benchmarks/CompBench.cpp)
	target_link_libraries(bench_comp PUBLIC LibEugeneCore LibCompShared)

	add_executable(bench_decomp src/core/storage/compression/benchmarks/DecompBench.cpp)
	target_link_libraries(bench_decomp PUBLIC LibEugeneCore LibCompShared)

	message("-- Building benchmarks")
endif()

add_executable(main src/standalone/json-parse/main.cpp)
target_link_libraries(main
        CONAN_PKG::nlohmann_json)