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
  -Wfatal-errors
  -fdiagnostics-color)

enable_testing()
include(FetchContent)

add_compile_definitions(NDEBUG="true")

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

set(LibEugeneBtree_SRC storage/btree/Btree.h
        storage/btree/Btree.cpp
        storage/btree/Node.h
        storage/btree/BtreePrinter.h)

set(LibEugenePager_SRC storage/Pager.h
        storage/Pager.cpp)

set(LibEugeneCompression_SRC storage/compression/Compressor.h
        storage/compression/Decompressor.h)

set(LibEugeneServer_Example
    server/handler/Handler.h
    server/detail/Credentials.h
    server/detail/CredentialsStorage.h
    server/detail/CredentialsDecoder.h
    server/detail/Storage.h
    server/tests/UserRepository.h
    server/tests/ExampleStorage.h)

set(LibEugeneCore_SRC
    Config.h
    Util.h
    Util.cpp
    Logger.h
    ${LibEugeneBtree_SRC}
    ${LibEugenePager_SRC}
    ${LibEugeneCompression_SRC}
    ${LibEugeneServer_Example})

list(TRANSFORM LibEugeneCore_SRC PREPEND "src/core/")

add_library(LibEugeneCore "${LibEugeneCore_SRC}")
set_target_properties(LibEugeneCore PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(LibEugeneCore
                           PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(
  LibEugeneCore
  CONAN_PKG::fmt
  CONAN_PKG::ms-gsl
  CONAN_PKG::catch2
  CONAN_PKG::spdlog
  CONAN_PKG::libnop
  CONAN_PKG::andreasbuhr-cppcoro
  CONAN_PKG::benchmark
  CONAN_PKG::cpprestsdk
  CONAN_PKG::cppitertools)

if(DEFINED EU_BUILD_TESTS)
  message(STATUS "EU_BUILD_TESTS = " ${EU_BUILD_TESTS})
  if("${EU_BUILD_TESTS}" STREQUAL "yes")
    macro(eugene_test component path_prefix)
      add_executable(Test${component} ${path_prefix}/${component}.cpp)
      target_link_libraries(Test${component} TestMain LibEugeneCore)
      add_test(NAME Test${component} COMMAND Test${component})
      message(
        STATUS
          "\tTest about '${component}' component added from \"${path_prefix}/${component}.cpp\""
      )
    endmacro()

    message(STATUS "Building core unit tests")

    add_library(TestMain src/core/Main.cpp)
    target_link_libraries(TestMain CONAN_PKG::catch2)

    eugene_test(Btree src/core/storage/btree)
    eugene_test(Node src/core/storage/btree)
    eugene_test(Pager src/core/storage)
    eugene_test(Util src/core)

    add_library(LibTestsShared src/core/storage/compression/tests/Shared.h)
    set_target_properties(LibTestsShared PROPERTIES LINKER_LANGUAGE CXX)
    target_include_directories(
      LibTestsShared
      PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src/core/storage/compression/tests")

    eugene_test(CompDecompTests src/core/storage/compression/tests)
    eugene_test(CompTests src/core/storage/compression/tests)
    eugene_test(DecompTests src/core/storage/compression/tests)
  endif()
endif()

if(DEFINED EU_BUILD_BENCHMARKS)
  message(STATUS "EU_BUILD_BENCHMARKS = " ${EU_BUILD_BENCHMARKS})
  if("${EU_BUILD_BENCHMARKS}" STREQUAL "yes")
    macro(eugene_bench component path_prefix)
      set(bench_name ${component})
      add_executable(${bench_name} ${path_prefix}/${bench_name}.cpp)
      target_link_libraries(${bench_name} LibEugeneCore CONAN_PKG::benchmark)
      message(
        STATUS
          "\tBenchmark about '${component}' component added from \"${path_prefix}/${bench_name}.cpp\""
      )
    endmacro()

    message(STATUS "Building core benchmarks")

    eugene_bench(BtreeBench src/core/storage/btree)

    add_library(LibCompShared src/core/storage/compression/benchmarks/Shared.h)
    set_target_properties(LibCompShared PROPERTIES LINKER_LANGUAGE CXX)
    target_include_directories(
      LibCompShared
      PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/src/core/storage/compression/benchmarks")

    eugene_bench(CompBench src/core/storage/compression/benchmarks)
    eugene_bench(DecompBench src/core/storage/compression/benchmarks)
  endif()
endif()

if(DEFINED EU_BUILD_EXAMPLES)
  message(STATUS "EU_BUILD_EXAMPLES = " ${EU_BUILD_EXAMPLES})
  if("${EU_BUILD_EXAMPLES}" STREQUAL "yes")
    macro(eugene_example example)
	  set(example_name Example_${example})
	  add_executable(${example_name} ${CMAKE_CURRENT_SOURCE_DIR}/src/examples/${example}.cpp)
      target_link_libraries(${example_name} LibEugeneCore)
      message(STATUS "\tExample '${example}' added")
    endmacro()

        eugene_example(person_kv)
        eugene_example(UsageServer)
    endif ()
endif ()
