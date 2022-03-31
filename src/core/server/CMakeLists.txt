cmake_minimum_required(VERSION 3.19)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

if (UNIX)
    add_definitions(-Wno-sign-compare -Wno-enum-compare)
endif (UNIX)
option(DBMS "Dbms" OFF)

set (LibEugeneServer_Example
        tests/UserRepository.h
        tests/ExampleUsage.cpp)

set(LibEugeneServer_External
        ../Logger.h
        ../storage/btree/Btree.h
        ../storage/btree/Btree.cpp)

set(LibEugeneServer_SRC
        handler/Handler.cpp handler/Handler.h
        detail/CredentialsStorage.h
        detail/CredentialsDecoder.h
        detail/Storage.h
        ${LibEugeneServer_Example}
        ${LibEugeneServer_External} tests/ExampleStorage.h)

add_library(LibEugeneServer "${LibEugeneServer_SRC}")
set_target_properties(LibEugeneServer PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(LibEugeneServer PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(LibEugeneServer
        CONAN_PKG::fmt
        CONAN_PKG::ms-gsl
        CONAN_PKG::catch2
        CONAN_PKG::spdlog
        CONAN_PKG::libnop
        CONAN_PKG::andreasbuhr-cppcoro
        CONAN_PKG::benchmark
        CONAN_PKG::cpprestsdk
        CONAN_PKG::cppitertools)

add_executable(restserver
        tests/ExampleUsage.cpp
        handler/Handler.cpp)

target_link_libraries(restserver PRIVATE LibEugeneServer)
