cmake_minimum_required(VERSION 3.19)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

if (UNIX)
    add_definitions(-Wno-sign-compare -Wno-enum-compare)
endif (UNIX)
option(DBMS "Dbms" OFF)

set(LibEugeneServer_SRC
        handler/Handler.cpp
        src/User.cpp src/User.h src/ClientGroup.cpp src/ClientGroup.h )

add_library(LibEugeneServer "${LibEugeneServer_SRC}")
set_target_properties(LibEugeneServer PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(LibEugeneServer PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(LibEugeneServer CONAN_PKG::cpprestsdk)

add_library(LibEugeneServer_handler handler/Handler.h)
set_target_properties(LibEugeneServer_handler PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(LibEugeneServer_handler PUBLIC src)

add_executable(restserver
        tests/ExampleUsage.cpp
        handler/Handler.cpp)

target_link_libraries(restserver PRIVATE LibEugeneServer)
