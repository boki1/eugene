cmake_minimum_required(VERSION 3.20)
project(storage)

set(CMAKE_CXX_STANDARD 20)

include_directories(.)

add_executable(storage
        Compression.cpp
        Compression.h
        Storage.h)
