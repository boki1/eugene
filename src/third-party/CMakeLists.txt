add_library(expected expected/Expected.h expected/Expected.cpp)
set_target_properties(expected PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(expected PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/..")

add_library(catch2 catch2/Catch2.h catch2/Catch2.cpp)
set_target_properties(catch2 PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(catch2 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/..")
