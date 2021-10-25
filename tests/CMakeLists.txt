set(TESTS_DEPENDENCIES catch2)

############################## Main ()

add_library(test__main_fun TestMain.cpp)
target_link_libraries(test__main_fun ${TESTS_DEPENDENCIES})

############################## ItWorks

add_executable(test__itworks TestItWorks.cpp)
target_link_libraries(test__itworks PUBLIC test__main_fun)
add_test(NAME ItWorks COMMAND test__itworks)


############################## Compression

add_executable(test__compressor CompressorDecompressorTests/TestCompressor.cpp)
target_link_libraries(test__compressor PUBLIC test__main_fun)
add_test(NAME Compressor COMMAND test__compressor)