
add_library(llsm_tiff parallelReadTiff.c)
target_compile_options(llsm_tiff PRIVATE ${OpenMP_C_FLAGS})
target_include_directories(llsm_tiff PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(llsm_tiff PUBLIC ${OpenMP_C_LIBRARIES})