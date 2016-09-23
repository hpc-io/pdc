# CMakeLists files in this project can
# refer to the root source directory of the project as ${PDC_SOURCE_DIR} and
# to the root binary directory of the project as ${PDC_BINARY_DIR}.
cmake_minimum_required (VERSION 2.8.11)
project (PDC)

#include_directories(${PROJECT_SOURCE_DIR})
#link_directories(${PROJECT_BINARY_DIR})

add_subdirectory (src)
add_subdirectory (tests)
