# CMakeLists files in this project can
# refer to the root source directory of the project as ${PDC_SOURCE_DIR} and
# to the root binary directory of the project as ${PDC_BINARY_DIR}.
cmake_minimum_required (VERSION 2.8.11)
project (PDC)

#include_directories(${PROJECT_SOURCE_DIR})
#link_directories(${PROJECT_BINARY_DIR})

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

find_package(mercury REQUIRED)
if(mercury_FOUND)
    include_directories(${MERCURY_INCLUDE_DIR})
endif()

#add_definitions(-DENABLE_MPI=1)
#add_definitions(-DENABLE_MULTITHREAD=1)

add_subdirectory (src)
add_subdirectory (tests)
