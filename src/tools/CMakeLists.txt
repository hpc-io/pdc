project(${PDC_SOURCE_DIR})
cmake_minimum_required (VERSION 3.0)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PDC_INCLUDES_BUILD_TIME}
  ${PROJECT_BINARY_DIR}
  ${PDC_SOURCE_DIR}
  ${PDC_SOURCE_DIR}/src/client_api/include
  $ENV{HOME}/Sandbox/c-blosc/blosc
  $ENV{HOME}/include
)

set(EXT_LIB "")
set(EXT_INCLUDE_DIRS "")


#-----------------------------------------------------------------------------
# Find HDF5
#-----------------------------------------------------------------------------
#HDF5

find_package(HDF5 MODULE REQUIRED)
if (NOT HDF5_FOUND)
  message(STATUS "Could not find HDF5 via CMake configuration, fallback to traditional search")
  find_package(HDF5 NO_MODULE NAMES hdf5 COMPONENTS C shared)
  if (NOT HDF5_FOUND)
    message(FATAL_ERROR "Could not find HDF5, please make sure that HDF5 has been compiled with shared libraries enabled.")
  endif()
endif()
if(HDF5_FOUND)
  set(EXT_INCLUDE_DIRS
    ${EXT_INCLUDE_DIRS}
    ${HDF5_INCLUDE_DIRS}
  )
  set(EXT_LIB
    ${EXT_LIB}
    ${HDF5_LIBRARIES}
  )
endif()

# *************************************************
# * Find MERCURY
# *************************************************
# Mercury
find_package(MERCURY REQUIRED)
if(MERCURY_FOUND)
  set(EXT_INCLUDE_DIRS ${MERCURY_INCLUDE_DIRS}
    ${EXT_INCLUDE_DIRS}
    )
  set(EXT_LIB ${MERCURY_LIBRARIES} ${EXT_LIB})
endif()
include_directories(${EXT_INCLUDE_DIRS})

set(PROGRAMS
  pdc_import
  pdc_export
  pdc_ls
  )

add_library(cjson cjson/cJSON.c)
set(EXT_LIB cjson ${EXT_LIB})

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc pdc_commons ${EXT_LIB})
endforeach(program)



