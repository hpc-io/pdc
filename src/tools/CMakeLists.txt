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

set(PROGRAMS
  pdc_import
  pdc_export
  pdc_ls
  )

add_library(cjson cjson/cJSON.c)

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
      target_compile_options(${program} PRIVATE ${SUPPRESSED_LIST})
  endif()
  target_link_libraries(${program} pdc)
  target_link_libraries(${program} cjson)
endforeach(program)


# *******************************************
# Add the HDF5 library for pdc-neon
# *******************************************
FIND_LIBRARY(HDF5_LIBRARY NAMES hdf5_debug PATHS $ENV{HOME}/lib)
