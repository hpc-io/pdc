cmake_minimum_required (VERSION 2.8.12)

project(PDC_VOL C)

include_directories(
  ${PDC_EXT_INCLUDE_DEPENDENCIES}
)

find_package(PDC REQUIRED)
if(PDC_FOUND)
    #message(STATUS "PDC include directory: ${PDC_INCLUDE_DIR}")
  set(PDC_EXT_INCLUDE_DEPENDENCIES ${PDC_INCLUDE_DIR}
    ${PDC_EXT_INCLUDE_DEPENDENCIES}
    )
  set(PDC_EXT_LIB_DEPENDENCIES pdc ${PDC_EXT_LIB_DEPENDENCIES})
endif()

add_definitions(-DENABLE_MPI=1)
add_library(cjson cjson/cJSON.c)

set(PROGRAMS
  pdc_ls
  )

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} ${PDC_EXT_LIB_DEPENDENCIES})
  target_link_libraries(${program} cjson)
endforeach(program)
