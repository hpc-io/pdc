cmake_minimum_required (VERSION 2.7)

PROJECT(H5BOSS_PDC)

find_package(MERCURY REQUIRED)
if(MERCURY_FOUND)
    #message(STATUS "mercury include directory: ${MERCURY_INCLUDE_DIR}")
  set(MERCURY_EXT_INCLUDE_DEPENDENCIES ${MERCURY_INCLUDE_DIR}
    ${MERCURY_EXT_INCLUDE_DEPENDENCIES}
    )
  set(MERCURY_EXT_LIB_DEPENDENCIES mercury ${MERCURY_EXT_LIB_DEPENDENCIES})
endif()

find_package(PDC REQUIRED)
if(PDC_FOUND)
    #message(STATUS "PDC include directory: ${PDC_INCLUDE_DIR}")
  set(PDC_EXT_INCLUDE_DEPENDENCIES ${PDC_INCLUDE_DIR}
    ${PDC_EXT_INCLUDE_DEPENDENCIES}
    )
  set(PDC_EXT_LIB_DEPENDENCIES pdc ${PDC_EXT_LIB_DEPENDENCIES})
endif()

include_directories(${PDC_EXT_INCLUDE_DEPENDENCIES})


set(PROGRAMS h5boss_import h5boss_query_random h5boss_pdc_query)

foreach(program ${PROGRAMS})
  add_executable(${program}.exe ${program}.c)
  target_link_libraries(${program}.exe ${PDC_EXT_LIB_DEPENDENCIES} )
endforeach(program)

