cmake_minimum_required (VERSION 2.8.12)

# Setup cmake policies.
foreach(p
    CMP0012
    CMP0013
    CMP0014
    CMP0022 # CMake 2.8.12
    CMP0025 # CMake 3.0
    CMP0053 # CMake 3.1
    CMP0054 # CMake 3.1
    CMP0074 # CMake 3.12
    CMP0075 # CMake 3.12
    CMP0083 # CMake 3.14
    CMP0093 # CMake 3.15
  )
  if(POLICY ${p})
    cmake_policy(SET ${p} NEW)
  endif()
endforeach()

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

#HDF5
option(USE_SYSTEM_HDF5 "Use system-installed HDF5." ON)
  if(USE_SYSTEM_HDF5)
  find_package(HDF5 NO_MODULE NAMES hdf5 COMPONENTS C shared)
  if(HDF5_FOUND)
    set(HDF5_C_SHARED_LIBRARY hdf5-shared)
#    if(NOT TARGET ${HDF5_C_SHARED_LIBRARY})
#      message(FATAL_ERROR "Could not find hdf5 shared target, please make "
#"sure that HDF5 has ben compiled with shared libraries enabled.")
#    endif()
    set(PDC_EXT_INCLUDE_DEPENDENCIES
        ${PDC_EXT_INCLUDE_DEPENDENCIES}
        ${HDF5_INCLUDE_DIR}
      )
    set(PDC_EXT_LIB_DEPENDENCIES
      ${PDC_EXT_LIB_DEPENDENCIES}
      ${HDF5_C_SHARED_LIBRARY}
    )
  else()
    # Allow for HDF5 autotools builds
    find_package(HDF5 MODULE REQUIRED)
    if(HDF5_FOUND)
      set(PDC_EXT_INCLUDE_DEPENDENCIES
        ${PDC_EXT_INCLUDE_DEPENDENCIES}
        ${HDF5_INCLUDE_DIRS}
      )
      set(PDC_EXT_LIB_DEPENDENCIES
        ${PDC_EXT_LIB_DEPENDENCIES}
        ${HDF5_LIBRARIES}
      )
    else()
      message(FATAL_ERROR "Could not find HDF5, please check HDF5_DIR.")
    endif()
  endif()
endif()

option(USE_SYSTEM_OPENMP "Use system-installed OpenMP." ON)
if(USE_SYSTEM_OPENMP)
  find_package(OpenMP REQUIRED)
  if(OPENMP_FOUND)
      add_definitions(-DENABLE_OPENMP=1)
      set(ENABLE_OPENMP 1)
      set(OPENMP_LIBRARIES "${OpenMP_C_LIBRARIES}")
  else()
      message(FATAL_ERROR "OpenMP not found")
  endif()
endif()


add_definitions(-DENABLE_MPI=1)
add_library(cjson cjson/cJSON.c)


# set(PROGRAMS
#   pdc_ls
#   pdc_import
#   pdc_export
#   )

# foreach(program ${PROGRAMS})
#   add_executable(${program} ${program}.c)
#   target_link_libraries(${program} ${PDC_EXT_LIB_DEPENDENCIES})
#   target_link_libraries(${program} pdc)
#   target_link_libraries(${program} cjson)
#   target_include_directories(${program} PUBLIC ${PDC_INCLUDE_DIR})
# endforeach(program)


# Find LibTIFF
option(USE_LIB_TIFF "Enable LibTiff." ON)
if(USE_LIB_TIFF)
  find_package(TIFF REQUIRED)
  if(TIFF_FOUND)
    set(LLSM_LIB_SOURCE 
      llsm/parallelReadTiff.c
      llsm/csvReader.c
      llsm/pdc_list.c
    )
    # Add the LibTIFF include directory to the include path
    include_directories(${TIFF_INCLUDE_DIRS})
    add_library(llsm_tiff ${LLSM_LIB_SOURCE})
    target_compile_options(llsm_tiff PRIVATE ${OpenMP_C_FLAGS})
    target_link_libraries(llsm_tiff PUBLIC ${OpenMP_C_LIBRARIES})
    target_link_libraries(llsm_tiff PUBLIC ${TIFF_LIBRARIES})
    target_include_directories(llsm_tiff PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/llsm)
    

    add_executable(llsm_importer llsm_importer.c)
    target_link_libraries(llsm_importer ${PDC_EXT_LIB_DEPENDENCIES})
    target_link_libraries(llsm_importer pdc)
    target_link_libraries(llsm_importer cjson)
    target_link_libraries(llsm_importer ${TIFF_LIBRARIES})
    target_link_libraries(llsm_importer llsm_tiff)
    target_include_directories(llsm_importer PUBLIC ${PDC_INCLUDE_DIR})
  else()
    message(WARNING "LibTiff not found, ignore building the executables which requires LibTiff support.")
  endif()
endif()