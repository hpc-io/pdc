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

project(PDC_LLSM_EXAM C)

set(LLSM_EXT_INCLUDE_DIRS "")
set(LLSM_EXT_LIBRARIES "")



set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)

find_package(PDC REQUIRED)
if(PDC_FOUND)
    #message(STATUS "PDC include directory: ${PDC_INCLUDE_DIR}")
  set(LLSM_EXT_INCLUDE_DIRS ${PDC_INCLUDE_DIR}
    ${LLSM_EXT_INCLUDE_DIRS}
    )
  set(LLSM_EXT_LIBRARIES pdc ${LLSM_EXT_LIBRARIES})
endif()

option(USE_SYSTEM_MPI "Use system-installed MPI." ON)
if(USE_SYSTEM_MPI)
  find_package(MPI)
  if(MPI_FOUND)
    add_definitions(-DLLSM_ENABLE_MPI=1)
    SET(CMAKE_C_COMPILER ${MPI_C_COMPILER})
    SET(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
    set(LLSM_EXT_INCLUDE_DIRS ${MPI_C_INCLUDE_PATH}
      ${LLSM_EXT_INCLUDE_DIRS}
      )
    set(LLSM_EXT_LIBRARIES ${MPI_C_LIBRARIES} ${LLSM_EXT_LIBRARIES})
  endif()
endif()

option(USE_SYSTEM_OPENMP "Use system-installed OpenMP." ON)
if(USE_SYSTEM_OPENMP)
  find_package(OpenMP REQUIRED)
  if(OPENMP_FOUND)
      add_definitions(-DENABLE_OPENMP=1)
      set(ENABLE_OPENMP 1)
      set(OPENMP_LIBRARIES "${OpenMP_C_LIBRARIES}")
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fopenmp")
  else()
      message(FATAL_ERROR "OpenMP not found")
  endif()
endif()

include_directories(
  ${LLSM_EXT_INCLUDE_DIRS}
)


# Find LibTIFF
option(USE_LIB_TIFF "Enable LibTiff." ON)
if(USE_LIB_TIFF)
  find_package(TIFF REQUIRED)
  if(TIFF_FOUND)
    set(LLSM_LIB_SOURCE 
      llsm_aux/parallelReadTiff.c
      llsm_aux/csvReader.c
      llsm_aux/pdc_list.c
    )
    # Add the LibTIFF include directory to the include path
    include_directories(${TIFF_INCLUDE_DIRS})
    add_library(llsm_tiff ${LLSM_LIB_SOURCE})
    target_compile_options(llsm_tiff PRIVATE ${OpenMP_C_FLAGS})
    target_link_libraries(llsm_tiff PUBLIC ${OpenMP_C_LIBRARIES})
    target_link_libraries(llsm_tiff PUBLIC ${TIFF_LIBRARIES})
    target_include_directories(llsm_tiff PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/llsm)

    add_executable(llsm_importer llsm_importer.c)
    target_link_libraries(llsm_importer ${PDC_EXT_LIB_DEPENDENCIES} pdc ${TIFF_LIBRARIES} llsm_tiff ${LLSM_EXT_LIBRARIES})
    target_include_directories(llsm_importer PUBLIC ${LLSM_EXT_INCLUDE_DIRS})
  else()
    message(WARNING "LibTiff not found, ignore building the executables which requires LibTiff support.")
  endif()
endif()