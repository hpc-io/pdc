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

project(METADATA_JSON_LOADER C)

set(JMD_LDR_EXT_INCLUDE_DIRS "")
set(JMD_LDR_EXT_LIBRARIES "")

set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
# set(JMD_DEBUG ON CACHE BOOL "Enable debug mode.")
option(JMD_DEBUG_MODE "Enable debug mode." ON)
if(JMD_DEBUG_MODE)
  add_definitions(-DJMD_DEBUG=1)
endif()

option(USE_SYSTEM_MPI "Use system-installed MPI." ON)
if(USE_SYSTEM_MPI)
  find_package(MPI)
  if(MPI_FOUND)
    add_definitions(-DJMD_LDR_ENABLE_MPI=1)
    SET(CMAKE_C_COMPILER ${MPI_C_COMPILER})
    SET(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
    set(JMD_LDR_EXT_INCLUDE_DIRS ${MPI_C_INCLUDE_PATH}
      ${JMD_LDR_EXT_INCLUDE_DIRS}
      )
    set(JMD_LDR_EXT_LIBRARIES ${MPI_C_LIBRARIES} ${JMD_LDR_EXT_LIBRARIES})
  endif()
endif()

find_package(PDC REQUIRED)
if(PDC_FOUND)
    #message(STATUS "PDC include directory: ${PDC_INCLUDE_DIR}")
  set(JMD_LDR_EXT_INCLUDE_DIRS ${PDC_INCLUDE_DIR}
    ${JMD_LDR_EXT_INCLUDE_DIRS}
    )
  set(JMD_LDR_EXT_LIBRARIES pdc ${JMD_LDR_EXT_LIBRARIES})
endif()


add_library(cjson cjson/cJSON.c)

add_executable(metadata_json_loader 
            metadata_json_loader.c 
            metadata_json_processor.h 
            metadata_json_printer.c 
            metadata_json_printer.h 
            metadata_json_importer.c 
            metadata_json_importer.h)
target_link_libraries(metadata_json_loader ${PDC_EXT_LIB_DEPENDENCIES} pdc cjson ${JMD_LDR_EXT_LIBRARIES})
target_include_directories(metadata_json_loader PUBLIC ${PDC_EXT_INCLUDE_DEPENDENCIES} ${JMD_LDR_EXT_INCLUDE_DIRS})