#------------------------------------------------------------------------------
# PDC Commons
#------------------------------------------------------------------------------

set(PDC_COMMON_LIBRARY_NAME pdc_commons CACHE INTERNAL "")
set(PDC_COMMON_SERVER_LIB pdc_commons_server CACHE INTERNAL "")
set(PDC_COMMON_CLIENT_LIB pdc_commons_client CACHE INTERNAL "")
#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------

# #set(PDC_EXT_LIB_DEPENDENCIES ${PDC_COMMON_LIBRARY_NAME} ${PDC_EXT_LIB_DEPENDENCIES})
# set(PDC_EXT_INCLUDE_DEPENDENCIES ${CMAKE_CURRENT_SOURCE_DIR}/profiling)
# set(PDC_EXPORTED_LIBS pdcprof)

if(THREADS_HAVE_PTHREAD_ARG)
  set_property(TARGET ${PDC_COMMON_LIBRARY_NAME} PROPERTY COMPILE_OPTIONS "-pthread")
  set_property(TARGET ${PDC_COMMON_LIBRARY_NAME} PROPERTY INTERFACE_COMPILE_OPTIONS "-pthread")
endif()

# Math library

find_library(MATH_LIBRARY m)
if(MATH_LIBRARY)
  set(PDC_EXT_LIB_DEPENDENCIES ${MATH_LIBRARY} ${PDC_EXT_LIB_DEPENDENCIES}) 
endif()

# Mercury
find_package(MERCURY REQUIRED)
if(MERCURY_FOUND)
  set(PDC_EXT_INCLUDE_DEPENDENCIES ${MERCURY_INCLUDE_DIRS}
    ${PDC_EXT_INCLUDE_DEPENDENCIES}
    )
  set(PDC_EXT_LIB_DEPENDENCIES ${MERCURY_LIBRARIES} ${PDC_EXT_LIB_DEPENDENCIES})
endif()

include_directories(${PDC_EXT_INCLUDE_DEPENDENCIES})

#------------------------------------------------------------------------------
# Include directories
#------------------------------------------------------------------------------

# Get a list of all directories that contain header files
file(GLOB_RECURSE LOCAL_INCLUDE_DIRS "*.h")

# Remove specified directories from the list
# list(FILTER LOCAL_INCLUDE_DIRS EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/comm")

# Remove the /filename.h at the end of each directory
list(TRANSFORM LOCAL_INCLUDE_DIRS REPLACE "/[^/]*$" "")

# Remove duplicates
list(REMOVE_DUPLICATES LOCAL_INCLUDE_DIRS)

set(PDC_COMMONS_INCLUDE_DIRS
  ${LOCAL_INCLUDE_DIRS}
  ${PDC_INCLUDES_BUILD_TIME}
  ${PROJECT_BINARY_DIR}
  ${PDC_SOURCE_DIR}
  ${PDC_EXT_INCLUDE_DEPENDENCIES}
)

include_directories(
  ${PDC_COMMONS_INCLUDE_DIRS}
)

message(STATUS "===MERCURY_INCLUDE_DIRS: ${MERCURY_INCLUDE_DIRS}")
message(STATUS "===PDC_EXT_INCLUDE_DEPENDENCIES: ${PDC_EXT_INCLUDE_DEPENDENCIES}")
message(STATUS "===PDC_COMMONS_INCLUDE_DIRS: ${PDC_COMMONS_INCLUDE_DIRS}")

install(
  FILES
    ${CMAKE_BINARY_DIR}/pdc_config.h
    ${PDC_SOURCE_DIR}/src/commons/utils/include/pdc_id_pkg.h
    ${PDC_SOURCE_DIR}/src/commons/utils/include/pdc_malloc.h
    ${PDC_SOURCE_DIR}/src/commons/utils/include/pdc_linkedlist.h
  DESTINATION
    ${PDC_INSTALL_INCLUDE_DIR}
  COMPONENT
    headers
)

#------------------------------------------------------------------------------
# Configure module header files
#------------------------------------------------------------------------------
# Set unique vars used in the autogenerated config file (symbol import/export)
if(BUILD_SHARED_LIBS)
  set(PDC_BUILD_SHARED_LIBS 1)
  set(PDC_LIBTYPE SHARED)
else()
  set(PDC_BUILD_SHARED_LIBS 0)
  set(PDC_LIBTYPE STATIC)
endif()

if(PDC_ENABLE_TIMING)
    add_definitions(-DPDC_TIMING=1)
endif()

#------------------------------------------------------------------------------
# Set sources
#------------------------------------------------------------------------------

# Collect all source files
file(GLOB_RECURSE PDC_COMMONS_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*.c)
file(GLOB_RECURSE PDC_COMMONS_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/*.h)
list(FILTER PDC_COMMONS_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/serde/.*")
list(FILTER PDC_COMMONS_HEADERS EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/serde/.*")
list(FILTER PDC_COMMONS_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/.*_test.c")

message(STATUS "===PDC_COMMONS_SOURCES: ${PDC_COMMONS_SOURCES}")

file(GLOB_RECURSE PDC_COMMONS_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*_test.c)
list(FILTER PDC_COMMONS_TEST_SRC EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/serde/.*")
message(STATUS "===PDC_COMMONS_TEST_SRC: ${PDC_COMMONS_TEST_SRC}")

#------------------------------------------------------------------------------
# Libraries
#------------------------------------------------------------------------------
# PDC COMMONS


add_library(${PDC_COMMON_LIBRARY_NAME} ${PDC_LIBTYPE} ${PDC_COMMONS_SOURCES} ${PDC_COMMONS_HEADERS})

target_include_directories(${PDC_COMMON_LIBRARY_NAME}
  PUBLIC  "$<BUILD_INTERFACE:${PDC_COMMONS_INCLUDE_DIRS}>"
          $<INSTALL_INTERFACE:${PDC_INSTALL_INCLUDE_INTERFACE}>
)

target_link_libraries(${PDC_COMMON_LIBRARY_NAME} INTERFACE
  ${PDC_EXT_LIB_DEPENDENCIES}
)

set(PDC_EXPORTED_LIBS ${PDC_COMMON_LIBRARY_NAME} ${PDC_EXPORTED_LIBS})

#------------------------------------------------------------------------------
# Tests
#------------------------------------------------------------------------------

# for each test source in $PDC_COMMONS_TEST_SRC, create an executable and link with PDC_COMMON_LIBRARY_NAME
foreach(_test_src ${PDC_COMMONS_TEST_SRC})
  get_filename_component(_test_name ${_test_src} NAME_WE)
  add_executable(${_test_name} ${_test_src})
  message(STATUS "===_test_name: ${_test_name}")
  target_link_libraries(${_test_name} ${PDC_COMMON_LIBRARY_NAME})
  # add_test(${_test_name} ${_test_name})
endforeach()

#-----------------------------------------------------------------------------
# Specify project header files to be installed
#-----------------------------------------------------------------------------


set(PDC_PUBLIC_HEADERS "")
set(PUBLIC_HEADER_DIR_LIST 
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/generic/include
  ${CMAKE_CURRENT_SOURCE_DIR}/profiling/include
)

foreach(_header_dir ${PUBLIC_HEADER_DIR_LIST})
  file(GLOB_RECURSE _dir_headers ${_header_dir}/*.h)
  list(APPEND PDC_PUBLIC_HEADERS ${_dir_headers})
endforeach()

set(PDC_COMMONS_HEADERS
  ${PDC_PUBLIC_HEADERS}
  ${PROJECT_BINARY_DIR}/pdc_config_sys.h
  ${PROJECT_BINARY_DIR}/pdc_config.h
  )

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  FILES
    ${PDC_COMMONS_HEADERS}
  DESTINATION
    ${PDC_INSTALL_INCLUDE_DIR}
  COMPONENT
    headers
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------

install(
  TARGETS
    ${PDC_COMMON_LIBRARY_NAME} 
  EXPORT
    ${PDC_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${PDC_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${PDC_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${PDC_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
  EXPORT
    ${PDC_EXPORTED_TARGETS}
  DESTINATION
    ${PDC_INSTALL_DATA_DIR}/cmake/pdc
  FILE
    ${PDC_EXPORTED_TARGETS}.cmake
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
if(NOT PDC_EXTERNALLY_CONFIGURED)
EXPORT (
  TARGETS
    ${PDC_EXPORTED_LIBS}
  FILE
    ${PDC_EXPORTED_TARGETS}.cmake
)
endif()

#------------------------------------------------------------------------------
# Set variables for parent scope
#------------------------------------------------------------------------------
# Used by config.cmake.build.in and Testing
set(PDC_INCLUDES_BUILD_TIME
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PDC_EXT_INCLUDE_DEPENDENCIES}
  ${PDC_COMMONS_INCLUDE_DIRS}
  PARENT_SCOPE
)

# Used by config.cmake.install.in
set(PDC_INCLUDES_INSTALL_TIME
  ${PDC_COMMONS_INCLUDE_DIRS}
  ${PDC_INSTALL_INCLUDE_DIR}
  ${PDC_EXT_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)

set(PDC_COMMONS_LIBRARIES ${PDC_COMMON_LIBRARY_NAME} PARENT_SCOPE)