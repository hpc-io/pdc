set(PDC_COMMON_LIBRARY_NAME pdc_commons)
if(THREADS_HAVE_PTHREAD_ARG)
  set_property(TARGET ${PDC_COMMON_LIBRARY_NAME} PROPERTY COMPILE_OPTIONS "-pthread")
  set_property(TARGET ${PDC_COMMON_LIBRARY_NAME} PROPERTY INTERFACE_COMPILE_OPTIONS "-pthread")
endif()

if(CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(${PDC_COMMON_LIBRARY_NAME} "${CMAKE_THREAD_LIBS_INIT}")
endif()

# Collect all source files
file(GLOB_RECURSE PDC_COMMONS_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*.c)
file(GLOB_RECURSE PDC_COMMONS_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/*.h)


# Get a list of all directories that contain header files
file(GLOB_RECURSE PDC_COMMONS_INCLUDE_LIST LIST_DIRECTORIES true "*.h")

# Remove the /filename.h at the end of each directory
list(TRANSFORM PDC_COMMONS_INCLUDE_LIST REPLACE "/[^/]*$" "")

# Remove duplicates
list(REMOVE_DUPLICATES PDC_COMMONS_INCLUDE_LIST)

message(STATUS "COMMONS PDC_COMMONS_INCLUDE_LIST: ${PDC_COMMONS_INCLUDE_LIST}")

set(PDC_COMMONS_INCLUDE_LIST ${PDC_COMMONS_INCLUDE_LIST} PARENT_SCOPE)

# Create library for the subdirectory
add_library(${PDC_COMMON_LIBRARY_NAME} ${PDC_COMMONS_SOURCES} ${PDC_COMMONS_HEADERS})

# Include subdirectory's header files
target_include_directories(${PDC_COMMON_LIBRARY_NAME} PUBLIC 
  $<BUILD_INTERFACE:${PDC_COMMONS_INCLUDE_LIST}>  # for building
  $<INSTALL_INTERFACE:include/pdc_commons>  # for installed package
)

# Publish header files for client/server programs to use
install(FILES ${PDC_COMMONS_HEADERS} DESTINATION ${PDC_INSTALL_INCLUDE_DIR}/commons)

# Set the installation path for the library
install(TARGETS ${PDC_COMMON_LIBRARY_NAME} DESTINATION ${PDC_INSTALL_LIB_DIR})


