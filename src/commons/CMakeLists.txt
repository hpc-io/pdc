set(PDC_COMMON_LIBRARY_NAME pdc_commons CACHE INTERNAL "")

if(THREADS_HAVE_PTHREAD_ARG)
  set_property(TARGET ${PDC_COMMON_LIBRARY_NAME} PROPERTY COMPILE_OPTIONS "-pthread")
  set_property(TARGET ${PDC_COMMON_LIBRARY_NAME} PROPERTY INTERFACE_COMPILE_OPTIONS "-pthread")
endif()

# Collect all source files
file(GLOB_RECURSE PDC_COMMONS_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*.c)
file(GLOB_RECURSE PDC_COMMONS_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

# Get a list of all directories that contain header files
file(GLOB_RECURSE PDC_COMMONS_INCLUDE_DIRS LIST_DIRECTORIES true "*.h")

# Remove the /filename.h at the end of each directory
list(TRANSFORM PDC_COMMONS_INCLUDE_DIRS REPLACE "/[^/]*$" "")

# Remove duplicates
list(REMOVE_DUPLICATES PDC_COMMONS_INCLUDE_DIRS)

# Create library for the subdirectory
add_library(${PDC_COMMON_LIBRARY_NAME} SHARED ${PDC_COMMONS_SOURCES})
# Include subdirectory's header files
target_include_directories(${PDC_COMMON_LIBRARY_NAME} PUBLIC 
  "$<BUILD_INTERFACE:${PDC_COMMONS_INCLUDE_DIRS}>"  # for building
  $<INSTALL_INTERFACE:${PDC_INSTALL_INCLUDE_INTERFACE}/commons>  # for installed package
)
if(CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(${PDC_COMMON_LIBRARY_NAME} "${CMAKE_THREAD_LIBS_INIT}")
endif()

# Publish header files for client/server programs to use
install(FILES ${PDC_COMMONS_HEADERS} DESTINATION ${PDC_INSTALL_INCLUDE_DIR}/commons)

# Set the installation path for the library
install(TARGETS ${PDC_COMMON_LIBRARY_NAME} DESTINATION ${PDC_INSTALL_LIB_DIR})

# export library target and include directories
set(PDC_COMMON_LIBRARIES ${PDC_COMMON_LIBRARY_NAME} CACHE INTERNAL "PDC common libraries")
set(PDC_COMMON_INCLUDE_DIRS ${PDC_COMMON_INCLUDE_DIRS} CACHE INTERNAL "PDC common include directories")
