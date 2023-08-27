# FindJULIA.cmake

# Check if JULIA_HOME is set
if(NOT DEFINED ENV{JULIA_HOME})
    message(FATAL_ERROR "JULIA_HOME environment variable is not set. Please set it to the JULIA installation directory.")
endif()

# Set JULIA installation directory
set(JULIA_INSTALL_DIR $ENV{JULIA_HOME})

# Find JULIA library
find_library(JULIA_LIBRARY
    NAMES julia
    PATHS ${JULIA_INSTALL_DIR}/lib
    NO_DEFAULT_PATH
    NO_CMAKE_FIND_ROOT_PATH
)

# Find JULIA include directory
find_path(JULIA_INCLUDE_DIR
    NAMES julia.h
    PATHS ${JULIA_INSTALL_DIR}/include/julia
    NO_DEFAULT_PATH
    NO_CMAKE_FIND_ROOT_PATH
)

# Check if JULIA library and include directory are found
if(NOT JULIA_LIBRARY)
    message(FATAL_ERROR "JULIA library not found. Please set the JULIA_HOME environment variable to the correct JULIA installation directory.")
endif()

if(NOT JULIA_INCLUDE_DIR)
    message(FATAL_ERROR "JULIA include directory not found. Please set the JULIA_HOME environment variable to the correct JULIA installation directory.")
endif()

# Set JULIA_FOUND variable
set(JULIA_FOUND TRUE CACHE BOOL "JULIA found")

# Set JULIA_INCLUDE_DIRS and JULIA_LIBRARIES variables
set(JULIA_INCLUDE_DIRS ${JULIA_INCLUDE_DIR})
set(JULIA_LIBRARIES ${JULIA_LIBRARY})

# Report JULIA library and include directory
message(STATUS "JULIA library found: ${JULIA_LIBRARY}")
message(STATUS "JULIA include directory found: ${JULIA_INCLUDE_DIR}")