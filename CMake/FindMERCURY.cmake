# FindMERCURY.cmake

# Find the system's MERCURY library
# This will define:
#
# MERCURY_FOUND - System has MERCURY
# MERCURY_INCLUDE_DIRS - The MERCURY include directory
# MERCURY_LIBRARIES - The libraries needed to use MERCURY

find_package(MERCURY QUIET HINTS $ENV{MERCURY_HOME} $ENV{MERCURY_DIR} $ENV{MERCURY_ROOT} $ENV{MERCURYPATH} $ENV{MERCURY_PATH})
if(MERCURY_FOUND)
  message(STATUS "mercury dir = ${MERCURY_DIR}")
  # NOTE: enable the following if you need ${MERCURY_INCLUDE_DIR} in the future
  # NOTE: remember to add MERCURY_HOME to PATH or CMAKE_PREFIX_PATH if you enable the following.
  find_path(MERCURY_INCLUDE_DIR mercury.h HINTS ${MERCURY_DIR})
  find_library(MERCURY_LIBRARY 
    NAMES
    mercury
    mercury_debug
    HINTS ${MERCURY_DIR}
  )

  find_library(MERCURY_NA_LIBRARY
      NAMES
      na 
      na_debug
      HINTS ${MERCURY_DIR}
  )

  find_library(MERCURY_UTIL_LIBRARY
    NAMES
    mercury_util
    HINTS ${MERCURY_DIR}
  )

  set(MERCURY_LIBRARIES ${MERCURY_LIBRARY} ${MERCURY_NA_LIBRARY} ${MERCURY_UTIL_LIBRARY})
  set(MERCURY_INCLUDE_DIRS ${MERCURY_INCLUDE_DIR})
  message(STATUS "mercury include dir = ${MERCURY_INCLUDE_DIRS}")
  message(STATUS "mercury lib = ${MERCURY_LIBRARIES}")
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(MERCURY DEFAULT_MSG MERCURY_LIBRARY MERCURY_INCLUDE_DIR)
else(MERCURY_FOUND)
  set(MERCURY_LIBRARIES "")
endif()

mark_as_advanced(MERCURY_INCLUDE_DIR MERCURY_LIBRARY)
