# FindMercury.cmake

# Find the system's Mercury library
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
  find_library(MERCURY_LIBRARY mercury HINTS ${MERCURY_DIR})
  message(STATUS "mercury include dir = ${MERCURY_INCLUDE_DIR}")
  message(STATUS "mercury lib = ${MERCURY_LIBRARY}")
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(MERCURY DEFAULT_MSG MERCURY_LIBRARY MERCURY_INCLUDE_DIR)
  set(MERCURY_LIBRARIES ${MERCURY_LIBRARY})
  set(MERCURY_INCLUDE_DIRS ${MERCURY_INCLUDE_DIR})
else(MERCURY_FOUND)
  set(MERCURY_LIBRARIES "")
endif()

mark_as_advanced(MERCURY_INCLUDE_DIR MERCURY_LIBRARY)