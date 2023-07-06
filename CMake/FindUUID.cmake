# FindUUID.cmake

# Find the system's UUID library
# This will define:
#
# UUID_FOUND - System has UUID
# UUID_INCLUDE_DIRS - The UUID include directory
# UUID_LIBRARIES - The libraries needed to use UUID

find_package(PkgConfig)
pkg_check_modules(UUID REQUIRED uuid)

find_path(UUID_INCLUDE_DIR uuid/uuid.h HINTS ${UUID_INCLUDE_DIRS})
find_library(UUID_LIBRARY NAMES uuid HINTS ${UUID_LIBRARY_DIRS})


include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(UUID DEFAULT_MSG
                                  UUID_INCLUDE_DIR UUID_LIBRARY)

mark_as_advanced(UUID_INCLUDE_DIR UUID_LIBRARY)