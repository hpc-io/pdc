# FindUUID.cmake

# Find the system's UUID library
# This will define:
#
# UUID_FOUND - System has UUID
# UUID_INCLUDE_DIRS - The UUID include directory
# UUID_LIBRARIES - The libraries needed to use UUID

find_package(UUID QUIET HINTS $ENV{LIBUUID_HOME} $ENV{LIBUUID_DIR} $ENV{LIBUUID_ROOT} $ENV{LIBUUID_PATH} $ENV{UUID_HOME} $ENV{UUID_DIR} $ENV{UUID_ROOT} $ENV{UUID_PATH})
if (UUID_FOUND)
    find_path(UUID_INCLUDE_DIR uuid/uuid.h HINTS ${UUID_DIR})
    find_library(UUID_LIBRARY NAMES uuid)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(UUID DEFAULT_MSG UUID_LIBRARY UUID_INCLUDE_DIR)
    set(UUID_INCLUDE_DIRS ${UUID_INCLUDE_DIR})
    set(UUID_LIBRARIES ${UUID_LIBRARY})
else(UUID_FOUND)
    set(UUID_LIBRARIES "")
endif (UUID_FOUND)

mark_as_advanced(UUID_INCLUDE_DIR UUID_LIBRARY)