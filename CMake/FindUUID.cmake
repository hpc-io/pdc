# On Mac OS X the uuid functions are in the System library

if(APPLE)
  set(UUID_LIBRARY_VAR System)
else()
  set(UUID_LIBRARY_VAR uuid)
endif()

find_library(UUID_LIBRARY
  NAMES ${UUID_LIBRARY_VAR}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib
)

find_path(UUID_INCLUDE_DIR uuid/uuid.h
  HINTS ${PC_UUID_INCLUDEDIR} ${PC_UUID_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include
)

if (UUID_LIBRARY AND UUID_INCLUDE_DIR)
  set(UUID_LIBRARIES ${UUID_LIBRARY})
  set(UUID_FOUND "TRUE")
else ()
  set(UUID_FOUND "FALSE")
endif ()

if (UUID_FOUND)
   if (NOT UUID_FIND_QUIETLY)
      message(STATUS "Found UUID: ${UUID_LIBRARIES}")
   endif ()
else ()
   if (UUID_FIND_REQUIRED)
      message( "library: ${UUID_LIBRARY}" )
      message( "include: ${UUID_INCLUDE_DIR}" )
      message(FATAL_ERROR "Could not find UUID library")
   endif ()
endif ()

mark_as_advanced(
  UUID_LIBRARY
  UUID_INCLUDE_DIR
)
