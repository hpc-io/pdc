find_package(PkgConfig)
pkg_check_modules(FYAML fyaml)

find_path(FYAML_INCLUDE_DIR libfyaml.h)
find_library(FYAML_LIBRARY NAMES fyaml)

set(FYAML_INCLUDE_DIRS ${FYAML_INCLUDE_DIR})
set(FYAML_LIBRARIES ${FYAML_LIBRARY})

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(
  FYAML DEFAULT_MSG
  FYAML_INCLUDE_DIR FYAML_LIBRARY
)

mark_as_advanced(FYAML_INCLUDE_DIR FYAML_LIBRARY)
