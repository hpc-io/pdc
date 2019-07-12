# - Try to find HDF5
# Once done this will define
#  HDF5_FOUND - System has HDF5
#  HDF5_INCLUDE_DIRS - The HDF5 include directories
#  HDF5_LIBRARIES - The libraries needed to use HDF5

if(NOT ENV{HDF5_DIR})
  set(HDF5_DIR ENV${HDF5_DIR})
endif()

find_path(HDF5_INCLUDE_DIR hdf5.h
  HINTS ${PC_HDF5_INCLUDEDIR} ${PC_HDF5_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include /Users/kmu/Research/PDC/tools/HDF5_Jerome/include ${HDF5_DIR}/include)

find_library(HDF5_LIBRARY NAMES hdf5
  HINTS ${PC_HDF5_LIBDIR} ${PC_HDF5_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib /Users/kmu/Research/PDC/tools/HDF5_Jerome/lib ${HDF5_DIR}/lib)

set(HDF5_INCLUDE_DIRS ${HDF5_INCLUDE_DIR})
set(HDF5_LIBRARIES ${HDF5_LIBRARY})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set PDC_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(HDF5 DEFAULT_MSG
                                  HDF5_INCLUDE_DIR HDF5_LIBRARY)
mark_as_advanced(HDF5_INCLUDE_DIR HDF5_LIBRARY)
