# - Try to find HDF5_VOL_PDC
# Once done this will define
#  HDF5_VOL_PDC_FOUND - System has HDF5_VOL_PDC
#  HDF5_VOL_PDC_INCLUDE_DIRS - The HDF5_VOL_PDC include directories
#  HDF5_VOL_PDC_LIBRARIES - The libraries needed to use HDF5_VOL_PDC

if(NOT ENV{HDF5_VOL_PDC_DIR})
  set(HDF5_VOL_PDC_DIR ENV${HDF5_VOL_PDC_DIR})
endif()

find_path(HDF5_VOL_PDC_INCLUDE_DIR H5VLpdc.h
  HINTS ${PC_HDF5_VOL_PDC_INCLUDEDIR} ${PC_HDF5_VOL_PDC_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include ${CMAKE_CURRENT_SOURCE_DIR}/../src ${HDF5_VOL_PDC_DIR}/include)

find_library(HDF5_VOL_PDC_LIBRARY NAMES hdf5_vol_pdc
  HINTS ${PC_HDF5_LIBDIR} ${PC_HDF5_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib ${CMAKE_CURRENT_BINARY_DIR}/../bin ${HDF5_VOL_PDC_DIR}/lib)

set(HDF5_VOL_PDC_INCLUDE_DIRS ${HDF5_VOL_PDC_INCLUDE_DIR})
set(HDF5_VOL_PDC_LIBRARIES ${HDF5_VOL_PDC_LIBRARY})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set HDF5_VOL_PDC_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(HDF5_VOL_PDC DEFAULT_MSG
                                  HDF5_VOL_PDC_INCLUDE_DIR HDF5_VOL_PDC_LIBRARY)
mark_as_advanced(HDF5_VOL_PDC_INCLUDE_DIR HDF5_VOL_PDC_LIBRARY)
