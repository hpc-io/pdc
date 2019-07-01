cmake_minimum_required(VERSION 2.8.12.2 FATAL_ERROR)
project(HDF5_VOL_PDC_EXAMPLES C)

#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# HDF5 PDC
find_package(HDF5_VOL_PDC REQUIRED)

#-----------------------------------------------------------------------------
# Define Sources
#-----------------------------------------------------------------------------
set(examples
  vpicio
)

foreach (example ${examples})
  add_executable (h5pdc_${example}
    ${CMAKE_CURRENT_SOURCE_DIR}/h5pdc_${example}.c
  )
  target_link_libraries(h5pdc_${example} hdf5_vol_pdc)
endforeach()
