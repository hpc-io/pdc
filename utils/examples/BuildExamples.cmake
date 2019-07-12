#------------------------------------------------------------------------------
# Builds the examples as a separate project using a custom target.
# This is included in ../CMakeLists.txt to build examples as a separate
# project.

#------------------------------------------------------------------------------
# Make sure it uses the same build configuration as HDF5 PDC.
if(CMAKE_CONFIGURATION_TYPES)
  set(build_config_arg -C "${CMAKE_CFG_INTDIR}")
else()
  set(build_config_arg)
endif()

set(extra_params)
foreach(flag CMAKE_C_FLAGS_DEBUG
             CMAKE_C_FLAGS_RELEASE
             CMAKE_C_FLAGS_MINSIZEREL
             CMAKE_C_FLAGS_RELWITHDEBINFO)
  if(${${flag}})
    set(extra_params ${extra_params}
      -D${flag}:STRING=${${flag}})
  endif()
endforeach()

set(examples_dependencies
  hdf5_vol_pdc
  )

add_custom_command(
  OUTPUT "${HDF5_VOL_PDC_BINARY_DIR}/Hdf5VolPdcExamples.done"
  COMMAND ${CMAKE_CTEST_COMMAND}
  ARGS ${build_config_arg}
       --build-and-test
       ${HDF5_VOL_PDC_SOURCE_DIR}/examples
       ${HDF5_VOL_PDC_BINARY_DIR}/examples
       --build-noclean
       --build-two-config
       --build-project Hdf5VolPdcExamples
       --build-generator ${CMAKE_GENERATOR}
       --build-makeprogram ${CMAKE_MAKE_PROGRAM}
       --build-options -DHDF5_VOL_PDC_DIR:PATH=${HDF5_VOL_PDC_BINARY_DIR}
                       -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
                       -DCMAKE_C_COMPILER:FILEPATH=${CMAKE_C_COMPILER}
                       -DCMAKE_C_FLAGS:STRING=${CMAKE_C_FLAGS}
                       -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:PATH=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
                       -DCMAKE_RUNTIME_OUTPUT_DIRECTORY:PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
                       ${extra_params}
                       --no-warn-unused-cli
  COMMAND ${CMAKE_COMMAND} -E touch
          "${HDF5_VOL_PDC_BINARY_DIR}/Hdf5VolPdcExamples.done"
  COMMENT "Build examples as a separate project"
  DEPENDS ${examples_dependencies}
)

# Add custom target to ensure that the examples get built.
add_custom_target(examples ALL DEPENDS
  "${HDF5_VOL_PDC_BINARY_DIR}/Hdf5VolPdcExamples.done")
