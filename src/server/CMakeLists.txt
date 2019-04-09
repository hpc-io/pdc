find_package(MERCURY REQUIRED)

include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PDC_INCLUDES_BUILD_TIME}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/dablooms
  ${MERCURY_INCLUDE_DIR}
)

add_definitions( -DIS_PDC_SERVER=1 )

add_executable(pdc_server.exe 
               pdc_server.c
               pdc_server_data.c
               pdc_server_metadata.c
               pdc_analysis_server.c
               ../api/pdc_client_server_common.c
               dablooms/dablooms.c
               dablooms/murmur.c
               hash-table.c
)

#
# Note that analysis or transform functions can be run on the server
# and because these are load dynamically via dlopen, missing library
# dependencies will cause the library load to fail.  One suggestion
# might be to use -rpath=dir when linking.  Alternately, setting the
# user LD_LIBRARY_PATH to include additional search paths to user
# libraries can allow the dlopen functionality to work correctly.
#
# Note that this HINT should also be applied to applications themselves
# so that loading additional libraries (in addition to PDC) works
# in a parallel context (for example).

target_link_libraries(pdc_server.exe mercury pdcprof -lm -ldl ${PDC_EXT_LIB_DEPENDENCIES})
