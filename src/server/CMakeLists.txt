find_package(MERCURY REQUIRED)

if(PDC_ENABLE_FASTBIT)
    add_definitions(-DENABLE_FASTBIT=1)
    find_path(FASTBIT_INCLUDE_DIR fastbit/iapi.h)
    find_library(FASTBIT_LIBRARY fastbit $ENV{HOME}/cori/fastbit-2.0.3/install)
endif()

include_directories(
  ${PDC_INCLUDES_BUILD_TIME}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PDC_SOURCE_DIR}/src/server/include
  ${PDC_SOURCE_DIR}/src/server/pdc_server_region
  ${PDC_SOURCE_DIR}/src/server/pdc_server_region/include
  ${PDC_SOURCE_DIR}/src/server/pdc_server_analysis/include
  ${PDC_SOURCE_DIR}/src/server/dablooms
  ${PDC_SOURCE_DIR}/src/api/include
  ${PDC_SOURCE_DIR}/src/api/pdc_obj/include
  ${PDC_SOURCE_DIR}/src/api/pdc_region/include
  ${PDC_SOURCE_DIR}/src/api/pdc_query/include
  ${PDC_SOURCE_DIR}/src/api/pdc_transform/include
  ${PDC_SOURCE_DIR}/src/api/pdc_analysis/include
  ${PDC_SOURCE_DIR}/src/api/profiling/include
  ${PDC_SOURCE_DIR}/src/utils/include
  ${MERCURY_INCLUDE_DIR}
  ${FASTBIT_INCLUDE_DIR}
)

add_definitions( -DIS_PDC_SERVER=1 )
add_definitions( -DPDC_LUSTRE_TOTAL_OST=${PDC_LUSTRE_TOTAL_OST} )
if(PDC_ENABLE_TIMING)
    add_definitions(-DPDC_TIMING=1)
endif()

add_executable(pdc_server.exe
               pdc_server.c
               pdc_server_metadata.c
               pdc_client_server_common.c
               dablooms/pdc_dablooms.c
               dablooms/pdc_murmur.c
               pdc_hash-table.c
               ${PDC_SOURCE_DIR}/src/server/pdc_server_analysis/pdc_server_analysis.c
               ${PDC_SOURCE_DIR}/src/server/pdc_server_region/pdc_server_data.c
               ${PDC_SOURCE_DIR}/src/server/pdc_server_region/pdc_server_region_cache.c
               ${PDC_SOURCE_DIR}/src/server/pdc_server_region/pdc_server_region_transfer.c
               ${PDC_SOURCE_DIR}/src/server/pdc_server_region/pdc_server_region_transfer_metadata_query.c
               ${PDC_SOURCE_DIR}/src/utils/pdc_region_utils.c
               ${PDC_SOURCE_DIR}/src/utils/pdc_timing.c
               ${PDC_SOURCE_DIR}/src/api/pdc_analysis/pdc_analysis_common.c
               ${PDC_SOURCE_DIR}/src/api/pdc_transform/pdc_transforms_common.c
               ${PDC_SOURCE_DIR}/src/api/pdc_analysis/pdc_hist_pkg.c
)

if(NOT ${PDC_INSTALL_BIN_DIR} MATCHES ${PROJECT_BINARY_DIR}/bin)
install(
  TARGETS
    pdc_server.exe
  DESTINATION ${PDC_INSTALL_BIN_DIR}
)
endif()

if(PDC_ENABLE_FASTBIT)
    message(STATUS "Enabled fastbit")
    target_link_libraries(pdc_server.exe mercury pdcprof -lm -ldl ${PDC_EXT_LIB_DEPENDENCIES} ${FASTBIT_LIBRARY}/libfastbit.so)
else()
    target_link_libraries(pdc_server.exe mercury pdcprof -lm -ldl ${PDC_EXT_LIB_DEPENDENCIES})
endif()


