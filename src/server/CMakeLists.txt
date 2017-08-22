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
               ../api/pdc_client_server_common.c 
               dablooms/dablooms.c 
               dablooms/murmur.c 
)
target_link_libraries(pdc_server.exe mercury pdcprof -lm -ldl)
