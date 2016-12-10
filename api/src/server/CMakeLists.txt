include_directories(${PROJECT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/dablooms)

add_definitions( -DIS_PDC_SERVER=1 )

add_executable(pdc_server.exe pdc_server.c ../pdc_client_server_common.c dablooms/dablooms.c dablooms/murmur.c )
target_link_libraries(pdc_server.exe mercury)
