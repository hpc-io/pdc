include_directories(${PROJECT_SOURCE_DIR})

add_executable(pdc_server.exe pdc_server.c ../pdc_client_server_common.c)
target_link_libraries(pdc_server.exe mercury)
