include_directories(${PROJECT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/dablooms)
#
#include(ExternalProject)
#
#ExternalProject_Add(project_skiplist
#  DOWNLOAD_DIR "${CMAKE_CURRENT_BINARY_DIR}/skiplist"
#  URL https://svwh.dl.sourceforge.net/project/myskl/myskl/0.2.1/myskl-0.2.1.src.tar.gz 
#  UPDATE_COMMAND ""
#  PREFIX ${CMAKE_CURRENT_BINARY_DIR}/skiplist
#  BUILD_IN_SOURCE 1
#  CONFIGURE_COMMAND  ./configure --enable-thread-safety-full --prefix=${CMAKE_CURRENT_BINARY_DIR}/build 
#  BUILD_COMMAND      make
#  INSTALL_COMMAND    make install 
#)
#
#ExternalProject_Get_Property(project_skiplist install_dir)
#
#include_directories(${install_dir}/include/)
#
#add_library(skiplist SHARED IMPORTED)
#set_property(TARGET skiplist PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/build/lib/libmyskl.so)
#
#add_dependencies(skiplist project_skiplist)

###


add_definitions( -DIS_PDC_SERVER=1 )

add_executable(pdc_server.exe pdc_server.c ../pdc_client_server_common.c dablooms/dablooms.c dablooms/murmur.c )
target_link_libraries(pdc_server.exe mercury)
#target_link_libraries(pdc_server.exe skiplist)
