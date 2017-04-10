include_directories(${PROJECT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/dablooms)

#include(ExternalProject)
#
#ExternalProject_Add(project_skiplist
#  DOWNLOAD_DIR "${CMAKE_CURRENT_BINARY_DIR}/skiplist"
#  GIT_REPOSITORY git@github.com:houjun/myskl.git
#  BUILD_IN_SOURCE 1
#  CONFIGURE_COMMAND  ./configure --enable-thread-safety-full --prefix=${CMAKE_INSTALL_PREFIX}/skiplist/
#  UPDATE_COMMAND     ""
#  BUILD_COMMAND      make
#  INSTALL_COMMAND    make install 
#)
#
#include_directories(${CMAKE_INSTALL_PREFIX}/skiplist/include/)
#add_library(skiplist SHARED IMPORTED)
#
#set_property(TARGET skiplist PROPERTY IMPORTED_LOCATION ${CMAKE_INSTALL_PREFIX}/skiplist/lib/libmyskl.so)
#
#add_dependencies(skiplist project_skiplist)

###


add_definitions( -DIS_PDC_SERVER=1 )

add_executable(pdc_server.exe pdc_server.c ../pdc_client_server_common.c dablooms/dablooms.c dablooms/murmur.c )
target_link_libraries(pdc_server.exe mercury -lm)
#target_link_libraries(pdc_server.exe skiplist)
