include_directories(${PROJECT_SOURCE_DIR})

include(ExternalProject)

ExternalProject_Add(project_dablooms
  #DOWNLOAD_DIR "${CMAKE_CURRENT_BINARY_DIR}/dablooms"
  GIT_REPOSITORY git@github.com:bitly/dablooms.git
  UPDATE_COMMAND ""
  PREFIX ${CMAKE_CURRENT_BINARY_DIR}/dablooms
  CONFIGURE_COMMAND "" 
  # Tang: this is a quick fix for cmake to enter project_dablooms instead of project_dablooms-build
  BUILD_COMMAND cd ../project_dablooms && make
  INSTALL_COMMAND cd ../project_dablooms && make install prefix=${CMAKE_CURRENT_BINARY_DIR}/dablooms
)

ExternalProject_Get_Property(project_dablooms install_dir)

include_directories(${install_dir}/include/)

add_library(dablooms SHARED IMPORTED)
set_property(TARGET dablooms PROPERTY IMPORTED_LOCATION ${install_dir}/lib/libdablooms.so)

add_dependencies(dablooms project_dablooms)



###

add_definitions( -DIS_PDC_SERVER=1 )

add_executable(pdc_server.exe pdc_server.c ../pdc_client_server_common.c)
add_dependencies(pdc_server.exe dablooms)
target_link_libraries(pdc_server.exe mercury dablooms)
