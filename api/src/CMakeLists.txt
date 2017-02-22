include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(PDC_HEADERS "")
set(PDC_SOURCES "")

set(PDC_HEADERS ${PDC_HEADERS}
                pdc.h
                pdc_private.h
                pdc_error.h
                pdc_interface.h
                pdc_malloc.h
                pdc_id_pkg.h
                pdc_linkedlist.h
                pdc_prop.h
                pdc_prop_pkg.h
                pdc_cont.h
                pdc_cont_pkg.h
		pdc_obj.h
		pdc_obj_pkg.h
		pdc_dt_conv.h
                pdc_client_connect.h
                pdc_client_server_common.h)

set(PDC_SOURCES ${PDC_SOURCES}
                pdc.c
                pdc_interface.c 
                pdc_error.c
                pdc_malloc.c
                pdc_prop.c
                pdc_cont.c
		pdc_obj.c
		pdc_dt_conv.c
                pdc_client_connect.c
                pdc_client_server_common.c)

add_library(pdc STATIC ${PDC_HEADERS} ${PDC_SOURCES})

target_link_libraries(pdc mercury)

add_subdirectory (server)
