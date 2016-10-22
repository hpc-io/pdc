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
		pdc_pdc.h
		pdc_pdc_pkg.h
                pdc_prop.h
                pdc_prop_pkg.h
		pdc_cont.h
		pdc_cont_pkg.h)

set(PDC_SOURCES ${PDC_SOURCES}
                pdc.c
                pdc_interface.c 
                pdc_error.c
                pdc_malloc.c
		pdc_pdc.c
                pdc_prop.c
		pdc_cont.c)

add_library(pdc STATIC ${PDC_HEADERS} ${PDC_SOURCES})
