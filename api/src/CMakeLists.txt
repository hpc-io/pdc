include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(PDC_HEADERS "")
set(PDC_SOURCES "")

set(PDC_HEADERS ${PDC_HEADERS}
                pdc.h
                pdc_private.h
                pdc_error.h
                pdc_interface.h
                pdc_malloc.h
                )

set(PDC_SOURCES ${PDC_SOURCES}
                pdc.c
                pdc_interface.c 
                pdc_error.c
                pdc_malloc.c)

add_library(pdc STATIC ${PDC_HEADERS} ${PDC_SOURCES})
