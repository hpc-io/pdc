include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(PDC_HEADERS "")
set(PDC_SOURCES "")

set(PDC_HEADERS ${PDC_HEADERS}
                pdc.h
                pdc_private.h)

set(PDC_SOURCES ${PDC_SOURCES}
                pdc.c)

add_library(pdc STATIC ${PDC_HEADERS} ${PDC_SOURCES})
