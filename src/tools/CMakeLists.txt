option(JMD_DEBUG_MODE "Enable debug mode." OFF)
if(JMD_DEBUG_MODE)
  add_definitions(-DJMD_DEBUG=1)
endif()

set(TOOLS_EXT_INCLUDE "")
set(TOOLS_EXT_LIB "")

# *************************************************
# *  MERCURY
# *************************************************
find_package(MERCURY REQUIRED)
if(MERCURY_FOUND)
  set(TOOLS_EXT_INCLUDE ${MERCURY_INCLUDE_DIRS}
    ${TOOLS_EXT_INCLUDE}
    )
  set(TOOLS_EXT_LIB ${MERCURY_LIBRARIES} ${TOOLS_EXT_LIB})
endif()

#HDF5
find_package(HDF5 MODULE)
  if(NOT HDF5_FOUND)
    message(STATUS "Could not find HDF5, fallback to NO_MODULE mode.")
    find_package(HDF5 NO_MODULE NAMES hdf5 COMPONENTS C shared)
    if(NOT HDF5_FOUND)
      message(FATAL_ERROR "Could not find HDF5, please check HDF5_DIR or make sure that HDF5 has ben compiled with shared libraries enabled.")
    else()
      set(HDF5_LIBRARIES ${HDF5_LIBRARIES} hdf5-shared)
      set(HDF5_INCLUDE_DIRS ${HDF5_INCLUDE_DIRS} ${HDF5_INCLUDE_DIR})
    endif()
  endif()

if (HDF5_FOUND)
  set(TOOLS_EXT_INCLUDE
    ${TOOLS_EXT_INCLUDE}
    ${HDF5_INCLUDE_DIRS}
  )
  set(TOOLS_EXT_LIB
    ${TOOLS_EXT_LIB}
    ${HDF5_LIBRARIES}
  )
endif()

# option(USE_SYSTEM_HDF5 "Use system-installed HDF5." ON)
# if(USE_SYSTEM_HDF5)
#   find_package(HDF5 NO_MODULE NAMES hdf5 COMPONENTS C shared)
#   if(HDF5_FOUND)
#     set(HDF5_C_SHARED_LIBRARY hdf5-shared)
#   #    if(NOT TARGET ${HDF5_C_SHARED_LIBRARY})
#   #      message(FATAL_ERROR "Could not find hdf5 shared target, please make "
#   #"sure that HDF5 has ben compiled with shared libraries enabled.")
#   #    endif()
#     set(TOOLS_EXT_INCLUDE
#         ${TOOLS_EXT_INCLUDE}
#         ${HDF5_INCLUDE_DIR}
#       )
#     set(TOOLS_EXT_LIB
#       ${TOOLS_EXT_LIB}
#       ${HDF5_C_SHARED_LIBRARY}
#     )
#   endif()
# else()
#   # Allow for HDF5 autotools builds
#   find_package(HDF5 MODULE REQUIRED)
#   if(HDF5_FOUND)
#     set(TOOLS_EXT_INCLUDE
#       ${TOOLS_EXT_INCLUDE}
#       ${HDF5_INCLUDE_DIRS}
#     )
#     set(TOOLS_EXT_LIB
#       ${TOOLS_EXT_LIB}
#       ${HDF5_LIBRARIES}
#     )
#   else()
#     message(FATAL_ERROR "Could not find HDF5, please check HDF5_DIR.")
#   endif()
# endif()

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PDC_INCLUDES_BUILD_TIME}
  ${PROJECT_BINARY_DIR}
  ${PDC_SOURCE_DIR}
  ${PDC_SOURCE_DIR}/src/client_api/include
  $ENV{HOME}/Sandbox/c-blosc/blosc
  $ENV{HOME}/include
  ${TOOLS_EXT_INCLUDE}
)


set(PROGRAMS
  pdc_import
  pdc_export
  pdc_ls
  )

add_library(cjson cjson/cJSON.c)

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc cjson ${TOOLS_EXT_LIB})
endforeach(program)


add_executable(metadata_json_loader 
            metadata_json_loader.c 
            meta_json/metadata_json_processor.h 
            meta_json/metadata_json_printer.c 
            meta_json/metadata_json_printer.h 
            meta_json/metadata_json_importer.c 
            meta_json/metadata_json_importer.h
            fs/fs_ops.c
            fs/fs_ops.h)
target_link_libraries(metadata_json_loader ${PDC_EXT_LIB_DEPENDENCIES} pdc cjson ${TOOLS_EXT_LIB})
target_include_directories(metadata_json_loader PUBLIC ${PDC_EXT_INCLUDE_DEPENDENCIES})