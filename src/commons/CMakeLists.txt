
#------------------------------------------------------------------------------
# Setup SRC path for each package
#------------------------------------------------------------------------------
set(COLLECTIONS_DIR     ${CMAKE_CURRENT_SOURCE_DIR}/collections)
set(INDEX_DIR           ${CMAKE_CURRENT_SOURCE_DIR}/index)
set(FILE_DIR            ${CMAKE_CURRENT_SOURCE_DIR}/file)
set(RPC_DIR             ${CMAKE_CURRENT_SOURCE_DIR}/rpc)
set(SERDE_DIR           ${CMAKE_CURRENT_SOURCE_DIR}/serde)
set(UTILS_DIR           ${CMAKE_CURRENT_SOURCE_DIR}/utils)

#------------------------------------------------------------------------------
# Collect all source files
#------------------------------------------------------------------------------
file(GLOB_RECURSE COLLECTIONS_DIR_SOURCES ${COLLECTIONS_DIR}/*.c)
file(GLOB_RECURSE INDEX_DIR_SOURCES ${INDEX_DIR}/*.c)
file(GLOB_RECURSE FILE_DIR_SOURCES ${FILE_DIR}/*.c)
file(GLOB_RECURSE RPC_DIR_SOURCES ${RPC_DIR}/*.c)
file(GLOB_RECURSE SERDE_DIR_SOURCES ${SERDE_DIR}/*.c)
file(GLOB_RECURSE UTILS_DIR_SOURCES ${UTILS_DIR}/*.c)


#------------------------------------------------------------------------------
# Collect all header files
#------------------------------------------------------------------------------
file(GLOB_RECURSE COLLECTIONS_DIR_HEADERS ${COLLECTIONS_DIR}/*.h)
file(GLOB_RECURSE INDEX_DIR_HEADERS ${INDEX_DIR}/*.h)
file(GLOB_RECURSE FILE_DIR_HEADERS ${FILE_DIR}/*.h)
file(GLOB_RECURSE RPC_DIR_HEADERS ${RPC_DIR}/*.h)
file(GLOB_RECURSE SERDE_DIR_HEADERS ${SERDE_DIR}/*.h)
file(GLOB_RECURSE UTILS_DIR_HEADERS ${UTILS_DIR}/*.h)


set(PDC_COMMONS_INCLUDE_LIST
    ${COLLECTIONS_DIR}
    ${INDEX_DIR}
    ${FILE_DIR}
    ${RPC_DIR}
    ${SERDE_DIR}
    ${UTILS_DIR}
)

# Function to build libraries in subdirectories
function(build_subdirectory_libraries SUBDIR SOURCES HEADERS)
  # Set the library name
  set(LIBRARY_NAME pdc_commons_${SUBDIR})

  # Create library for the subdirectory
  add_library(${LIBRARY_NAME} ${SOURCES})

  # Include subdirectory's header files
  target_include_directories(${LIBRARY_NAME} PUBLIC ${PDC_COMMONS_INCLUDE_LIST})

  # Publish header files for client/server programs to use
  install(FILES ${HEADERS} DESTINATION ${PDC_INSTALL_INCLUDE_DIR}/commons/${SUBDIR})

  # Set the installation path for the library
  install(TARGETS ${LIBRARY_NAME} DESTINATION ${PDC_INSTALL_LIB_DIR})
endfunction()


# Build libraries for each subdirectory
build_subdirectory_libraries(utils "${UTILS_DIR_SOURCES}" "${UTILS_DIR_HEADERS}")
build_subdirectory_libraries(collections "${COLLECTIONS_DIR_SOURCES}" "${COLLECTIONS_DIR_HEADERS}")
build_subdirectory_libraries(serde "${SERDE_DIR_SOURCES}" "${SERDE_DIR_HEADERS}")
build_subdirectory_libraries(file "${FILE_DIR_SOURCES}" "${FILE_DIR_HEADERS}")
build_subdirectory_libraries(rpc "${RPC_DIR_SOURCES}" "${RPC_DIR_HEADERS}")
build_subdirectory_libraries(index "${INDEX_DIR_SOURCES}" "${INDEX_DIR_HEADERS}")