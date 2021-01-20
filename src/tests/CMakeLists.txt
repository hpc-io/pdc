include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PDC_INCLUDES_BUILD_TIME}
  $ENV{HOME}/Sandbox/c-blosc/blosc
  $ENV{HOME}/include
)

set(PROGRAMS
  pdc_init
  create_prop
  create_region
  set_prop
  dup_prop
  write_obj
  create_cont
  open_cont
  cont_life
  cont_iter
  cont_info
  cont_getid
  create_obj
  open_obj
  obj_info
  obj_iter
  obj_life
  obj_dim
  obj_buf
  obj_tags
  obj_put_data
  region_obj_map
  create_obj_scale
  close_server
  update_obj
  stat_obj
  delete_obj
  delete_obj_scale
  search_obj_scale
  obj_lock 
  list_all
  init_only
  buf_obj_map
  buf_obj_map_mpi
  buf_map_mpi_v2
  vpicio
  bdcats
  vpicio_v2
  bdcats_v2
  cont_add_del
  data_server_meta_test
  kvtag_add_get
  kvtag_get
  kvtag_add_get_scale
  kvtag_query
  kvtag_query_scale
  obj_transformation
  query_data
  query_vpic_create_data
  query_vpic
  query_vpic_multi
  query_vpic_multi_preload
  query_vpic_multi_nopreload
  query_vpic_bin_sds_preload
  query_vpic_bin_sds_nopreload
  query_vpic_bin_sds1_preload
  query_vpic_bin_sds1_nopreload
  query_vpic_multi_nopreload1
  gen_fastbit_idx
  query_vpic_exyz_nopreload
  query_vpic_exyz_preload
  )

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc)
endforeach(program)

set(SCRIPTS
  run_test.sh
  run_multiple_test.sh
  )

foreach(script ${SCRIPTS})
  file(
       COPY ${script} DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
       FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE WORLD_READ WORLD_EXECUTE
    )
endforeach(script)


# *******************************************
# Add the HDF5 library for pdc-neon
# *******************************************
FIND_LIBRARY(HDF5_LIBRARY NAMES hdf5_debug PATHS $ENV{HOME}/lib)


# *******************************************
# Create a transform library which contains:
#  1. compression/depression functions
#  2. datatype conversion
# *******************************************
OPTION(GEN_TRANSFORM_LIB "Create a transform library" OFF)
if(GEN_TRANSFORM_LIB)
  FIND_LIBRARY(BLOSC_LIBRARY blosc $ENV{HOME}/Sandbox/c-blosc/build/blosc)
  set(PDC_TRANSFORMS_SRCS pdc_transforms_lib.c)
  add_library(pdctransforms  ${PDC_TRANSFORMS_SRCS})
  target_link_libraries(pdctransforms pdc ${BLOSC_LIBRARY})
endif()

# *******************************************
# Create a transform library for testing
# *******************************************
set(PDC_ANALYSIS_SRCS pdc_analysis_lib.c)
add_library(pdcanalysis ${PDC_ANALYSIS_SRCS})
target_link_libraries(pdcanalysis pdc)

add_test(NAME pdc_init    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./pdc_init )
add_test(NAME create_prop WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_prop )
add_test(NAME set_prop WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./set_prop )
add_test(NAME dup_prop WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./dup_prop )
add_test(NAME create_cont WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_cont )
add_test(NAME open_cont   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_cont )
add_test(NAME cont_life   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_life )
add_test(NAME cont_iter   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_iter )
add_test(NAME cont_info   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_info )
add_test(NAME cont_getid   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_getid )
add_test(NAME create_obj  WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_obj )
add_test(NAME open_obj    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_obj )
add_test(NAME obj_iter    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_iter )
add_test(NAME obj_life    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_life )
add_test(NAME obj_dim     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_dim )
add_test(NAME obj_buf     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_buf )
add_test(NAME obj_tags   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_tags )
add_test(NAME obj_info   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_info )
add_test(NAME obj_put_data   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_put_data )
add_test(NAME create_region WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_region )
add_test(NAME region_obj_map WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map )
add_test(NAME write_obj WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj )
add_test(NAME query_data WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./query_data )
add_test(NAME vpicio_bdcats WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./bdcats )

