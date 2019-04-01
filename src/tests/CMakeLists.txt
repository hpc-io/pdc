include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PDC_INCLUDES_BUILD_TIME}
#  $ENV{HOME}/Sandbox/c-blosc/blosc
)

add_definitions(-DENABLE_MPI=1)

set(PROGRAMS
  pdc_init
  create_prop
  create_cont
  open_cont
  cont_life
  cont_iter
  create_obj
  open_obj
  obj_iter
  obj_life
  obj_dim
  obj_buf
  create_obj_scale
  close_server
  update_obj
  stat_obj
  delete_obj
  delete_obj_scale
  search_obj_scale
  obj_lock 
  list_all
  data_server_read
  data_server_write
  data_rw_test
  init_only
  obj_buf_map
  obj_buf_map_mpi
  buf_obj_map_mpi
  vpicio
  bdcats
  data_server_write_vpic
  data_server_write_vpic_multits
  data_server_read_vpic
  data_server_read_vpic_multits
  data_server_read_vpic_spatial
  data_server_read_vpic_spatial_multits
  write_1obj
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
  import_vpic
  query_vpic_multi
#  obj_analysis
  )

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc)
endforeach(program)

set(SCRIPTS
  run_test.sh
  )

foreach(script ${SCRIPTS})
  file(
       COPY ${script} DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
       FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE WORLD_READ WORLD_EXECUTE
    )
endforeach(script)

# *******************************************
# Create a transform library which contains:
#  1. compression/depression functions
#  2. datatype conversion
# *******************************************
FIND_LIBRARY(BLOSC_LIBRARY blosc $ENV{HOME}/Sandbox/c-blosc/build/blosc)
set(PDC_TRANSFORMS_SRCS pdc_transforms_lib.c)
add_library(pdctransforms  ${PDC_TRANSFORMS_SRCS})
target_link_libraries(pdctransforms pdc ${BLOSC_LIBRARY})

# *******************************************
# Create a transform library for testing
# *******************************************
set(PDC_ANALYSIS_SRCS pdc_analysis_lib.c)
add_library(pdcanalysis ${PDC_ANALYSIS_SRCS})
target_link_libraries(pdcanalysis pdc)

add_test(NAME pdc_init    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./pdc_init )
add_test(NAME create_prop WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_prop )
add_test(NAME create_cont WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_cont )
add_test(NAME open_cont   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_cont )
add_test(NAME cont_life   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_life )
add_test(NAME cont_iter   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_iter )
add_test(NAME create_obj  WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_obj )
add_test(NAME open_obj    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_obj )
add_test(NAME obj_iter    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_iter )
add_test(NAME obj_life    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_life )
add_test(NAME obj_dim     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_dim )
add_test(NAME obj_buf     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_buf )
