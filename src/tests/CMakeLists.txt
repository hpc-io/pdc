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
#  obj_transformation
#  obj_analysis
  )

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc)
endforeach(program)

# *******************************************
# Create a transform library which contains:
#  1. compression/depression functions
#  2. datatype conversion
# *******************************************
#FIND_LIBRARY(BLOSC_LIBRARY blosc $ENV{HOME}/Sandbox/c-blosc/build/blosc)
#set(PDC_TRANSFORMS_SRCS pdc_transforms_lib.c)
#add_library(pdctransforms  ${PDC_TRANSFORMS_SRCS})
#target_link_libraries(pdctransforms pdc ${BLOSC_LIBRARY})

# *******************************************
# Create a transform library for testing
# *******************************************
set(PDC_ANALYSIS_SRCS pdc_analysis_lib.c)
add_library(pdcanalysis ${PDC_ANALYSIS_SRCS})
target_link_libraries(pdcanalysis pdc)
