include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PDC_INCLUDES_BUILD_TIME}
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
  vpicio
  data_server_write_vpic
  data_server_write_vpic_multits
  data_server_read_vpic
  data_server_read_vpic_multits
  data_server_read_vpic_spatial
  data_server_read_vpic_spatial_multits
  write_1obj
  blocked_lock
  )

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc)
endforeach(program)
