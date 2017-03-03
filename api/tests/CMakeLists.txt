include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/tests)
link_directories(${PROJECT_BINARY_DIR}/src)

set(PROGRAMS pdc_init create_prop create_cont open_cont cont_life cont_iter create_obj open_obj obj_iter obj_life obj_dim obj_buf create_obj_scale close_server update_obj stat_obj delete_obj delete_obj_scale search_obj_scale obj_lock)

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc)
endforeach(program)

