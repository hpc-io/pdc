include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/tests)
link_directories(${PROJECT_BINARY_DIR}/src)

set(PROGRAMS create_prop)

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc)
endforeach(program)

