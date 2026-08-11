include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/helper/include
  ${PDC_INCLUDES_BUILD_TIME}
  ${PROJECT_BINARY_DIR}
  ${PDC_SOURCE_DIR}
  ${PDC_SOURCE_DIR}/src/client_api/include
  $ENV{HOME}/Sandbox/c-blosc/blosc
  $ENV{HOME}/include
)

set(TEST_EXT_LIB "")
set(TEST_EXT_INCLUDE_DIRS "")
set(EXTRA_SRC_FILE "")

# *************************************************
# Julia Support
# *************************************************
if (PDC_ENABLE_JULIA)
  include_directories(${JULIA_INCLUDE_DIRS})
endif(PDC_ENABLE_JULIA)

# *************************************************
# *  Find UUID library
# *************************************************
find_package(UUID)
if(UUID_FOUND)
  include_directories(${UUID_INCLUDE_DIRS})
endif(UUID_FOUND)

# *************************************************
# *  MERCURY
# *************************************************
find_package(MERCURY REQUIRED)
if(MERCURY_FOUND)
  set(TEST_EXT_INCLUDE_DIRS ${MERCURY_INCLUDE_DIRS}
    ${TEST_EXT_INCLUDE_DIRS}
    )
  set(TEST_EXT_LIB ${MERCURY_LIBRARIES} ${TEST_EXT_LIB})
endif()
include_directories(${TEST_EXT_INCLUDE_DIRS})

set(PROGRAMS
  misc/pdc_init
  prop/dup_prop
  obj/read_obj
  obj/read_obj_shared
  cont/open_cont
  cont/cont_life
  cont/cont_iter
  cont/cont_info
  cont/cont_del
  cont/cont_getid
  cont/cont_tags
  misc/consistency_semantics
  obj/create_obj
  obj/open_obj
  obj/open_existing_obj
  obj/obj_info
  obj/obj_del
  obj/obj_iter
  obj/obj_life
  obj/obj_buf
  obj/obj_tags
  obj/obj_put_data
  obj/obj_get_data
  misc/read_write_perf
  misc/read_write_col_perf
  region/region_transfer_partial
  region/region_transfer_2D_partial
  region/region_transfer_3D_partial
  region/region_transfer_overlap
  region/region_transfer_overlap_2D
  region/region_transfer_overlap_3D
  region/region_transfer_merge
  obj/obj_round_robin_io
  obj/obj_round_robin_io_all
  obj/delete_obj
  misc/vpicio
  misc/bdcats
  tags/kvtag_add_get
  tags/kvtag_query
  region/region_transfer_query
  region/region_transfer
  region/region_transfer_status
  region/region_transfer_skewed
  region/region_transfer_2D
  region/region_transfer_3D
  region/region_transfer_write_only
  region/region_transfer_read_only
  region/region_transfer_write_read
  region/region_transfer_all
  region/region_transfer_all_2D
  region/region_transfer_all_3D
  region/region_transfer_all_append
  region/region_transfer_all_append_2D
  region/region_transfer_all_append_3D
  region/region_transfer_all_split_wait
  region/region_transfer_set_dims
  region/region_transfer_set_dims_2D
  region/region_transfer_set_dims_3D
  query/query_data
  misc/producer_waitall
)

# TODO: Check if import_vpic.c is needed. If yes, we have to add the following :
# if (HDF5_FOUND) 
#   set(TEST_EXT_LIB ${HDF5_LIBRARIES} ${TEST_EXT_LIB})
#   set(TEST_EXT_INCLUDE_DIRS ${HDF5_INCLUDE_DIRS} ${TEST_EXT_INCLUDE_DIRS})
# else ()
#   message(FATAL_ERROR "Could not find HDF5, please make sure that HDF5 has been compiled with shared libraries enabled.")
# endif()

foreach(program ${PROGRAMS})
  get_filename_component(program_name ${program} NAME)
  get_filename_component(program_dir ${program} DIRECTORY)

  set(SOURCE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${program}.c")

  if(EXISTS ${SOURCE_FILE})
    add_executable(${program_name} ${SOURCE_FILE})
    target_include_directories(${program_name} PRIVATE 
      ${CMAKE_SOURCE_DIR}/include 
      ${CMAKE_SOURCE_DIR}/src/tests/include
    )
    target_link_libraries(${program_name} pdc pdc_commons ${TEST_EXT_LIB})
  else()
    message(WARNING "Source file ${SOURCE_FILE} not found, skipping ${program_name}.")
  endif()
endforeach()

if(BUILD_MPI_TESTING)
  set(MPI_PROGRAMS
    tags/kvtag_range_query_scale
    tags/kvtag_affix_query_scale
    tags/kvtag_add_get_benchmark
    tags/kvtag_add_get_scale
  )

if(PDC_ENABLE_IDIOMS)
  list(APPEND MPI_PROGRAMS
    misc/llsm_idioms_bench
    misc/idioms_ci_test
  )
endif()

  foreach(program ${MPI_PROGRAMS})
    get_filename_component(program_name ${program} NAME)
    get_filename_component(program_dir ${program} DIRECTORY)
  
    set(SOURCE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${program_dir}/${program_name}.c")
  
    if(EXISTS ${SOURCE_FILE})
      add_executable(${program_name} ${SOURCE_FILE})
      if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${program_name} PRIVATE ${SUPPRESSED_LIST})
      endif()
      target_link_libraries(${program_name} pdc pdc_commons ${TEST_EXT_LIB})
      target_include_directories(${program_name} PRIVATE 
        ${TEST_EXT_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/src/tests/include)
    else()
      message(WARNING "Source file ${SOURCE_FILE} not found, skipping ${program_name}.")
    endif()
  endforeach()
endif()


if(UUID_FOUND)
  message(STATUS "UUID library found: ${UUID_LIBRARIES}")
  message(STATUS "UUID include directory: ${UUID_INCLUDE_DIRS}")
  set(ENHANCED_PROGRAMS 
    dart/dart_algo_sim
    dart/dart_attr_dist_test
    dart/dart_func_test
    dart/dart_test
  )

  set(TEST_EXT_LIB ${TEST_EXT_LIB} ${UUID_LIBRARIES})
  set(TEST_EXT_INCLUDE_DIRS ${TEST_EXT_INCLUDE_DIRS} ${UUID_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/helper/include)

  set(EXTRA_SRC_FILE "")
  if (PDC_ENABLE_JULIA)
    list(APPEND EXTRA_SRC_FILE
      ${CMAKE_CURRENT_SOURCE_DIR}/helper/julia_helper_loader.c
    )
    list(APPEND TEST_EXT_LIB
      ${JULIA_LIBRARIES}
    )
    list(APPEND TEST_EXT_INCLUDE_DIRS
      ${JULIA_INCLUDE_DIRS}
    )
  endif(PDC_ENABLE_JULIA)

  foreach(program ${ENHANCED_PROGRAMS})
    get_filename_component(program_name ${program} NAME)
    get_filename_component(program_dir ${program} DIRECTORY)

    set(SOURCE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${program}.c")

    if(EXISTS ${SOURCE_FILE})
      add_executable(${program_name} ${SOURCE_FILE} ${EXTRA_SRC_FILE})
      target_link_libraries(${program_name} pdc pdc_commons ${TEST_EXT_LIB})
      target_include_directories(${program_name} PRIVATE ${TEST_EXT_INCLUDE_DIRS})
    else()
      message(WARNING "Source file ${SOURCE_FILE} not found, skipping ${program_name}.")
    endif()
  endforeach(program)
endif(UUID_FOUND)


set(SCRIPTS
  run_test.sh
  mpi_test.sh
  run_multiple_test.sh
  run_multiple_mpi_test.sh
  run_checkpoint_restart_test.sh
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
OPTION(GEN_TRANSFORM_LIB "Create a transform library" OFF)
if(GEN_TRANSFORM_LIB)
  FIND_LIBRARY(BLOSC_LIBRARY blosc $ENV{HOME}/Sandbox/c-blosc/build/blosc)
  set(PDC_TRANSFORMS_SRCS transforms/pdc_transforms_lib.c)
  add_library(pdctransforms  ${PDC_TRANSFORMS_SRCS})
  target_link_libraries(pdctransforms pdc ${BLOSC_LIBRARY})
endif()

# *******************************************
# Create a transform library for testing
# *******************************************
set(PDC_ANALYSIS_SRCS analysis/pdc_analysis_lib.c)
add_library(pdcanalysis ${PDC_ANALYSIS_SRCS})
target_link_libraries(pdcanalysis pdc)

add_test(NAME pdc_init          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./pdc_init )
add_test(NAME dup_prop          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./dup_prop )
add_test(NAME open_cont         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_cont )
add_test(NAME cont_life         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_life )
add_test(NAME cont_iter         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_iter )
add_test(NAME cont_info         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_info )
add_test(NAME cont_getid        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_getid )
add_test(NAME cont_tags         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_tags )
add_test(NAME cont_del          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_del )
add_test(NAME obj_del           WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_del )
add_test(NAME open_obj          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_obj )
add_test(NAME obj_iter          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_iter )
add_test(NAME obj_life          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_life )
add_test(NAME obj_buf           WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_buf )
add_test(NAME obj_tags          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_tags )
add_test(NAME kvtag_add_get     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./kvtag_add_get)
add_test(NAME kvtag_query     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./kvtag_query 100 1 10 0)
if(PDC_ENABLE_IDIOMS)
add_test(NAME idioms_ci_test     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./idioms_ci_test)
endif()
add_test(NAME obj_info          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_info )
add_test(NAME obj_put_data      WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_put_data )
add_test(NAME obj_get_data      WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_get_data )
add_test(NAME region_transfer    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer )
add_test(NAME region_transfer_status    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_status )
add_test(NAME region_transfer_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_2D )
add_test(NAME region_transfer_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_3D )
add_test(NAME region_transfer_skewed    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_skewed )
add_test(NAME region_transfer_partial WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_partial )
add_test(NAME region_transfer_2D_partial WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_2D_partial )
add_test(NAME region_transfer_3D_partial WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_3D_partial )
add_test(NAME region_transfer_overlap WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_overlap )
add_test(NAME region_transfer_overlap_2D WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_overlap_2D )
add_test(NAME region_transfer_overlap_3D WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_overlap_3D )
add_test(NAME region_transfer_merge WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_merge )
add_test(NAME region_transfer_write_read WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_write_read )
add_test(NAME region_transfer_all    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all )
add_test(NAME region_transfer_all_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_2D )
add_test(NAME region_transfer_all_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_3D )
add_test(NAME region_transfer_all_append    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append )
add_test(NAME region_transfer_all_append_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_2D )
add_test(NAME region_transfer_all_append_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_3D )
add_test(NAME region_transfer_all2    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all 0)
add_test(NAME region_transfer_all2_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_2D 0)
add_test(NAME region_transfer_all2_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_3D 0)
add_test(NAME region_transfer_all_append2    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append 0)
add_test(NAME region_transfer_all_append2_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_2D 0)
add_test(NAME region_transfer_all_append2_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_3D 0)
add_test(NAME region_transfer_all3    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all 0 0)
add_test(NAME region_transfer_all3_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_2D 0 0)
add_test(NAME region_transfer_all3_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_3D 0 0)
add_test(NAME region_transfer_all_append3    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append 0 0)
add_test(NAME region_transfer_all_append3_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_2D 0 0)
add_test(NAME region_transfer_all_append3_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_3D 0 0)
add_test(NAME region_transfer_all4    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all 1 0)
add_test(NAME region_transfer_all4_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_2D 1 0)
add_test(NAME region_transfer_all4_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_3D 1 0)
add_test(NAME region_transfer_all_append4    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append 1 0)
add_test(NAME region_transfer_all_append4_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_2D 1 0)
add_test(NAME region_transfer_all_append4_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_append_3D 1 0)
add_test(NAME region_transfer_all_split_wait    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_transfer_all_split_wait )
add_test(NAME read_obj_int     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 int)
add_test(NAME read_obj_float   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 float)
add_test(NAME read_obj_double  WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 double)
add_test(NAME read_obj_char    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 char)
add_test(NAME read_obj_uint    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 uint)
add_test(NAME read_obj_int64   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 int64)
add_test(NAME read_obj_uint64  WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 uint64)
add_test(NAME read_obj_int16   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 int16)
add_test(NAME read_obj_int8    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./read_obj o 1 int8)
add_test(NAME vpicio_bdcats_transfer_request     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./bdcats)

set_tests_properties(pdc_init           PROPERTIES LABELS serial)
set_tests_properties(dup_prop           PROPERTIES LABELS serial)
set_tests_properties(open_cont          PROPERTIES LABELS serial)
set_tests_properties(cont_life          PROPERTIES LABELS serial)
set_tests_properties(cont_iter          PROPERTIES LABELS serial)
set_tests_properties(cont_info          PROPERTIES LABELS serial)
set_tests_properties(cont_getid         PROPERTIES LABELS serial)
set_tests_properties(cont_tags          PROPERTIES LABELS serial)
set_tests_properties(cont_del           PROPERTIES LABELS serial)
set_tests_properties(obj_del            PROPERTIES LABELS serial)
set_tests_properties(open_obj           PROPERTIES LABELS serial)
set_tests_properties(obj_iter           PROPERTIES LABELS serial)
set_tests_properties(obj_life           PROPERTIES LABELS serial)
set_tests_properties(obj_buf            PROPERTIES LABELS serial)
set_tests_properties(obj_tags           PROPERTIES LABELS serial)
set_tests_properties(kvtag_add_get      PROPERTIES LABELS serial)
set_tests_properties(kvtag_query        PROPERTIES LABELS serial)
if(PDC_ENABLE_IDIOMS)
set_tests_properties(idioms_ci_test     PROPERTIES LABELS serial)
endif()
set_tests_properties(obj_info           PROPERTIES LABELS serial)
set_tests_properties(obj_put_data       PROPERTIES LABELS serial)
set_tests_properties(obj_get_data       PROPERTIES LABELS serial)
set_tests_properties(region_transfer     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_status     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_skewed     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_partial     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_2D_partial  PROPERTIES LABELS serial)
set_tests_properties(region_transfer_3D_partial  PROPERTIES LABELS serial)
set_tests_properties(region_transfer_overlap     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_overlap_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_overlap_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_write_read     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all2     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all2_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all2_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append2     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append2_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append2_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all3     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all3_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all3_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append3     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append3_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append3_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all4     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all4_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all4_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append4     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append4_2D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_append4_3D     PROPERTIES LABELS serial)
set_tests_properties(region_transfer_all_split_wait     PROPERTIES LABELS serial)
set_tests_properties(read_obj_int      PROPERTIES LABELS serial)
set_tests_properties(read_obj_float    PROPERTIES LABELS serial)
set_tests_properties(read_obj_double   PROPERTIES LABELS serial)
set_tests_properties(read_obj_char     PROPERTIES LABELS serial)
set_tests_properties(read_obj_uint     PROPERTIES LABELS serial)
set_tests_properties(read_obj_int64    PROPERTIES LABELS serial)
set_tests_properties(read_obj_uint64   PROPERTIES LABELS serial)
set_tests_properties(read_obj_int16    PROPERTIES LABELS serial)
set_tests_properties(read_obj_int8     PROPERTIES LABELS serial)
set_tests_properties(vpicio_bdcats_transfer_request      PROPERTIES LABELS serial)

if(BUILD_MPI_TESTING)
    add_test(NAME read_obj_shared_int WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 int)
    add_test(NAME read_obj_shared_float WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 float)
    add_test(NAME read_obj_shared_double WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 double)
    add_test(NAME read_obj_shared_char WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 char)
    add_test(NAME read_obj_shared_uint WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 uint)
    add_test(NAME read_obj_shared_int64 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 int64)
    add_test(NAME read_obj_shared_uint64 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 uint64)
    add_test(NAME read_obj_shared_int16 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 int16)
    add_test(NAME read_obj_shared_int8 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 4 6 o 1 int8)
    add_test(NAME region_transfer_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_2D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_3D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_skewed_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_skewed ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_partial_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_partial ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_2D_partial_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_2D_partial ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_3D_partial_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_3D_partial ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_overlap_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_overlap ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_overlap_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_overlap_2D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_overlap_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_overlap_3D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_write_read_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_write_read ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_all_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_all_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_2D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_all_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_3D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_all_append_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_all_append_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_2D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_all_append_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_3D ${MPI_RUN_CMD} 4 6 )
    add_test(NAME region_transfer_all2_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all ${MPI_RUN_CMD} 4 6 0 1)
    add_test(NAME region_transfer_all2_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_2D ${MPI_RUN_CMD} 4 6 0 1)
    add_test(NAME region_transfer_all2_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_3D ${MPI_RUN_CMD} 4 6 0 1)
    add_test(NAME region_transfer_all_append2_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append ${MPI_RUN_CMD} 4 6 0 1)
    add_test(NAME region_transfer_all_append2_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_2D ${MPI_RUN_CMD} 4 6 0 1)
    add_test(NAME region_transfer_all_append2_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_3D ${MPI_RUN_CMD} 4 6 0 1)
    add_test(NAME region_transfer_all3_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all ${MPI_RUN_CMD} 4 6 0 0)
    add_test(NAME region_transfer_all3_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_2D ${MPI_RUN_CMD} 4 6 0 0)
    add_test(NAME region_transfer_all3_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_3D ${MPI_RUN_CMD} 4 6 0 0)
    add_test(NAME region_transfer_all_append3_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append ${MPI_RUN_CMD} 4 6 0 0)
    add_test(NAME region_transfer_all_append3_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_2D ${MPI_RUN_CMD} 4 6 0 0)
    add_test(NAME region_transfer_all_append3_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_3D ${MPI_RUN_CMD} 4 6 0 0)
    add_test(NAME region_transfer_all4_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all ${MPI_RUN_CMD} 4 6 1 0)
    add_test(NAME region_transfer_all4_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_2D ${MPI_RUN_CMD} 4 6 1 0)
    add_test(NAME region_transfer_all4_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_3D ${MPI_RUN_CMD} 4 6 1 0)
    add_test(NAME region_transfer_all_append4_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append ${MPI_RUN_CMD} 2 4 1 0)
    add_test(NAME region_transfer_all_append4_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_2D ${MPI_RUN_CMD} 4 6 1 0)
    add_test(NAME region_transfer_all_append4_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_append_3D ${MPI_RUN_CMD} 4 6 1 0)
    add_test(NAME region_transfer_all_split_wait_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_transfer_all_split_wait ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_round_robin_io_1D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_round_robin_io ${MPI_RUN_CMD} 4 4 int 1 )
    add_test(NAME obj_round_robin_io_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_round_robin_io ${MPI_RUN_CMD} 4 4 int 2 )
    add_test(NAME obj_round_robin_io_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_round_robin_io ${MPI_RUN_CMD} 4 4 int 3 )
    add_test(NAME obj_round_robin_io_all_1D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_round_robin_io ${MPI_RUN_CMD} 4 4 int 1 )
    add_test(NAME obj_round_robin_io_all_2D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_round_robin_io ${MPI_RUN_CMD} 4 4 int 2 )
    add_test(NAME obj_round_robin_io_all_3D    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_round_robin_io ${MPI_RUN_CMD} 4 4 int 3 )
    add_test(NAME pdc_init_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./pdc_init ${MPI_RUN_CMD} 4 6 )
    add_test(NAME dup_prop_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./dup_prop ${MPI_RUN_CMD} 4 6 )
    add_test(NAME open_cont_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./open_cont ${MPI_RUN_CMD} 4 6 )
    add_test(NAME cont_life_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_life ${MPI_RUN_CMD} 4 6 )
    add_test(NAME cont_iter_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_iter ${MPI_RUN_CMD} 4 6 )
    add_test(NAME cont_info_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_info ${MPI_RUN_CMD} 4 6 )
    add_test(NAME cont_getid_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_getid ${MPI_RUN_CMD} 4 6 )
    add_test(NAME cont_tags_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_tags ${MPI_RUN_CMD} 4 6 )
    add_test(NAME consistency_semantics WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./consistency_semantics ${MPI_RUN_CMD} 2 4 )
    add_test(NAME open_obj_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./open_obj ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_iter_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_iter ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_life_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_life ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_buf_mpi     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_buf ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_tags_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_tags ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_info_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_info ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_put_data_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_put_data ${MPI_RUN_CMD} 4 6 )
    add_test(NAME obj_get_data_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_get_data ${MPI_RUN_CMD} 4 6 )
    add_test(NAME vpicio_bdcats_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_mpi_test.sh ${MPI_RUN_CMD} 4 6 ./vpicio ./bdcats)
    add_test(NAME vpicio_bdcats_strategy_0 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_mpi_test.sh ${MPI_RUN_CMD} 4 6 ./vpicio ./bdcats 0)
    add_test(NAME vpicio_bdcats_strategy_1 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_mpi_test.sh ${MPI_RUN_CMD} 4 6 ./vpicio ./bdcats 1)
    add_test(NAME vpicio_bdcats_strategy_2 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_mpi_test.sh ${MPI_RUN_CMD} 4 6 ./vpicio ./bdcats 2)
    add_test(NAME vpicio_mpi_small_cache WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./vpicio ${MPI_RUN_CMD} 1 16)

    set_tests_properties(read_obj_shared_int                  PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_float                PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_double               PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_char                 PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_uint                 PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_int64                PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_uint64               PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_int16                PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(read_obj_shared_int8                 PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(consistency_semantics                PROPERTIES LABELS "parallel;parallel_consistency")
    set_tests_properties(region_transfer_partial_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_2D_partial_mpi       PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_3D_partial_mpi       PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_overlap_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_overlap_2D_mpi       PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_overlap_3D_mpi       PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_mpi                  PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_2D_mpi               PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_3D_mpi               PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_skewed_mpi           PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(region_transfer_write_read_mpi       PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(vpicio_bdcats_mpi                    PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(vpicio_mpi_small_cache               PROPERTIES LABELS "parallel;parallel_region_transfer" )
    set_tests_properties(vpicio_bdcats_strategy_0 PROPERTIES LABELS "parallel;writeout-strategies")
    set_tests_properties(vpicio_bdcats_strategy_1 PROPERTIES LABELS "parallel;writeout-strategies")
    set_tests_properties(vpicio_bdcats_strategy_2 PROPERTIES LABELS "parallel;writeout-strategies")
    set_tests_properties(region_transfer_all_mpi              PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_2D_mpi           PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_3D_mpi           PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append_mpi       PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append_2D_mpi    PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append_3D_mpi    PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all2_mpi             PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all2_2D_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all2_3D_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append2_mpi      PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append2_2D_mpi   PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append2_3D_mpi   PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all3_mpi             PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all3_2D_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all3_3D_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append3_mpi      PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append3_2D_mpi   PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append3_3D_mpi   PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all4_mpi             PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all4_2D_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all4_3D_mpi          PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append4_mpi      PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append4_2D_mpi   PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_append4_3D_mpi   PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(region_transfer_all_split_wait_mpi   PROPERTIES LABELS "parallel;parallel_region_transfer_all" )
    set_tests_properties(obj_round_robin_io_1D                PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_round_robin_io_2D                PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_round_robin_io_3D                PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_round_robin_io_all_1D            PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_round_robin_io_all_2D            PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_round_robin_io_all_3D            PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(pdc_init_mpi                         PROPERTIES LABELS "parallel;parallel_pdc" )
    set_tests_properties(dup_prop_mpi                         PROPERTIES LABELS "parallel;parallel_prop" )
    set_tests_properties(open_cont_mpi                        PROPERTIES LABELS "parallel;parallel_cont" )
    set_tests_properties(cont_life_mpi                        PROPERTIES LABELS "parallel;parallel_cont" )
    set_tests_properties(cont_iter_mpi                        PROPERTIES LABELS "parallel;parallel_cont" )
    set_tests_properties(cont_info_mpi                        PROPERTIES LABELS "parallel;parallel_cont" )
    set_tests_properties(cont_getid_mpi                       PROPERTIES LABELS "parallel;parallel_cont" )
    set_tests_properties(cont_tags_mpi                        PROPERTIES LABELS "parallel;parallel_cont" )
    set_tests_properties(open_obj_mpi                         PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_iter_mpi                         PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_life_mpi                         PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_buf_mpi                          PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_tags_mpi                         PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_info_mpi                         PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_put_data_mpi                     PROPERTIES LABELS "parallel;parallel_obj" )
    set_tests_properties(obj_get_data_mpi                     PROPERTIES LABELS "parallel;parallel_obj" )
endif()
