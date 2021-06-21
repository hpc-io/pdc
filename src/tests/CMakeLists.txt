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
  create_obj_coll
  set_prop
  dup_prop
  write_obj
  write_obj_shared
  create_cont
  create_cont_coll
  open_cont
  cont_life
  cont_iter
  cont_info
  cont_del
  cont_getid
  cont_tags
  create_obj
  open_obj
  obj_info
  obj_del
  obj_iter
  obj_life
  obj_dim
  obj_buf
  obj_tags
  obj_put_data
  obj_get_data
  read_write_perf
  read_write_col_perf
  open_obj_round_robin
  region_obj_map
  region_obj_map_2D
  region_obj_map_3D
  region_obj_map_partial
  region_obj_map_2D_partial
  region_obj_map_3D_partial
  region_obj_map_overlap
  region_obj_map_overlap_2D
  region_obj_map_overlap_3D
  read_obj_shared
  obj_round_robin_io
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
  #query_vpic_create_data
  #query_vpic
  #query_vpic_multi
  #query_vpic_multi_preload
  #query_vpic_multi_nopreload
  #query_vpic_bin_sds_preload
  #query_vpic_bin_sds_nopreload
  #query_vpic_bin_sds1_preload
  #query_vpic_bin_sds1_nopreload
  #query_vpic_multi_nopreload1
  #gen_fastbit_idx
  #query_vpic_exyz_nopreload
  #query_vpic_exyz_preload
  )

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.c)
  target_link_libraries(${program} pdc)
endforeach(program)

set(SCRIPTS
  run_test.sh
  mpi_test.sh
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

add_test(NAME pdc_init          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./pdc_init )
add_test(NAME create_prop       WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_prop )
add_test(NAME set_prop          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./set_prop )
add_test(NAME dup_prop          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./dup_prop )
add_test(NAME create_cont       WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_cont )
add_test(NAME open_cont         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_cont )
add_test(NAME cont_life         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_life )
add_test(NAME cont_iter         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_iter )
add_test(NAME cont_info         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_info )
add_test(NAME cont_getid        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_getid )
add_test(NAME cont_tags         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_tags )
add_test(NAME cont_del          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./cont_del )
add_test(NAME create_obj        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_obj )
add_test(NAME obj_del           WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_del )
add_test(NAME open_obj          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./open_obj )
add_test(NAME obj_iter          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_iter )
add_test(NAME obj_life          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_life )
add_test(NAME obj_dim           WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_dim )
add_test(NAME obj_buf           WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_buf )
add_test(NAME obj_tags          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_tags )
add_test(NAME obj_info          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_info )
add_test(NAME obj_put_data      WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_put_data )
add_test(NAME obj_get_data      WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./obj_get_data )
add_test(NAME create_region     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./create_region )
add_test(NAME region_obj_map    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map )
add_test(NAME region_obj_map_2D WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_2D )
add_test(NAME region_obj_map_3D WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_3D )
add_test(NAME region_obj_map_partial WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_partial )
add_test(NAME region_obj_map_2D_partial WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_2D_partial )
add_test(NAME region_obj_map_3D_partial WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_3D_partial )
add_test(NAME region_obj_map_overlap WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_overlap )
add_test(NAME region_obj_map_overlap_2D WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_overlap_2D )
add_test(NAME region_obj_map_overlap_3D WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./region_obj_map_overlap_3D )
add_test(NAME write_obj_int     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 int)
add_test(NAME write_obj_float   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 float)
add_test(NAME write_obj_double  WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 double)
add_test(NAME write_obj_char    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 char)
add_test(NAME write_obj_uint    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 uint)
add_test(NAME write_obj_int64   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 int64)
add_test(NAME write_obj_uint64  WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 uint64)
add_test(NAME write_obj_int16   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 int16)
add_test(NAME write_obj_int8    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./write_obj o 1 int8)
add_test(NAME query_data        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_test.sh ./query_data o 1)
add_test(NAME vpicio_bdcats     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./bdcats)

set_tests_properties(pdc_init           PROPERTIES LABELS serial )
set_tests_properties(create_prop        PROPERTIES LABELS serial )
set_tests_properties(set_prop           PROPERTIES LABELS serial )
set_tests_properties(dup_prop           PROPERTIES LABELS serial )
set_tests_properties(create_cont        PROPERTIES LABELS serial )
set_tests_properties(open_cont          PROPERTIES LABELS serial )
set_tests_properties(cont_life          PROPERTIES LABELS serial )
set_tests_properties(cont_iter          PROPERTIES LABELS serial )
set_tests_properties(cont_info          PROPERTIES LABELS serial )
set_tests_properties(cont_getid         PROPERTIES LABELS serial )
set_tests_properties(cont_tags          PROPERTIES LABELS serial )
set_tests_properties(cont_del           PROPERTIES LABELS serial )
set_tests_properties(create_obj         PROPERTIES LABELS serial )
set_tests_properties(obj_del            PROPERTIES LABELS serial )
set_tests_properties(open_obj           PROPERTIES LABELS serial )
set_tests_properties(obj_iter           PROPERTIES LABELS serial )
set_tests_properties(obj_life           PROPERTIES LABELS serial )
set_tests_properties(obj_dim            PROPERTIES LABELS serial )
set_tests_properties(obj_buf            PROPERTIES LABELS serial )
set_tests_properties(obj_tags           PROPERTIES LABELS serial )
set_tests_properties(obj_info           PROPERTIES LABELS serial )
set_tests_properties(obj_put_data       PROPERTIES LABELS serial )
set_tests_properties(obj_get_data       PROPERTIES LABELS serial )
set_tests_properties(create_region      PROPERTIES LABELS serial )
set_tests_properties(region_obj_map     PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_2D  PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_3D  PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_partial     PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_2D_partial  PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_3D_partial  PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_overlap     PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_overlap_2D     PROPERTIES LABELS serial )
set_tests_properties(region_obj_map_overlap_3D     PROPERTIES LABELS serial )
set_tests_properties(write_obj_int      PROPERTIES LABELS serial )
set_tests_properties(write_obj_float    PROPERTIES LABELS serial )
set_tests_properties(write_obj_double   PROPERTIES LABELS serial )
set_tests_properties(write_obj_char     PROPERTIES LABELS serial )
set_tests_properties(write_obj_uint     PROPERTIES LABELS serial )
set_tests_properties(write_obj_int64    PROPERTIES LABELS serial )
set_tests_properties(write_obj_uint64   PROPERTIES LABELS serial )
set_tests_properties(write_obj_int16    PROPERTIES LABELS serial )
set_tests_properties(write_obj_int8     PROPERTIES LABELS serial )
set_tests_properties(query_data         PROPERTIES LABELS serial )
set_tests_properties(vpicio_bdcats      PROPERTIES LABELS serial )
#add_test(NAME vpicio_query_vpic WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic )
#add_test(NAME vpicio_query_vpic_multi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_multi )
#add_test(NAME vpicio_query_vpic_multi_preload WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_multi_preload )
#add_test(NAME vpicio_query_vpic_multi_nopreload1 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_multi_nopreload1 )
#add_test(NAME vpicio_query_vpic_create_data WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_create_data )
#add_test(NAME vpicio_query_vpic_bin_sds1_nopreload WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_bin_sds1_nopreload )
#add_test(NAME vpicio_query_vpic_bin_sds1_preload WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_bin_sds1_preload )
#add_test(NAME vpicio_query_vpic_exyz_preload WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_exyz_preload )
#add_test(NAME vpicio_query_vpic_exyz_nopreload WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND run_multiple_test.sh ./vpicio ./query_vpic_exyz_nopreload )

if(BUILD_MPI_TESTING)
    add_test(NAME write_obj_shared_int WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int)
    add_test(NAME write_obj_shared_float WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 float)
    add_test(NAME write_obj_shared_double WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 double)
    add_test(NAME write_obj_shared_char WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 char)
    add_test(NAME write_obj_shared_uint WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 uint)
    add_test(NAME write_obj_shared_int64 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int64)
    add_test(NAME write_obj_shared_uint64 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 uint64)
    add_test(NAME write_obj_shared_int16 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int16)
    add_test(NAME write_obj_shared_int8 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int8)
    add_test(NAME read_obj_shared_int WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int)
    add_test(NAME read_obj_shared_float WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 float)
    add_test(NAME read_obj_shared_double WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 double)
    add_test(NAME read_obj_shared_char WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 char)
    add_test(NAME read_obj_shared_uint WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 uint)
    add_test(NAME read_obj_shared_int64 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int64)
    add_test(NAME read_obj_shared_uint64 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 uint64)
    add_test(NAME read_obj_shared_int16 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int16)
    add_test(NAME read_obj_shared_int8 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./read_obj_shared ${MPI_RUN_CMD} 2 4 o 1 int8)
    add_test(NAME create_obj_coll    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./create_obj_coll ${MPI_RUN_CMD} 2 4 )
    add_test(NAME create_cont_coll    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./create_cont_coll ${MPI_RUN_CMD} 2 4 )
    add_test(NAME open_obj_round_robin    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./open_obj_round_robin ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_2D_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_2D ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_3D_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_3D ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_partial_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_partial ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_2D_partial_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_2D_partial ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_3D_partial_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_3D_partial ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_overlap_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_overlap ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_overlap_2D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_overlap_2D ${MPI_RUN_CMD} 2 4 )
    add_test(NAME region_obj_map_overlap_3D_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./region_obj_map_overlap_3D ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_round_robin_io    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_round_robin_io ${MPI_RUN_CMD} 2 2 int )
    add_test(NAME pdc_init_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./pdc_init ${MPI_RUN_CMD} 2 4 )
    add_test(NAME create_prop_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./create_prop ${MPI_RUN_CMD} 2 4 )
    add_test(NAME set_prop_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./set_prop ${MPI_RUN_CMD} 2 4 )
    add_test(NAME dup_prop_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./dup_prop ${MPI_RUN_CMD} 2 4 )
    add_test(NAME create_cont_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./create_cont ${MPI_RUN_CMD} 2 4 )
    add_test(NAME open_cont_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./open_cont ${MPI_RUN_CMD} 2 4 )
    add_test(NAME cont_life_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_life ${MPI_RUN_CMD} 2 4 )
    add_test(NAME cont_iter_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_iter ${MPI_RUN_CMD} 2 4 )
    add_test(NAME cont_info_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_info ${MPI_RUN_CMD} 2 4 )
    add_test(NAME cont_getid_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_getid ${MPI_RUN_CMD} 2 4 )
    add_test(NAME cont_tags_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./cont_tags ${MPI_RUN_CMD} 2 4 )
    add_test(NAME create_obj_mpi  WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./create_obj ${MPI_RUN_CMD} 2 4 )
    add_test(NAME open_obj_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./open_obj ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_iter_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_iter ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_life_mpi    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_life ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_dim_mpi     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_dim ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_buf_mpi     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_buf ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_tags_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_tags ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_info_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_info ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_put_data_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_put_data ${MPI_RUN_CMD} 2 4 )
    add_test(NAME obj_get_data_mpi   WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./obj_get_data ${MPI_RUN_CMD} 2 4 )
    add_test(NAME create_region_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./create_region ${MPI_RUN_CMD} 2 4 )
    add_test(NAME write_obj_mpi WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND mpi_test.sh ./write_obj ${MPI_RUN_CMD} 2 4 o 1 int)

    set_tests_properties(write_obj_shared_int PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_float  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_double  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_char  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_uint  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_int64  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_uint64  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_int16  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_shared_int8  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_int  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_float  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_double  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_char  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_uint  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_int64  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_uint64  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_int16  PROPERTIES LABELS parallel )
    set_tests_properties(read_obj_shared_int8  PROPERTIES LABELS parallel )
    set_tests_properties(create_obj_coll     PROPERTIES LABELS parallel )
    set_tests_properties(create_cont_coll     PROPERTIES LABELS parallel )
    set_tests_properties(open_obj_round_robin     PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_2D_mpi     PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_3D_mpi     PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_partial_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_2D_partial_mpi PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_3D_partial_mpi PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_overlap_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_overlap_2D_mpi PROPERTIES LABELS parallel )
    set_tests_properties(region_obj_map_overlap_3D_mpi PROPERTIES LABELS parallel )
    set_tests_properties(obj_round_robin_io     PROPERTIES LABELS parallel )
    set_tests_properties(pdc_init_mpi     PROPERTIES LABELS parallel )
    set_tests_properties(create_prop_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(set_prop_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(dup_prop_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(create_cont_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(open_cont_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(cont_life_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(cont_iter_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(cont_info_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(cont_getid_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(cont_tags_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(create_obj_mpi   PROPERTIES LABELS parallel )
    set_tests_properties(open_obj_mpi     PROPERTIES LABELS parallel )
    set_tests_properties(obj_iter_mpi     PROPERTIES LABELS parallel )
    set_tests_properties(obj_life_mpi     PROPERTIES LABELS parallel )
    set_tests_properties(obj_dim_mpi      PROPERTIES LABELS parallel )
    set_tests_properties(obj_buf_mpi      PROPERTIES LABELS parallel )
    set_tests_properties(obj_tags_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(obj_info_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(obj_put_data_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(obj_get_data_mpi    PROPERTIES LABELS parallel )
    set_tests_properties(create_region_mpi  PROPERTIES LABELS parallel )
    set_tests_properties(write_obj_mpi  PROPERTIES LABELS parallel )
endif()
