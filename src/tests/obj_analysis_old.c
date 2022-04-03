/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>

#undef NDEBUG
#include <assert.h>

#include "pdc.h"

int main(int argc, char **argv)
{
  int rank = 0, size = 1;
  MPI_Comm comm;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_dup(MPI_COMM_WORLD, &comm);

  uint64_t array1_dims[2] = {4,4};
  int myArray1[4][4]   =
  { {1,  2, 3, 4},
    {5,  6, 7, 8},
    {9, 10,11,12},
    {13,14,15,16} };

  int *buf = (int *)calloc(4*4, sizeof(int));
  for(int i = 0; i < 4; i++)
  {
    for(int j = 0; j < 4; j++)
    {
      buf[i*4+j] = i*4 + j;
    }
  }

  // create a pdc
  pdcid_t pdc_id = PDCinit("pdc");
  printf("pdc_id: 0x%" PRIu64 "\n", pdc_id);

  // create a container property
  pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
  printf("cont_prop: 0x%" PRIu64 "\n", cont_prop);
  assert(cont_prop > 0);

  // create a container (collectively)
  pdcid_t cont_id = PDCcont_create_col("c1", cont_prop);
  printf("cont_id: 0x%" PRIu64 "\n", cont_id);
  assert(cont_id > 0);

  // create object properties for input and results
  pdcid_t obj1_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
  printf("obj1_prop: 0x%" PRIu64 "\n", obj1_prop);

  PDCprop_set_obj_dims     (obj1_prop, 2, array1_dims);
  PDCprop_set_obj_type     (obj1_prop, PDC_INT );
  PDCprop_set_obj_time_step(obj1_prop, 0       );
  PDCprop_set_obj_user_id  (obj1_prop, getuid());
  PDCprop_set_obj_app_name (obj1_prop, "object_analysis" );
  PDCprop_set_obj_tags(     obj1_prop, "tag0=1");
  // PDCprop_set_obj_buf      (obj1_prop, &buf[0]);

  pdcid_t obj2_prop = PDCprop_obj_dup(obj1_prop);
  printf("obj2_prop: 0x%" PRIu64 "\n", obj2_prop);
  PDCprop_set_obj_type(obj2_prop, PDC_INT);

  // pdcid_t obj1 = PDCobj_create(cont_id, "obj-var-array1", obj1_prop);
  pdcid_t obj1 = PDCobj_create_mpi(cont_id, "obj-var-array1", obj1_prop, 0, comm);
  printf("obj1: 0x%" PRIu64 "\n", obj1);
  assert(obj1 != 0);

  // pdcid_t obj2 = PDCobj_create(cont_id, "obj-var-result1", obj2_prop);
  pdcid_t obj2 = PDCobj_create_mpi(cont_id, "obj-var-result1", obj2_prop, 0, comm);
  printf("obj2: 0x%" PRIu64 "\n", obj2);
  assert(obj2 != 0);

  int       ndim = 2;

  uint64_t *offset = (uint64_t *)calloc(ndim, sizeof(uint64_t));
  uint64_t *offset_remote = (uint64_t *)calloc(ndim, sizeof(uint64_t));

  uint64_t *mysize = (uint64_t *)calloc(ndim, sizeof(uint64_t));
  mysize[0] = 4;
  mysize[1] = 4;

  pdcid_t region_1 = PDCregion_create(ndim, offset, mysize);
  pdcid_t region_11 = PDCregion_create(ndim, offset_remote, mysize);
  printf("region_1 id: %" PRIu64 "\n", region_1);
  printf("region_11 id: %" PRIu64 "\n", region_11);

  pdcid_t region_2 = PDCregion_create(ndim, offset, mysize);
  pdcid_t region_22 = PDCregion_create(ndim, offset_remote, mysize);
  printf("region_2 id: %" PRIu64 "\n", region_2);
  printf("region_22 id: %" PRIu64 "\n", region_22);

  MPI_Barrier(MPI_COMM_WORLD);

  pdcid_t region1_iter = PDCobj_data_iter_create(obj1, region_11);
  printf("region1_iter: %" PRIu64 "\n", region1_iter);

  pdcid_t region2_iter = PDCobj_data_iter_create(obj2, region_11);
  printf("region2_iter: %" PRIu64 "\n", region2_iter);

  /* Need to register the analysis function PRIOR TO establishing
   * the region mapping.  The obj_mapping will update the region
   * structures to indicate that an analysis operation has been
   * added to the lock release...
   */
  PDCobj_analysis_register("demo_sum", region1_iter, region2_iter);

  perr_t ret = SUCCEED;
//   // ret = PDCbuf_obj_map(&myArray1[0], PDC_INT, region_1, obj1, region_11);
   ret = PDCbuf_obj_map(&buf[0], PDC_INT, region_1, obj1, region_11);
   assert(ret == SUCCEED);
 
//   ret = PDCbuf_obj_map(&buf[0], PDC_INT, region_2, obj2, region_22);
//   assert(ret == SUCCEED);
 
   //  Simple test of invoking a function dynamically
 
   ret = PDCreg_obtain_lock(obj1, region_11, PDC_WRITE, PDC_NOBLOCK);
   // assert(ret == SUCCEED);
 
   ret = PDCreg_release_lock(obj1, region_11, PDC_WRITE);
   // assert(ret == SUCCEED);
 
//  ret = PDCreg_obtain_lock(obj2, region_22, PDC_WRITE, PDC_BLOCK);
//  assert(ret == SUCCEED);
// //
//  ret = PDCreg_release_lock(obj2, region_22, PDC_WRITE);
//  assert(ret == SUCCEED);

  size_t data_dims[2] = {0,0};
  // pdcid_t obj2_iter = PDCobj_data_block_iterator_create(obj2, PDC_REGION_ALL, 3);
  // pdcid_t obj2_iter = PDCobj_data_block_iterator_create(obj2, region_2, 3);

  // pdcid_t obj1_iter = PDCobj_data_iter_create(obj1, region_1);
  pdcid_t obj1_iter = PDCobj_data_iter_create(obj1, region_11);
  printf("obj1_iter: %" PRIu64"\n", obj1_iter);

  size_t number_of_slices = PDCobj_data_getSliceCount(obj1_iter);
  printf("number_of_slices: %d\n", number_of_slices);
  int *int_checkdata = NULL;
  size_t elements_per_block = PDCobj_data_getNextBlock(obj1_iter, (void **)&int_checkdata, data_dims);
  printf("elements_per_block: %d\n", elements_per_block);

  if (elements_per_block) {
    size_t slice, k;
    puts("\ntesting: 'obj1_iter' --------");
    printf("Total # of slices available = %ld\n",number_of_slices);
    printf("elements_per_slice = %ld, data_dims[0] = %ld, data_dims[1] = %ld\n", elements_per_block, data_dims[0], data_dims[1]);
    puts("data:");
    /* Should be: {6,7}, {10,11} */
    for(slice = 0; slice < number_of_slices; slice++) {
      for (k=0; k < elements_per_block; k++) {
        printf(" %4.2d", int_checkdata[k]);
      }
      puts("");
      PDCobj_data_getNextBlock(obj1_iter, (void **)&int_checkdata, NULL);
    }
    puts("\n");
  }

  // size_t elements_per_block;
  int *checkData = NULL;
  while((elements_per_block = PDCobj_data_getNextBlock(obj1_iter, (void **)&checkData, data_dims)) > 0)
  {
    printf("data_dims[0]: %d\n", data_dims[0]);
    size_t rows,column;
    int *next = checkData;
    for(rows=0; rows < data_dims[0]; rows++)
    {
      printf("\t%d\t%d\t%d\n", next[0], next[1], next[2]);
      next += 3;
    }
    puts("----------");
  }

  pdcid_t region11_iter = PDCobj_data_iter_create(obj1, region_11);
  printf("region11_iter: %" PRIu64 "\n", region11_iter);

 ret = PDCbuf_obj_unmap(obj1, region_11);
 assert(ret == SUCCEED);

  ret = PDCobj_close(obj1);
  assert(ret == SUCCEED);

  ret = PDCobj_close(region_1);
  assert(ret == SUCCEED);

  ret = PDCobj_close(region_11);
  assert(ret == SUCCEED);

  // close a container
  if(PDCcont_close(cont_id) < 0)
    printf("fail to close container %lld\n", cont_id);

  // close a container property
  if(PDCprop_close(cont_prop) < 0)
    printf("Fail to close property @ line %d\n", __LINE__);

  if(PDCclose(pdc_id) < 0)
    printf("fail to close PDC\n");

  free(buf);

  return 0;
}
