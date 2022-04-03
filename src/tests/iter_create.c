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

#ifndef ENABLE_MPI
#define ENABLE_MPI 1
#endif

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

static char *rand_string(char *str, size_t size)
{
  const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  size_t n;
  if (size) {
    --size;
    for (n = 0; n < size; n++) {
      int key = rand() % (int) (sizeof(charset) - 1);
      str[n] = charset[key];
    }
    str[size] = '\0';
  }
  return str;
}

#if 0
size_t myfunc2(float *stencil[3], float *out, size_t dims[2])
{
  size_t k, elements = dims[1];
  size_t last = elements-1;
  for(k = 0; k < elements; k++) {
    /* Edge cases first */
    if (k == 0)
      out[k] = stencil[0][0] + stencil[2][1];
    else if (k == last)
      out[k] = stencil[0][last-1] + stencil[2][last];
    /* The normal case is last */
    else
      out[k] = stencil[0][k-1] + stencil[2][k+1];
  }
  //    printf("out[0-9] = %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", out[0],out[1],out[2],out[3],out[4],out[5],out[6],out[7],out[8],out[9]);
  return 0;
}

//  ArrayUDF::
//   return (c(0,0)+c(0,-1)+c(0,1)+c(1,0)+c(-1,0)) * 5.0;
size_t myfunc3(float *stencil[3], float *out, size_t dims[2])
{
  size_t k, elements = dims[1];
  size_t last = elements-1;
  for(k = 0; k < elements; k++) {
    /* Edge cases first */
    if (k == 0)   /* self       +    above      +   below       +    left           + right */
      out[k] = (stencil[1][0] + stencil[0][0] + stencil[2][0] + stencil[1][0] + stencil[1][1]) * 5;
    else if (k == last)
      out[k] = (stencil[1][last] + stencil[0][last] + stencil[2][last] + stencil[1][last-1] + stencil[1][last]) *5;
    /* The normal case is last */
    else
      out[k] = (stencil[1][k] + stencil[0][k] + stencil[2][k] + stencil[1][k-1] + stencil[1][k+1]) * 5;
  }
  // printf("out[0-9] = %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", out[0],out[1],out[2],out[3],out[4],out[5],out[6],out[7],out[8],out[9]);
  return 0;
}

size_t arrayudf_stencils(pdcid_t iterIn, pdcid_t iterOut)
{
  float *dataIn = NULL;
  float *dataOut = NULL;
  float *stencil[3] = {NULL, NULL, NULL};
  size_t dimsIn[2] = {0,};
  size_t dimsOut[2] = {0,};
  size_t k, blockLengthOut, blockLengthIn, number_of_slices;
  size_t result = 0;
  double start, end, total;

  start = MPI_Wtime();
  // printf("Entered: %s\n----------------\n", __func__);
  number_of_slices = PDCobj_data_getSliceCount(iterOut);
  // printf("Total # of slices available = %ld\n",number_of_slices);
  if ((blockLengthIn = PDCobj_data_getNextBlock(iterIn, (void **)&dataIn, dimsIn)) == 0)
    printf("Empty Input!\n");
  stencil[0] = stencil[1] = dataIn;
  for (k=0; k< number_of_slices; k++) {
    if ((blockLengthOut = PDCobj_data_getNextBlock(iterOut, (void **)&dataOut, dimsOut)) > 0) {
      if ((blockLengthIn = PDCobj_data_getNextBlock(iterIn, (void **)&dataIn, dimsIn)) > 0) {
        stencil[2] = dataIn;
        if (myfunc3(stencil,dataOut,dimsIn) == 0) {
          stencil[0] = stencil[1];
          stencil[1] = stencil[2];
        }
      }
      else {
        result = myfunc3(stencil,dataOut,dimsOut);
      }
    }
  }
  end = MPI_Wtime();
  total = end-start;

  printf("Leaving: %s execution time = %lf seconds\n----------------\n", __func__, total);
  return result;
}
#endif	/* #if 0 */


/* Sum the elements of a row to produce a single output */

int main(int argc, char **argv)
{

#ifdef ENABLE_MPI
  MPI_Comm comm;
#endif
  int rank = 0, size = 1;
  perr_t ret = SUCCEED;
  pdcid_t pdc_id, cont_prop, cont_id;
  pdcid_t obj_prop, obj1_prop, obj2_prop, obj3_prop, obj4_prop;
  pdcid_t result1_prop, result2_prop;
  pdcid_t testobj, obj1, obj2, obj3, obj4;
  pdcid_t testreg, r1, r2, r3, r4;
  pdcid_t result1;
  pdcid_t input1_iter, result1_iter;
  pdcid_t input3d_iter, result3d_iter;
  size_t  number_of_slices, elements_per_block;
  size_t  data_dims[4] = {0,};

  uint64_t offset[2] = {1, 1};
  uint64_t rdims[2] = {2, 2};
  uint64_t array_dims[2] = {4,4};
  int myArray[4][4]    = { {1,  2, 3, 4},
    {5,  6, 7, 8},
    {9, 10,11,12},
    {13,14,15,16} };

  int *int_checkdata = NULL;


  uint64_t offset0[2] = {0, 0};
  uint64_t myTestArray_dims[2] = {10,10};
  float myTestArray[10][10] =
  {
    { 83.5, 86.5, 77.5, 15.5, 93.5, 35.5, 86.5, 92.5, 49.5, 21.5 },
    { 62.5, 27.5, 90.5, 59.5, 63.5, 26.5, 40.5, 26.5, 72.5, 36.5 },
    { 11.5, 68.5, 67.5, 29.5, 82.5, 30.5, 62.5, 23.5, 67.5, 35.5 },
    { 29.5, 2.5, 22.5, 58.5, 69.5, 67.5, 93.5, 56.5, 11.5, 42.5  },
    { 29.5, 73.5, 21.5, 19.5, 84.5, 37.5, 98.5, 24.5, 15.5, 70.5 },
    { 13.5, 26.5, 91.5, 80.5, 56.5, 73.5, 62.5, 70.5, 96.5, 81.5 },
    { 5.5, 25.5, 84.5, 27.5, 36.5, 5.5, 46.5, 29.5, 13.5, 57.5   },
    { 24.5, 95.5, 82.5, 45.5, 14.5, 67.5, 34.5, 64.5, 43.5, 50.5 },
    { 87.5, 8.5, 76.5, 78.5, 88.5, 84.5, 3.5, 51.5, 54.5, 99.5   },
    { 32.5, 60.5, 76.5, 68.5, 39.5, 12.5, 26.5, 86.5, 94.5, 39.5 }
  };

  uint64_t offsets3d[3] = {0, 0, 0};
  uint64_t my3D_dims[3] = {10, 10, 10};
  int my3DTestArray[10][10][10] =
  {
    /* 0 */
    {
      {83, 86, 77, 15, 93, 35, 86, 92, 49, 21},
      {62, 27, 90, 59, 63, 26, 40, 26, 72, 36},
      {11, 68, 67, 29, 82, 30, 62, 23, 67, 35},
      {29, 2, 22, 58, 69, 67, 93, 56, 11, 42},
      {29, 73, 21, 19, 84, 37, 98, 24, 15, 70},
      {13, 26, 91, 80, 56, 73, 62, 70, 96, 81},
      {5, 25, 84, 27, 36, 5, 46, 29, 13, 57},
      {24, 95, 82, 45, 14, 67, 34, 64, 43, 50},
      {87, 8, 76, 78, 88, 84, 3, 51, 54, 99},
      {32, 60, 76, 68, 39, 12, 26, 86, 94, 39}
    },
    /* 1 */
    {
      {95, 70, 34, 78, 67, 1, 97, 2, 17, 92},
      {52, 56, 1, 80, 86, 41, 65, 89, 44, 19},
      {40, 29, 31, 17, 97, 71, 81, 75, 9, 27},
      {67, 56, 97, 53, 86, 65, 6, 83, 19, 24},
      {28, 71, 32, 29, 3, 19, 70, 68, 8, 15},
      {40, 49, 96, 23, 18, 45, 46, 51, 21, 55},
      {79, 88, 64, 28, 41, 50, 93, 0, 34, 64},
      {24, 14, 87, 56, 43, 91, 27, 65, 59, 36},
      {32, 51, 37, 28, 75, 7, 74, 21, 58, 95},
      {29, 37, 35, 93, 18, 28, 43, 11, 28, 29}
    },
    /* 2 */
    {
      {76, 4, 43, 63, 13, 38, 6, 40, 4, 18},
      {28, 88, 69, 17, 17, 96, 24, 43, 70, 83},
      {90, 99, 72, 25, 44, 90, 5, 39, 54, 86},
      {69, 82, 42, 64, 97, 7, 55, 4, 48, 11},
      {22, 28, 99, 43, 46, 68, 40, 22, 11, 10},
      {5, 1, 61, 30, 78, 5, 20, 36, 44, 26},
      {22, 65, 8, 16, 82, 58, 24, 37, 62, 24},
      {0, 36, 52, 99, 79, 50, 68, 71, 73, 31},
      {81, 30, 33, 94, 60, 63, 99, 81, 99, 96},
      {59, 73, 13, 68, 90, 95, 26, 66, 84, 40}
    },
    /* 3 */
    {
      {90, 84, 76, 42, 36, 7, 45, 56, 79, 18},
      {87, 12, 48, 72, 59, 9, 36, 10, 42, 87},
      {6, 1, 13, 72, 21, 55, 19, 99, 21, 4},
      {39, 11, 40, 67, 5, 28, 27, 50, 84, 58},
      {20, 24, 22, 69, 96, 81, 30, 84, 92, 72},
      {72, 50, 25, 85, 22, 99, 40, 42, 98, 13},
      {98, 90, 24, 90, 9, 81, 19, 36, 32, 55},
      {94, 4, 79, 69, 73, 76, 50, 55, 60, 42},
      {79, 84, 93, 5, 21, 67, 4, 13, 61, 54},
      {26, 59, 44, 2, 2, 6, 84, 21, 42, 68}
    },
    /* 4 */
    {
      {28, 89, 72, 8, 58, 98, 36, 8, 53, 48},
      {3, 33, 33, 48, 90, 54, 67, 46, 68, 29},
      {0, 46, 88, 97, 49, 90, 3, 33, 63, 97},
      {53, 92, 86, 25, 52, 96, 75, 88, 57, 29},
      {36, 60, 14, 21, 60, 4, 28, 27, 50, 48},
      {56, 2, 94, 97, 99, 43, 39, 2, 28, 3},
      {0, 81, 47, 38, 59, 51, 35, 34, 39, 92},
      {15, 27, 4, 29, 49, 64, 85, 29, 43, 35},
      {77, 0, 38, 71, 49, 89, 67, 88, 92, 95},
      {43, 44, 29, 90, 82, 40, 41, 69, 26, 32}
    },
    /* 5 */
    {
      {61, 42, 60, 17, 23, 61, 81, 9, 90, 25},
      {96, 67, 77, 34, 90, 26, 24, 57, 14, 68},
      {5, 58, 12, 86, 0, 46, 26, 94, 16, 52},
      {78, 29, 46, 90, 47, 70, 51, 80, 31, 93},
      {57, 27, 12, 86, 14, 55, 12, 90, 12, 79},
      {10, 69, 89, 74, 55, 41, 20, 33, 87, 88},
      {38, 66, 70, 84, 56, 17, 6, 60, 49, 37},
      {5, 59, 17, 18, 45, 83, 73, 58, 73, 37},
      {89, 83, 7, 78, 57, 14, 71, 29, 0, 59},
      {18, 38, 25, 88, 74, 33, 57, 81, 93, 58}
    },
    /* 6 */
    {
      {70, 99, 17, 39, 69, 63, 22, 94, 73, 47},
      {31, 62, 82, 90, 92, 91, 57, 15, 21, 57},
      {74, 91, 47, 51, 31, 21, 37, 40, 54, 30},
      {98, 25, 81, 16, 16, 2, 31, 39, 96, 4},
      {38, 80, 18, 21, 70, 62, 12, 79, 77, 85},
      {36, 4, 76, 83, 7, 59, 57, 44, 99, 11},
      {27, 50, 36, 60, 18, 5, 63, 49, 44, 11},
      {5, 34, 91, 75, 55, 14, 89, 68, 93, 18},
      {5, 82, 22, 82, 17, 30, 93, 74, 26, 93},
      {86, 53, 43, 74, 14, 13, 79, 77, 62, 75}
    },
    /* 7 */
    {
      {88, 19, 10, 32, 94, 17, 46, 35, 37, 91},
      {53, 43, 73, 28, 25, 91, 10, 18, 17, 36},
      {63, 55, 90, 58, 30, 4, 71, 61, 33, 85},
      {89, 73, 4, 51, 5, 50, 68, 3, 85, 6},
      {95, 39, 49, 20, 67, 26, 63, 77, 96, 81},
      {65, 60, 36, 55, 70, 18, 11, 42, 32, 96},
      {79, 21, 70, 84, 72, 27, 34, 40, 83, 72},
      {98, 30, 63, 47, 50, 30, 73, 14, 59, 22},
      {47, 24, 82, 35, 32, 4, 54, 43, 98, 86},
      {40, 78, 59, 62, 62, 83, 41, 48, 23, 24}
    },
    /* 8 */
    {
      {72, 22, 54, 35, 21, 57, 65, 47, 71, 76},
      {69, 18, 1, 3, 53, 33, 7, 59, 28, 6},
      {97, 20, 84, 8, 34, 98, 91, 76, 98, 15},
      {52, 71, 89, 59, 6, 10, 16, 24, 9, 39},
      {0, 78, 9, 53, 81, 14, 38, 89, 26, 67},
      {47, 23, 87, 31, 32, 22, 81, 75, 50, 79},
      {90, 54, 50, 31, 13, 57, 94, 81, 81, 3},
      {20, 33, 82, 81, 87, 15, 96, 25, 4, 22},
      {92, 51, 97, 32, 34, 81, 6, 15, 57, 8},
      {95, 99, 62, 97, 83, 76, 54, 77, 9, 87}
    },
    /* 9 */
    {
      {32, 82, 21, 66, 63, 60, 82, 11, 85, 86},
      {85, 30, 90, 83, 14, 76, 16, 20, 92, 25},
      {28, 39, 25, 90, 36, 60, 18, 43, 37, 28},
      {82, 21, 10, 55, 88, 25, 15, 70, 37, 53},
      {8, 22, 83, 50, 57, 97, 27, 26, 69, 71},
      {51, 49, 10, 28, 39, 98, 88, 10, 93, 77},
      {90, 76, 99, 52, 31, 87, 77, 99, 57, 66},
      {52, 17, 41, 35, 68, 98, 84, 95, 76, 5},
      {66, 28, 54, 28, 8, 93, 78, 97, 55, 72},
      {74, 45, 0, 25, 97, 83, 12, 27, 82, 21}
    }
  };

#ifdef ENABLE_MPI
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_dup(MPI_COMM_WORLD, &comm);
#endif

  // create a pdc
  pdc_id = PDCinit("pdc");
  /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

  // create a container property
  cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
  if(cont_prop <= 0)
    printf("Fail to create container property @ line  %d!\n", __LINE__);

  // create a container (collectively)
  cont_id = PDCcont_create_col("c1", cont_prop);
  if(cont_id <= 0)
    printf("Fail to create container @ line  %d!\n", __LINE__);

  // create object properties for input and results
  obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
  PDCprop_set_obj_buf      (obj_prop, &myArray[0][0]);
  PDCprop_set_obj_time_step(obj_prop, rank       );
  PDCprop_set_obj_user_id  (obj_prop, getuid()    );
  PDCprop_set_obj_app_name (obj_prop, "object_analysis" );
  PDCprop_set_obj_tags(     obj_prop, "tag0=array1" );
  PDCprop_set_obj_dims(     obj_prop, 2, array_dims);
  PDCprop_set_obj_type(     obj_prop, PDC_INT);

  testobj = PDCobj_create(cont_id, "testobj", obj_prop);
  if (testobj < 0) {
    printf("Error getting an object id from server, exit...\n");
    exit(-1);
  }

  // create a region
  testreg = PDCregion_create(2, offset, rdims);
  pdcid_t test_iter = PDCobj_data_iter_create(testobj, testreg);

  number_of_slices = PDCobj_data_getSliceCount(test_iter);
  elements_per_block = PDCobj_data_getNextBlock(test_iter, (void **)&int_checkdata, data_dims);
  printf("number_of_slices: %d\n", number_of_slices);
  printf("elements_per_block: %d\n", elements_per_block);

  if (elements_per_block) {
    size_t slice, k;
    puts("\ntesting: 'test_iter' --------");
    printf("Total # of slices available = %ld\n",number_of_slices);
    printf("elements_per_slice = %ld, data_dims[0] = %ld, data_dims[1] = %ld\n", elements_per_block, data_dims[0], data_dims[1]);
    puts("data:");
    /* Should be: {6,7}, {10,11} */
    for(slice = 0; slice < number_of_slices; slice++) {
      for (k=0; k < elements_per_block; k++) {
        printf(" %4.2d", int_checkdata[k]);
      }
      puts("");
      PDCobj_data_getNextBlock(test_iter, (void **)&int_checkdata, NULL);
    }
    puts("\n");
  }

  PDCprop_close(obj_prop);
  PDCobj_close(testobj);
  PDCregion_close(testreg);

  // create object properties for input and results
  obj1_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

  PDCprop_set_obj_dims     (obj1_prop, 2, myTestArray_dims);
  PDCprop_set_obj_type     (obj1_prop, PDC_FLOAT );
  PDCprop_set_obj_time_step(obj1_prop, 0       );
  PDCprop_set_obj_user_id  (obj1_prop, getuid());
  PDCprop_set_obj_app_name (obj1_prop, "iter_create" );
  PDCprop_set_obj_tags     (obj1_prop, "tag0=1");
  PDCprop_set_obj_buf      (obj1_prop, &myTestArray[0][0]);

  // Duplicate the properties from 'obj1_prop' into 'obj2_prop'
  obj2_prop = PDCprop_obj_dup(obj1_prop);
  PDCprop_set_obj_type(obj2_prop, PDC_FLOAT); /* Dup doesn't replicate the datatype */

  obj1 = PDCobj_create_mpi(cont_id, "obj-var-array1", obj1_prop, 0, comm);
  if (obj1 == 0) {
    printf("Error getting an object id of %s from server, exit...\n", "obj-var-array1");
    exit(-1);
  }

  obj2 = PDCobj_create_mpi(cont_id, "obj-var-result1", obj2_prop, 0, comm);
  if (obj2 == 0) {
    printf("Error getting an object id of %s from server, exit...\n", "obj-var-result1");
    exit(-1);
  }

  // create regions
  r1 = PDCregion_create(2, offset0, myTestArray_dims);
  r2 = PDCregion_create(2, offset0, myTestArray_dims);

  input1_iter = PDCobj_data_iter_create(obj1, r1);
  result1_iter = PDCobj_data_iter_create(obj2, r2);

  float *checkData = NULL;
  number_of_slices = PDCobj_data_getSliceCount(input1_iter);
  elements_per_block = PDCobj_data_getNextBlock(input1_iter, (void **)&checkData, data_dims);
  printf("number_of_slices: %d\n", number_of_slices);
  printf("elements_per_block: %d\n", elements_per_block);

  if (elements_per_block) {
    size_t k;
    puts("\ntesting: 'input1_iter' --------");
    printf("Total # of slices available = %ld\n",number_of_slices);
    printf("elements_per_slice = %ld, data_dims[0] = %ld, data_dims[1] = %ld\n", elements_per_block, data_dims[0], data_dims[1]);
    puts("data:");
    for (k=0; k < elements_per_block; k++) {
      printf(" %4.1f", checkData[k]);
    }
    puts("\n");
  }

  /* ----------------- 3D test ----------------- */
  // create object properties for input and results
  obj3_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

  PDCprop_set_obj_dims     (obj3_prop, 3, my3D_dims);
  PDCprop_set_obj_type     (obj3_prop, PDC_INT );
  PDCprop_set_obj_time_step(obj3_prop, 0       );
  PDCprop_set_obj_user_id  (obj3_prop, getuid());
  PDCprop_set_obj_app_name (obj3_prop, "iter_create" );
  PDCprop_set_obj_tags     (obj3_prop, "tag0=1");
  PDCprop_set_obj_buf      (obj3_prop, my3DTestArray);

  // Duplicate the properties from 'obj1_prop' into 'obj2_prop'
  obj4_prop = PDCprop_obj_dup(obj3_prop);
  PDCprop_set_obj_type(obj4_prop, PDC_INT); /* Dup doesn't replicate the datatype */

  obj3 = PDCobj_create_mpi(cont_id, "obj-var-array-3d", obj3_prop, 0, comm);
  if (obj3 == 0) {
    printf("Error getting an object id of %s from server, exit...\n", "obj-var-array1");
    exit(-1);
  }

  obj4 = PDCobj_create_mpi(cont_id, "obj-var-result-3d", obj4_prop, 0, comm);
  if (obj4 == 0) {
    printf("Error getting an object id of %s from server, exit...\n", "obj-var-result1");
    exit(-1);
  }

  // create regions
  r3 = PDCregion_create(3, offsets3d, my3D_dims);
  r4 = PDCregion_create(3, offsets3d, my3D_dims);

  int_checkdata = NULL;

  input3d_iter = PDCobj_data_iter_create(obj3, r3);
  result3d_iter = PDCobj_data_iter_create(obj4, r4);

  number_of_slices = PDCobj_data_getSliceCount(input3d_iter);
  elements_per_block = PDCobj_data_getNextBlock(input3d_iter, (void **)&int_checkdata, data_dims);
  printf("number_of_slices: %d\n", number_of_slices);
  printf("elements_per_block: %d\n", elements_per_block);

  if (elements_per_block) {
    size_t row, col, k, slice;
    puts("\ntesting: 'input3d_iter' --------");
    printf("Total # of slices available = %ld\n",number_of_slices);
    printf("elements_per_slice = %ld, data_dims[0] = %ld, data_dims[1] = %ld, data_dims[2] = %ld\n", elements_per_block, data_dims[0], data_dims[1], data_dims[2]);
    puts("data:");
    for(slice = 0; slice < number_of_slices; slice++) {
      k = 0;
      printf("=== slice %d\n", (int)slice);
      for(row = 0; row < data_dims[1]; row++) {
        for(col = 0; col < data_dims[2]; col++) {
          printf(" %4.2d", int_checkdata[k++]);
        }
        puts("");
      }
      elements_per_block = PDCobj_data_getNextBlock(input3d_iter, (void **)&int_checkdata,NULL);
    }
  }

#ifdef ENABLE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif
  // close a container
  if(PDCcont_close(cont_id) < 0)
    printf("fail to close container %lld\n", cont_id);

  // close a container property
  if(PDCprop_close(cont_prop) < 0)
    printf("Fail to close property @ line %d\n", __LINE__);

  if(PDCclose(pdc_id) < 0)
    printf("fail to close PDC\n");

#ifdef ENABLE_MPI
  MPI_Finalize();
#endif

  return 0;
}
