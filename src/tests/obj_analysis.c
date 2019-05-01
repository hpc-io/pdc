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

/* #define ENABLE_MPI 1 */

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


/* Sum the elements of a row to produce a single output */

int main(int argc, char **argv)
{

#ifdef ENABLE_MPI
    MPI_Comm comm;
#endif
    int rank = 0, size = 1;
    perr_t ret = SUCCEED;
    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t obj1_prop, obj2_prop, obj3_prop;
    pdcid_t result1_prop, result2_prop;
    pdcid_t obj1, obj2;
    pdcid_t r1, r2;
    pdcid_t result1;
    pdcid_t result1_iter;
    pdcid_t input1_iter;

    int myresult1[2];     /*  sum{6,7}, sum{10,11} */

    uint64_t offset0[2] = {0, 0};
    uint64_t offset[2] = {1, 1};
    uint64_t rdims[2] = {2, 2};
    uint64_t array1_dims[2] = {4,4};
    int myArray1[4][4]   = { {1,  2, 3, 4}, 
			     {5,  6, 7, 8},
			     {9, 10,11,12},
                             {13,14,15,16} };

    uint64_t array2_dims[3] = {5,3,3};
    int myArray2[5][3][3] = { 
      {{10, 11, 12}, {13, 14, 15}, {16, 17, 18}},
      {{20, 21, 22}, {23, 24, 25}, {26, 27, 28}},
      {{30, 31, 32}, {33, 34, 35}, {36, 37, 38}},
      {{40, 41, 42}, {43, 44, 45}, {46, 47, 48}},
      {{50, 51, 42}, {53, 54, 55}, {56, 57, 58}}
    };

    uint64_t result1_dims[1] = {2};


#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
#endif

    // create a pdc
    pdc_id = PDC_init("pdc");
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
    obj1_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims     (obj1_prop, 2, array1_dims);
    PDCprop_set_obj_type     (obj1_prop, PDC_INT );
    PDCprop_set_obj_time_step(obj1_prop, 0       );
    PDCprop_set_obj_user_id  (obj1_prop, getuid());
    PDCprop_set_obj_app_name (obj1_prop, "object_analysis" );
    PDCprop_set_obj_tags(     obj1_prop, "tag0=1");

    // Duplicate the properties from 'obj1_prop' into 'obj2_prop'
    obj2_prop = PDCprop_obj_dup(obj1_prop);
    PDCprop_set_obj_type(obj2_prop, PDC_INT); /* Dup doesn't replicate the datatype */

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
    r1 = PDCregion_create(2, offset0, array1_dims);
    r2 = PDCregion_create(2, offset0, array1_dims);

    input1_iter = PDCobj_data_iter_create(obj1, r1);
    result1_iter = PDCobj_data_iter_create(obj2, r2);

    /* Need to register the analysis function PRIOR TO establishing
     * the region mapping.  The obj_mapping will update the region
     * structures to indicate that an analysis operation has been
     * added to the lock release...
     */
    PDCobj_analysis_register("demo_sum", input1_iter, result1_iter);

    ret = PDCbuf_obj_map(&myArray1[0], PDC_INT, r1, obj1, r2);

//  Simple test of invoking a function dynamically

    ret = PDCreg_obtain_lock(obj1, r1, WRITE, NOBLOCK);
    ret = PDCreg_release_lock(obj1, r1, WRITE);

    size_t data_dims[2] = {0,};
    pdcid_t obj2_iter = PDCobj_data_block_iterator_create(obj2, PDC_REGION_ALL, 3);
    size_t elements_per_block;
    int *checkData = NULL;
    while((elements_per_block = PDCobj_data_getNextBlock(obj2_iter, (void **)&checkData, data_dims)) > 0) {
      size_t rows,column;
      int *next = checkData;
      for(rows=0; rows < data_dims[0]; rows++) {
	printf("\t%d\t%d\t%d\n", next[0], next[1], next[2]);
	next += 3;
      }
      puts("----------");
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

    if(PDC_close(pdc_id) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
