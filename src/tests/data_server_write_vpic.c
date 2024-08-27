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
#include <math.h>
#include "pdc.h"
#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"

#define NUM_VAR       8
#define NUM_FLOAT_VAR 6
#define NUM_INT_VAR   2
#define NDIM          1
#define NPARTICLES    8388608
#define XDIM          64
#define YDIM          64
#define ZDIM          64

float
uniform_random_number()
{
    return (((float)rand()) / ((float)(RAND_MAX)));
}

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1, i;
    perr_t  ret;
    pdcid_t pdc_id, cont_prop, cont_id;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#else
    printf("MPI NOT Enabled!\n");
    fflush(stdout);
#endif

    char *obj_names[] = {"x", "y", "z", "px", "py", "pz", "id1", "id2"};

    pdcid_t                obj_ids[NUM_VAR];
    struct pdc_region_info obj_regions[NUM_VAR];
    pdc_metadata_t *       obj_metas[NUM_VAR];

    pdcid_t obj_prop_float, obj_prop_int;

    uint64_t float_bytes = NPARTICLES * sizeof(float);
    uint64_t int_bytes   = NPARTICLES * sizeof(int);

    uint64_t float_dims[NDIM] = {float_bytes * size};
    uint64_t int_dims[NDIM]   = {int_bytes * size};

    uint64_t myoffset[NDIM], mysize[NDIM];
    void *   mydata[NUM_VAR];

    int write_var = NUM_VAR;

    struct pdc_request request[NUM_VAR];

    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    struct timeval pdc_timer_start_1;
    struct timeval pdc_timer_end_1;

    double sent_time = 0.0, sent_time_total = 0.0, wait_time = 0.0, wait_time_total = 0.0;
    double write_time = 0.0, total_size = 0.0;

    // Float vars are first in the array follow by int vars
    for (i = 0; i < NUM_FLOAT_VAR; i++)
        mydata[i] = (void *)malloc(float_bytes);

    for (; i < NUM_VAR; i++)
        mydata[i] = (void *)malloc(int_bytes);

    if (argc > 1)
        write_var = atoi(argv[1]);

    if (write_var < 0 || write_var > 8)
        write_var = NUM_VAR;

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("VPIC_cont", cont_prop);
    if (cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create object property for float and int
    obj_prop_float = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    obj_prop_int   = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop_float, 1, float_dims);
    PDCprop_set_obj_type(obj_prop_float, PDC_FLOAT);
    PDCprop_set_obj_time_step(obj_prop_float, 0);
    PDCprop_set_obj_user_id(obj_prop_float, getuid());
    PDCprop_set_obj_app_name(obj_prop_float, "VPICIO");
    PDCprop_set_obj_tags(obj_prop_float, "tag0=1");

    PDCprop_set_obj_dims(obj_prop_int, 1, int_dims);
    PDCprop_set_obj_type(obj_prop_int, PDC_INT);
    PDCprop_set_obj_time_step(obj_prop_int, 0);
    PDCprop_set_obj_user_id(obj_prop_int, getuid());
    PDCprop_set_obj_app_name(obj_prop_int, "VPICIO");
    PDCprop_set_obj_tags(obj_prop_int, "tag0=1");

    // Create obj and region one by one
    for (i = 0; i < NUM_FLOAT_VAR; i++) {
        if (rank == 0) {
            obj_ids[i] = PDCobj_create(cont_id, obj_names[i], obj_prop_float);
            if (obj_ids[i] <= 0) {
                printf("Error getting an object %s from server, exit...\n", obj_names[i]);
                goto done;
            }
        }
        myoffset[0]           = rank * float_bytes;
        mysize[0]             = float_bytes;
        obj_regions[i].ndim   = NDIM;
        obj_regions[i].offset = myoffset;
        obj_regions[i].size   = mysize;
    }

    // Continue to create the int vars id1 and id2
    for (; i < NUM_VAR; i++) {
        if (rank == 0) {
            obj_ids[i] = PDCobj_create(cont_id, obj_names[i], obj_prop_int);
            if (obj_ids[i] <= 0) {
                printf("Error getting an object %s from server, exit...\n", obj_names[i]);
                goto done;
            }
        }
        myoffset[0]           = rank * int_bytes;
        mysize[0]             = int_bytes;
        obj_regions[i].ndim   = NDIM;
        obj_regions[i].offset = myoffset;
        obj_regions[i].size   = mysize;
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    for (i = 0; i < NUM_VAR; i++) {
#ifdef ENABLE_MPI
        ret = PDC_Client_query_metadata_name_timestep_agg(obj_names[i], 0, &obj_metas[i]);
#else
        ret = PDC_Client_query_metadata_name_timestep(obj_names[i], 0, &obj_metas[i]);
#endif
        if (ret != SUCCEED || obj_metas[i] == NULL || obj_metas[i]->obj_id == 0) {
            printf("Error with metadata!\n");
            exit(-1);
        }
    }

    for (i = 0; i < NPARTICLES; i++) {
        ((float *)mydata[0])[i] = uniform_random_number() * XDIM; // x
        ((float *)mydata[1])[i] = uniform_random_number() * YDIM; // y
        ((float *)mydata[2])[i] = (i * 1.0 / NPARTICLES) * ZDIM;  // z
        ((float *)mydata[3])[i] = uniform_random_number() * XDIM; // px
        ((float *)mydata[4])[i] = uniform_random_number() * YDIM; // py
        ((float *)mydata[5])[i] = (i * 2.0 / NPARTICLES) * ZDIM;  // pz
        ((int *)mydata[6])[i]   = i;                              // id1
        ((int *)mydata[7])[i]   = i * 2;                          // id2
    }

    if (rank == 0)
        printf("Write out %d variables\n", write_var);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    // Timing
    gettimeofday(&pdc_timer_start, 0);

    for (i = 0; i < write_var; i++) {

        // Timing
        gettimeofday(&pdc_timer_start_1, 0);

        request[i].n_client = (size / 31 == 0 ? 1 : 31);
        request[i].n_update = write_var;
        ret                 = PDC_Client_iwrite(obj_metas[i], &obj_regions[i], &request[i], mydata[i]);
        if (ret != SUCCEED) {
            printf("Error with PDC_Client_iwrite!\n");
            goto done;
        }

        gettimeofday(&pdc_timer_end_1, 0);
        sent_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
        sent_time_total += sent_time;
    }

    if (rank == 0)
        printf("Finished sending all write requests, now waiting\n");

    gettimeofday(&pdc_timer_start_1, 0);

    for (i = 0; i < write_var; i++) {
        ret = PDC_Client_wait(&request[i], 200000, 500);
        if (ret != SUCCEED) {
            printf("Error with PDC_Client_wait!\n");
            goto done;
        }
    }
    gettimeofday(&pdc_timer_end_1, 0);
    wait_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
    wait_time_total += wait_time;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&pdc_timer_end, 0);
    write_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    total_size = NPARTICLES * 4.0 * 8.0 * size / 1024.0 / 1024.0;
    if (rank == 0) {
        printf("Write %f MB data with %d ranks\nTotal write time: %.2f\nSent %.2f, wait %.2f, Throughput "
               "%.2f MB/s\n",
               total_size, size, write_time, sent_time_total, wait_time_total, total_size / write_time);
        fflush(stdout);
    }

done:
    if (PDCprop_close(obj_prop_float) < 0)
        printf("Fail to close float obj property \n");

    if (PDCprop_close(obj_prop_int) < 0)
        printf("Fail to close int obj property \n");

    if (PDCcont_close(cont_id) < 0)
        printf("Fail to close container\n");

    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close container property\n");

    if (PDCclose(pdc_id) < 0)
        printf("Fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
