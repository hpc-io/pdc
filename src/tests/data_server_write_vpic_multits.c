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

#define NUM_VAR_MAX       8
#define NUM_FLOAT_VAR_MAX 6
#define NUM_INT_VAR_MAX   2
#define TS_MAX            100
#define NDIM              1
#define XDIM              64
#define YDIM              64
#define ZDIM              64

char *obj_names[] = {"x", "y", "z", "px", "py", "pz", "id1", "id2"};

float
uniform_random_number()
{
    return (((float)rand()) / ((float)(RAND_MAX)));
}

void
usage(const char *name)
{
    printf(
        "Usage:\n./%s num_variable(up to 8) variable_size_per_proc_MB n_timestep compute_time\nexiting...\n",
        name);
}

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1, i, ts;
    perr_t  ret;
    pdcid_t pdc_id, cont_prop, cont_id;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    int      n_var = NUM_VAR_MAX, n_ts = 1;
    double   sleep_time    = 0, true_sleep_time;
    double   compute_total = 0.0, gen_time = 0.0;
    int      size_per_proc_var_MB = 32; // Default value to write 8388608 particles
    int      n_particles;
    uint64_t float_bytes, int_bytes;
    uint64_t float_dims[NDIM];
    uint64_t int_dims[NDIM];
    uint64_t myoffset[NDIM], mysize[NDIM];
    void *   mydata[NUM_VAR_MAX];

    pdcid_t                obj_prop_float, obj_prop_int;
    pdcid_t                obj_ids[TS_MAX][NUM_VAR_MAX];
    struct pdc_region_info obj_regions[TS_MAX][NUM_VAR_MAX];
    pdc_metadata_t *       obj_metas[TS_MAX][NUM_VAR_MAX];
    struct pdc_request     request[TS_MAX][NUM_VAR_MAX];

    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    struct timeval pdc_timer_start_1;
    struct timeval pdc_timer_start_2;
    struct timeval pdc_timer_end_1;
    struct timeval pdc_timer_end_2;

    double write_time = 0.0, write_time_total = 0.0, wait_time = 0.0, wait_time_total = 0.0;
    double create_time = 0.0, create_time_total = 0.0, query_time = 0.0, query_time_total = 0.0;
    double total_time = 0.0, total_size = 0.0;

    if (argc > 4) {
        n_var                = atoi(argv[1]);
        size_per_proc_var_MB = atoi(argv[2]);
        n_ts                 = atoi(argv[3]);
        sleep_time           = atof(argv[4]);
    }
    else {
        if (rank == 0)
            usage(argv[0]);
        goto exit;
    }
    // In VPIC-IO, each client writes 32MB per variable, 8 var per client, so 256MB per client
    n_particles   = size_per_proc_var_MB * 262144; // Convert to number of particles
    float_bytes   = n_particles * sizeof(float);
    int_bytes     = n_particles * sizeof(int);
    float_dims[0] = float_bytes * size;
    int_dims[0]   = int_bytes * size;

    if (n_var < 0 || n_var > 8)
        n_var = NUM_VAR_MAX;

    if (n_ts < 0)
        n_ts = 1;

    if (n_ts > TS_MAX)
        n_ts = TS_MAX;

    if (sleep_time < 0)
        sleep_time = 15;

    if (rank == 0) {
        printf("Write %d variables, each %dMB per proc, %d timesteps, %.1f compute time\n", n_var,
               size_per_proc_var_MB, n_ts, sleep_time);
        fflush(stdout);
    }

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
    PDCprop_set_obj_user_id(obj_prop_float, getuid());
    PDCprop_set_obj_app_name(obj_prop_float, "VPICIO");
    PDCprop_set_obj_tags(obj_prop_float, "tag0=1");

    PDCprop_set_obj_dims(obj_prop_int, 1, int_dims);
    PDCprop_set_obj_type(obj_prop_int, PDC_INT);
    PDCprop_set_obj_user_id(obj_prop_int, getuid());
    PDCprop_set_obj_app_name(obj_prop_int, "VPICIO");
    PDCprop_set_obj_tags(obj_prop_int, "tag0=1");

    // Float vars are first in the array follow by int vars
    for (i = 0; i < n_var; i++) {
        if (i < NUM_FLOAT_VAR_MAX)
            mydata[i] = (void *)malloc(float_bytes);
        else
            mydata[i] = (void *)malloc(int_bytes);
    }

    gettimeofday(&pdc_timer_start, 0);

    for (ts = 0; ts < n_ts; ts++) {
        gettimeofday(&pdc_timer_start_2, 0);
        // Generate data
        for (i = 0; i < n_particles; i++) {
            ((float *)mydata[0])[i] = uniform_random_number() * XDIM; // x
            if (n_var > 1) {
                ((float *)mydata[1])[i] = uniform_random_number() * YDIM; // y
                if (n_var > 2) {
                    ((float *)mydata[2])[i] = (i * 1.0 * ts / n_particles) * ZDIM; // z
                    if (n_var > 3) {
                        ((float *)mydata[3])[i] = uniform_random_number() * XDIM; // px
                        if (n_var > 4) {
                            ((float *)mydata[4])[i] = uniform_random_number() * YDIM; // py
                            if (n_var > 5) {
                                ((float *)mydata[5])[i] = (i * 2.0 * ts / n_particles) * ZDIM; // pz
                                if (n_var > 6) {
                                    ((int *)mydata[6])[i] = i * ts; // id1
                                    if (n_var > 7) {
                                        ((int *)mydata[7])[i] = i * 2 * ts; // id2
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        gettimeofday(&pdc_timer_start_1, 0);

        gen_time = PDC_get_elapsed_time_double(&pdc_timer_start_2, &pdc_timer_start_1);

        true_sleep_time = 0;
        if (gen_time < sleep_time) {
            true_sleep_time = sleep_time - gen_time;
        }

        if (rank == 0) {
            printf("Compute for %.2f seconds\n", gen_time + true_sleep_time);
        }
        fflush(stdout);

        // Sleep to fake compute time
        PDC_msleep((unsigned long)(true_sleep_time * 1000));
        compute_total += gen_time + true_sleep_time;

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        if (rank == 0) {
            printf("Compute done\n");
            fflush(stdout);
        }

        gettimeofday(&pdc_timer_start_1, 0);
        // Create obj and region one by one
        PDCprop_set_obj_time_step(obj_prop_float, ts);
        PDCprop_set_obj_time_step(obj_prop_int, ts);
        for (i = 0; i < n_var; i++) {
            if (i < NUM_FLOAT_VAR_MAX) {
                if (rank == 0) {
                    obj_ids[ts][i] = PDCobj_create(cont_id, obj_names[i], obj_prop_float);
                    if (obj_ids[ts][i] <= 0) {
                        printf("Error creating object %s, exit...\n", obj_names[i]);
                        goto done;
                    }
                }
                myoffset[0] = rank * float_bytes;
                mysize[0]   = float_bytes;
            }
            else {
                if (rank == 0) {
                    obj_ids[ts][i] = PDCobj_create(cont_id, obj_names[i], obj_prop_int);
                    if (obj_ids[ts][i] <= 0) {
                        printf("Error creating object %s, exit...\n", obj_names[i]);
                        goto done;
                    }
                }
                myoffset[0] = rank * int_bytes;
                mysize[0]   = int_bytes;
            } // end of else

            obj_regions[ts][i].ndim   = NDIM;
            obj_regions[ts][i].offset = myoffset;
            obj_regions[ts][i].size   = mysize;

        } // end of for

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif
        gettimeofday(&pdc_timer_end_1, 0);
        create_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
        create_time_total += create_time;

        if (rank == 0) {
            printf("%d: start querying objects\n", rank);
            fflush(stdout);
        }

        for (i = 0; i < n_var; i++) {
            ret = PDC_Client_query_metadata_name_timestep_agg(obj_names[i], ts, &obj_metas[ts][i]);
            if (ret != SUCCEED || obj_metas[ts][i] == NULL || obj_metas[ts][i]->obj_id == 0) {
                printf("Error with metadata! ts=%d\n", ts);
                exit(-1);
            }
        }

        gettimeofday(&pdc_timer_end_2, 0);
        query_time = PDC_get_elapsed_time_double(&pdc_timer_end_1, &pdc_timer_end_2);
        query_time_total += query_time;

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif
        if (rank == 0) {
            printf("%d: querying done\n", rank);
            fflush(stdout);
        }

        // Wait for the previous request to finish
        if (ts > 0 && ts != n_ts - 1) {
            // Timing
            gettimeofday(&pdc_timer_start_1, 0);

            for (i = 0; i < n_var; i++) {
                ret = PDC_Client_wait(&request[ts - 1][i], 30000, 100);
                if (ret != SUCCEED) {
                    printf("Error with PDC_Client_wait!\n");
                    goto done;
                }
            }
#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
#endif
            gettimeofday(&pdc_timer_end_1, 0);
            wait_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
            wait_time_total += wait_time;
        }

        if (rank == 0)
            printf("Timestep %d: start to write.\n", ts);
        fflush(stdout);

        // Last ts is sync IO
        if (ts != n_ts - 1) {
#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
#endif
            // Timing
            gettimeofday(&pdc_timer_start_1, 0);

            for (i = 0; i < n_var; i++) {
                request[ts][i].n_update = n_var;
                ret = PDC_Client_iwrite(obj_metas[ts][i], &obj_regions[ts][i], &request[ts][i], mydata[i]);
                if (ret != SUCCEED) {
                    printf("Error with PDC_Client_iwrite!\n");
                    goto done;
                }
            } // end of for

#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
#endif
            gettimeofday(&pdc_timer_end_1, 0);
            write_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
            write_time_total += write_time;
        }

        if (rank == 0)
            printf("Timestep %d: create time %.6f, query time %.6f, write time %.6f, wait time %.6f\n", ts,
                   create_time, query_time, write_time, wait_time);
    }

    // Perform last timestep write
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    // Timing
    gettimeofday(&pdc_timer_start_1, 0);

    for (i = 0; i < n_var; i++) {
        ret = PDC_Client_write(obj_metas[n_ts - 1][i], &obj_regions[n_ts - 1][i], mydata[i]);
        if (ret != SUCCEED) {
            printf("Error with PDC_Client_iwrite!\n");
            goto done;
        }
    } // end of for

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&pdc_timer_end_1, 0);
    write_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
    write_time_total += write_time;
    wait_time = write_time;
    wait_time_total += wait_time;

    if (rank == 0)
        printf("Timestep %d: write time %.2f, wait time %.2f\n", ts, write_time, wait_time);

    gettimeofday(&pdc_timer_end, 0);
    total_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    total_size = n_particles * 4.0 * 8 * size / 1024.0 / 1024.0;
    if (rank == 0) {
        printf("Write %d ts each of %.0fMB data with %d ranks: total %.2f\n"
               "create %.2f, query %.2f, write %.2f, wait %.2f, compute %.2f\n",
               n_ts, total_size, size, total_time, create_time_total, query_time_total, write_time_total,
               wait_time_total, compute_total);
        fflush(stdout);
    }

    // Free allocated space
    for (i = 0; i < n_var; i++) {
        if (mydata[i])
            free(mydata[i]);
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

exit:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
