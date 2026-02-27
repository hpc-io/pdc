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
#include <getopt.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"

int
assign_work_to_rank(int rank, int size, int nwork, int *my_count, int *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        LOG_INFO("assign_work_to_rank(): Error with input\n");
        return -1;
    }
    if (nwork < size) {
        if (rank < nwork)
            *my_count = 1;
        else
            *my_count = 0;
        (*my_start) = rank * (*my_count);
    }
    else {
        (*my_count) = nwork / size;
        (*my_start) = rank * (*my_count);

        // Last few ranks may have extra work
        if (rank >= size - nwork % size) {
            (*my_count)++;
            (*my_start) += (rank - (size - nwork % size));
        }
    }

    return 1;
}

void
print_usage(char *name)
{
    /* Modified: Changed usage to only require n_obj and n_query */
    LOG_JUST_PRINT("%s n_obj n_query\n", name);
}

int
main(int argc, char *argv[])
{
    pdcid_t     pdc, cont_prop, cont;
    pdcid_t *   obj_ids;
    /* Modified: Removed n_add_tag variable, kept n_query */
    int         n_obj, n_query, my_obj, my_obj_s, my_query, my_query_s;
    /* Removed: obj_1percent and tag_1percent */
    int         query_1percent = 0;
    int         proc_num, my_rank, i;
    char        obj_name[128];
    double      stime, total_time, percent_time;
    pdc_kvtag_t kvtag;
    void **     values;
    pdc_var_type_t value_type;
    size_t         value_size;
    int            ret_value = SUCCEED;
    /* Added: Counters for verification statistics */
    int            verified_success = 0;
    int            verified_fail = 0;
    int            all_verified_success = 0;
    int            all_verified_fail = 0;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif
    /* Modified: Changed argc check from 4 to 3 (removed n_add_tag parameter) */
    if (argc < 3) {
        if (my_rank == 0)
            print_usage(argv[0]);
        PGOTO_DONE(FAIL);
    }
    n_obj   = atoi(argv[1]);
    /* Removed: n_add_tag assignment */
    n_query = atoi(argv[2]);

    if (n_query > n_obj) {
        if (my_rank == 0)
            LOG_ERROR("n_query larger than n_obj! Exiting...\n");
        PGOTO_DONE(FAIL);
    }

    /* Removed: assign_work_to_rank call for n_add_tag */
    assign_work_to_rank(my_rank, proc_num, n_query, &my_query, &my_query_s);
    assign_work_to_rank(my_rank, proc_num, n_obj, &my_obj, &my_obj_s);

    /* Removed: obj_1percent and tag_1percent calculations */
    query_1percent = my_query / 100;

    if (my_rank == 0)
        /* Modified: Log message shows only query count */
        LOG_INFO("Open %d obj, query %d tags\n", my_obj, my_query);

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        PGOTO_ERROR(FAIL, "Failed to create container property");

    /* Added: Open existing container instead of creating new one */
    // Open the existing container
    cont = PDCcont_open("c1", pdc);
    if (cont <= 0)
        PGOTO_ERROR(FAIL, "Failed to open container");

    /* Removed: object property creation - not needed for opening existing objects */

    /* Added: Open existing objects instead of creating new ones */
    // Open existing objects
    obj_ids = (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
#endif

    /* Modified: Changed from PDCobj_create to PDCobj_open */
    for (i = 0; i < my_obj; i++) {
        sprintf(obj_name, "obj%d", my_obj_s + i);
        obj_ids[i] = PDCobj_open(obj_name, pdc);
        if (obj_ids[i] <= 0)
            PGOTO_ERROR(FAIL, "Failed to open object");

        /* Added: Progress reporting for object opening (reusing original format) */
        if (i > 0 && query_1percent > 0 && i % query_1percent == 0) {
#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
            percent_time = MPI_Wtime() - stime;
            if (my_rank == 0) {
                int    current_percentage              = i / query_1percent;
                int    estimated_current_object_number = n_obj / 100 * current_percentage;
                double tps                             = estimated_current_object_number / percent_time;
                LOG_INFO("[OBJ PROGRESS %3d%% ] %11d objects, %7.2f seconds, TPS: %10.2f \n",
                         current_percentage, estimated_current_object_number, percent_time, tps);
            }
#endif
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
#endif

    if (my_rank == 0)
        /* Modified: Changed message from "create" to "open" */
        LOG_INFO("Total time to open %11d objects: %7.2f , throughput %10.2f \n", n_obj, total_time,
                 n_obj / total_time);

    /* Removed: All tag addition code (kvtag initialization and put_tag loop) */

    /* Added: Initialize kvtag for query operations */
    // Setup kvtag for queries
    kvtag.name = "Group";

    values = (void **)calloc(my_query, sizeof(void *));

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
#endif
    for (i = 0; i < my_query; i++) {
        if (PDCobj_get_tag(obj_ids[i], kvtag.name, (void *)&values[i], (void *)&value_type,
                           (void *)&value_size) < 0)
            PGOTO_ERROR(FAIL, "Failed to get a kvtag from o%d\n", i + my_query_s);

        if (i % query_1percent == 0) {
#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
            percent_time = MPI_Wtime() - stime;
            if (my_rank == 0) {
                int    current_percentage             = i / query_1percent;
                int    estimated_current_query_number = n_obj / 100 * current_percentage;
                double tps                            = estimated_current_query_number / percent_time;
                LOG_INFO("[QRY PROGRESS %3d%% ] %11d queries, %7.2f seconds, TPS: %10.2f \n",
                         current_percentage, estimated_current_query_number, percent_time, tps);
            }
#endif
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
#endif
    if (my_rank == 0)
        LOG_INFO("Total time to retrieve 1 tag from %11d objects: %7.2f , throughput %10.2f \n", n_query,
                 total_time, n_query / total_time);

    /* Modified: Changed expected value calculation and added verification counting */
    // The first program adds tags with value = i + my_add_tag_s
    // Since we're querying the first my_query objects, and assuming they were tagged
    // by ranks in order, we need to calculate the expected value
    for (i = 0; i < my_query; i++) {
        /* Modified: Calculate expected value based on how tags were originally assigned */
        // When tags were added, each rank tagged objects starting from my_add_tag_s
        // For verification, we need to determine which rank added the tag and its offset
        int expected_value = i + my_query_s;  // Assuming tags were added in order starting from 0

        /* Added: Count successful and failed verifications instead of immediate error */
        if (*(int *)(values[i]) == expected_value) {
            verified_success++;
        } else {
            verified_fail++;
            /* Optional: Log first few failures for debugging */
            if (verified_fail <= 10) {
                LOG_ERROR("Verification failed for obj%d: expected %d, got %d\n",
                          i + my_query_s, expected_value, *(int *)(values[i]));
            }
        }
        free(values[i]);
    }

    free(values);
    /* Added: Free obj_ids array */
    free(obj_ids);

    /* Added: Aggregate verification statistics across all ranks */
#ifdef ENABLE_MPI
    MPI_Reduce(&verified_success, &all_verified_success, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&verified_fail, &all_verified_fail, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_verified_success = verified_success;
    all_verified_fail = verified_fail;
#endif

    if (my_rank == 0) {
        /* Modified: Enhanced completion message with verification statistics */
        LOG_INFO("==============================================================\n");
        LOG_INFO("Verification Summary:\n");
        LOG_INFO("  Total queries:        %11d\n", n_query);
        LOG_INFO("  Successfully verified: %11d (%6.2f%%)\n",
                 all_verified_success,
                 100.0 * all_verified_success / n_query);
        LOG_INFO("  Failed verification:   %11d (%6.2f%%)\n",
                 all_verified_fail,
                 100.0 * all_verified_fail / n_query);
        LOG_INFO("==============================================================\n");

        if (all_verified_fail > 0) {
            LOG_ERROR("WARNING: %d objects failed verification!\n", all_verified_fail);
        } else {
            LOG_INFO("SUCCESS: All objects verified correctly!\n");
        }
    }

    /* Added: Set return value based on verification results */
    if (verified_fail > 0) {
        ret_value = FAIL;
    }

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return ret_value;
}