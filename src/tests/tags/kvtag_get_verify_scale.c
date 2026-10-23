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

/*
 * Derive this rank's tag work from its object partition intersected with [0, n_tag).
 * Each rank verifies tags only on objects it owns that were tagged.
 */
static void
assign_tag_work_from_obj_partition(int my_obj_s, int my_obj, int n_tag, int *my_tag, int *my_tag_s)
{
    if (my_obj_s >= n_tag) {
        *my_tag   = 0;
        *my_tag_s = 0;
    }
    else {
        *my_tag_s = my_obj_s;
        *my_tag   = my_obj;
        if (*my_tag_s + *my_tag > n_tag)
            *my_tag = n_tag - *my_tag_s;
    }
}

void
print_usage(char *name)
{
    // required parameters: n_obj and n_query
    LOG_JUST_PRINT("%s n_obj n_tag\n", name);
}

int
main(int argc, char *argv[])
{
    pdcid_t        pdc, cont;
    pdcid_t *      obj_ids;
    int            n_obj, n_query, my_obj, my_obj_s, n_tag, n_tag_s;
    int            obj_1percent = 0;
    int            proc_num, my_rank, i;
    char           obj_name[128];
    double         stime, total_time, percent_time;
    pdc_kvtag_t    kvtag;
    void **        values;
    pdc_var_type_t value_type;
    size_t         value_size;
    int            ret_value = SUCCEED;
    // counters for verification statistics
    int verified_success     = 0;
    int verified_fail        = 0;
    int all_verified_success = 0;
    int all_verified_fail    = 0;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#else
    proc_num             = 1;
    my_rank              = 0;
#endif
    if (argc < 3) {
        if (my_rank == 0)
            print_usage(argv[0]);
        PGOTO_DONE(FAIL);
    }
    n_obj   = atoi(argv[1]);
    n_query = atoi(argv[2]);

    if (n_query > n_obj) {
        if (my_rank == 0)
            LOG_ERROR("n_query larger than n_obj! Exiting...\n");
        PGOTO_DONE(FAIL);
    }

    assign_work_to_rank(my_rank, proc_num, n_obj, &my_obj, &my_obj_s);
    assign_tag_work_from_obj_partition(my_obj_s, my_obj, n_query, &n_tag, &n_tag_s);

    obj_1percent = my_obj / 100;

    if (my_rank == 0)
        LOG_INFO("Open %d obj, query %d tags\n", my_obj, n_tag);

    // create a pdc
    pdc = PDCinit("pdc");

    // Open the existing container
    cont = PDCcont_open("c1", pdc);
    if (cont <= 0)
        PGOTO_ERROR(FAIL, "Failed to open container");

    // Open existing objects
    obj_ids = (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
#endif

    // open already created PDC objects and query tags
    for (i = 0; i < my_obj; i++) {
        sprintf(obj_name, "obj%d", my_obj_s + i);
        obj_ids[i] = PDCobj_open(obj_name, pdc);
        if (obj_ids[i] <= 0)
            PGOTO_ERROR(FAIL, "Failed to open object");

        // progress reporting for object opening
        if (i > 0 && obj_1percent > 0 && i % obj_1percent == 0) {
#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
            percent_time = MPI_Wtime() - stime;
            if (my_rank == 0) {
                int    current_percentage              = i / obj_1percent;
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
        LOG_INFO("Total time to open %11d objects: %7.2f , throughput %10.2f \n", n_obj, total_time,
                 n_obj / total_time);

    // Setup kvtag for queries
    kvtag.name = "Group";

    values = (void **)calloc(n_tag, sizeof(void *));

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
#endif
    for (i = 0; i < n_tag; i++) {
        if (PDCobj_get_tag(obj_ids[i], kvtag.name, (void *)&values[i], (void *)&value_type,
                           (void *)&value_size) < 0)
            PGOTO_ERROR(FAIL, "Failed to get a kvtag from obj%d\n", n_tag_s + i);

        int expected_value = n_tag_s + i;

        // count successful and failed verifications instead of immediate error
        if (*(int *)(values[i]) == expected_value) {
            verified_success++;
        }
        else {
            verified_fail++;
            // log first 10 failures for debugging
            if (verified_fail <= 10) {
                LOG_ERROR("Verification failed for obj%d: expected %d, got %d\n", n_tag_s + i, expected_value,
                          *(int *)(values[i]));
            }
        }
        free(values[i]);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
#endif
    if (my_rank == 0)
        LOG_INFO("Total time to retrieve %11d tag from %11d objects: %7.2f , throughput %10.2f \n", n_query,
                 n_obj, total_time, n_query / total_time);

    free(values);
    free(obj_ids);

    // aggregate verification statistics across all ranks
#ifdef ENABLE_MPI
    MPI_Reduce(&verified_success, &all_verified_success, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&verified_fail, &all_verified_fail, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_verified_success = verified_success;
    all_verified_fail    = verified_fail;
#endif

    if (my_rank == 0) {
        // completion message with verification statistics
        LOG_INFO("==============================================================\n");
        LOG_INFO("Verification Summary:\n");
        LOG_INFO("  Total queries:        %11d\n", n_query);
        LOG_INFO("  Successfully verified: %11d (%6.2f%%)\n", all_verified_success,
                 100.0 * all_verified_success / n_query);
        LOG_INFO("  Failed verification:   %11d (%6.2f%%)\n", all_verified_fail,
                 100.0 * all_verified_fail / n_query);
        LOG_INFO("==============================================================\n");

        if (all_verified_fail > 0) {
            LOG_ERROR("WARNING: %d objects failed verification!\n", all_verified_fail);
        }
        else {
            LOG_INFO("SUCCESS: All objects verified correctly!\n");
        }
    }

    // set return value based on verification results
    if (verified_fail > 0) {
        ret_value = FAIL;
    }

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return ret_value;
}