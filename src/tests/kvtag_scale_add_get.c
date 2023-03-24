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
#include <inttypes.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"

uint64_t
atoui64(char *arg)
{
    char    *endptr;
    uint64_t num = strtoull(arg, &endptr, 10);

    if (*endptr != '\0') {
        printf("Invalid input: %s\n", arg);
        return 1;
    }
    return num;
}

int
assign_work_to_rank(int rank, int size, uint64_t nwork, uint64_t *my_count, uint64_t *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        printf("assign_work_to_rank(): Error with input!\n");
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
    printf("%s n_obj n_obj_incr\n", name);
}

int
main(int argc, char *argv[])
{
    pdcid_t              pdc, cont_prop, cont, obj_prop;
    pdcid_t             *obj_ids;
    uint64_t             n_obj, n_obj_incr, my_obj, my_obj_s, curr_total_obj;
    uint64_t             i, v;
    int                  proc_num, my_rank;
    char                 obj_name[128];
    char                 tag_name[128];
    double               stime, total_time;
    void               **values;
    size_t               value_size;
    obj_handle          *oh;
    struct pdc_obj_info *info;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif
    if (argc < 3) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj      = atoui64(argv[1]);
    n_obj_incr = atoui64(argv[2]);

    if (n_obj_incr > n_obj) {
        if (my_rank == 0)
            printf("n_obj_incr cannot be larger than n_obj! Exiting...\n");
        goto done;
    }

    if (my_rank == 0)
        printf("Create %llu obj, %llu tags, query %llu\n", n_obj, n_obj, n_obj);

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    curr_total_obj = 0;

    if (my_rank == 0)
        printf("create obj_ids array\n");
    // Create a number of objects, add at least one tag to that object
    assign_work_to_rank(my_rank, proc_num, n_obj_incr, &my_obj, &my_obj_s);

    obj_ids = (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));
    values = (void **)calloc(my_obj, sizeof(void *));
    sprintf(tag_name, "tag%d", 2);
    do {

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        stime = MPI_Wtime();
#endif

        if (my_rank == 0)
            printf("starting creating %llu objects... \n", my_obj);

        for (i = 0; i < my_obj; i++) {
            v = my_obj_s + i + curr_total_obj;
            sprintf(obj_name, "obj%llu", v);
            obj_ids[i] = PDCobj_create(cont, obj_name, obj_prop);
            if (obj_ids[i] <= 0)
                printf("Fail to create object @ line  %d!\n", __LINE__);
        }

        curr_total_obj += n_obj_incr;

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        total_time = MPI_Wtime() - stime;
#endif
        if (my_rank == 0)
            printf("Created %llu objects in total now in %.4f seconds.\n", curr_total_obj, total_time);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        stime = MPI_Wtime();
#endif

        for (i = 0; i < my_obj; i++) {
            v = i + my_obj_s + curr_total_obj - n_obj_incr;
            if (PDCobj_put_tag(obj_ids[i], tag_name, (void *)&v, sizeof(uint64_t)) < 0)
                printf("fail to add a kvtag to o%llu\n", i + my_obj_s);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        total_time = MPI_Wtime() - stime;
#endif
        if (my_rank == 0)
            printf("Total time to add tags to %llu objects: %.4f\n", my_obj, total_time);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        stime = MPI_Wtime();
#endif
        for (i = 0; i < my_obj; i++) {
            v = i + my_obj_s + curr_total_obj - n_obj_incr;
            if (PDCobj_get_tag(obj_ids[i], tag_name, (void **)&values[i], (void *)&value_size) < 0)
                printf("fail to get a kvtag from o%llu\n", v);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        total_time = MPI_Wtime() - stime;
#endif
        if (my_rank == 0)
            printf("Total time to retrieve tags from %llu objects: %.4f\n", my_obj, total_time);

        fflush(stdout);

        for (i = 0; i < my_obj; i++) {
            v = i + my_obj_s + curr_total_obj - n_obj_incr;
            if (*(int *)(values[i]) != v)
                printf("Error with retrieved tag from o%llu\n", v);
            // free(values[i]);
        }
        // free(values);

        // close  objects
        for (i = 0; i < my_obj; i++) {
            v = i + my_obj_s + curr_total_obj - n_obj_incr;
            if (PDCobj_close(obj_ids[i]) < 0)
                printf("fail to close object o%llu\n", v);
        }

    } while (curr_total_obj < n_obj);

    for (i = 0; i < my_obj; i++) {
        free(values[i]);
    }
    free(obj_ids);
    free(values);

    // oh = PDCobj_iter_start(cont);

    // while (!PDCobj_iter_null(oh)) {
    //     info = PDCobj_iter_get_info(oh);
    //     info->
    //     if (info->obj_pt->type != PDC_DOUBLE) {
    //         printf("Type is not properly inherited from object property.\n");
    //         ret_value = 1;
    //     }
    //     if (info->obj_pt->ndim != ndim) {
    //         printf("Number of dimensions is not properly inherited from object property.\n");
    //         ret_value = 1;
    //     }
    //     if (info->obj_pt->dims[0] != dims[0]) {
    //         printf("First dimension is not properly inherited from object property.\n");
    //         ret_value = 1;
    //     }
    //     if (info->obj_pt->dims[1] != dims[1]) {
    //         printf("Second dimension is not properly inherited from object property.\n");
    //         ret_value = 1;
    //     }
    //     if (info->obj_pt->dims[2] != dims[2]) {
    //         printf("Third dimension is not properly inherited from object property.\n");
    //         ret_value = 1;
    //     }

    //     oh = PDCobj_iter_next(oh, cont);
    // }

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(obj_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    // close pdc
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
