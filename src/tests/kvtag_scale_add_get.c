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

/**
 * Assigns a portion of work to a specific rank in a parallel processing environment.
 *
 * This function calculates the start and count of work items that should be
 * assigned to a specific rank based on the total number of work items (nwork) and
 * the total number of ranks (size) in the environment.
 *
 * @param rank The rank of the process for which the work is being assigned (0-indexed).
 * @param size The total number of ranks in the parallel processing environment.
 * @param nwork The total number of work items to be distributed among the ranks.
 * @param my_count A pointer to a uint64_t variable that will store the number of work items assigned to the
 * rank.
 * @param my_start A pointer to a uint64_t variable that will store the starting index of the work items
 * assigned to the rank.
 * @return 0 if the function executes successfully, non-zero if an error occurs.
 */
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
    return 0;
}

char **
gen_strings(int n_strings, int string_len)
{
    srand(time(NULL)); // seed the random number generator with the current time

    char **str = malloc(n_strings * sizeof(char *)); // allocate memory for the array of strings

    for (int i = 0; i < n_strings; i++) {
        str[i] = malloc((string_len + 1) * sizeof(char)); // allocate memory for each string in the array
        for (int j = 0; j < string_len; j++) {
            str[i][j] = 'a' + rand() % 26; // generate a random lowercase letter
        }
        str[i][string_len] = '\0'; // terminate the string with a null character
    }
    return str; // return the array of strings
}

/**
 * @brief Prints the usage instructions for the given program.
 * This function displays the correct usage and command-line options for the program
 * identified by the name parameter. It is typically called when the user provides
 * incorrect or insufficient arguments.
 * @param name A pointer to a character string representing the name of the program.
 */
void
print_usage(char *name)
{
    printf("Usage: %s <n_obj> <n_obj_incr> <n_attr> <n_attr_len> <n_query>\n\n", name);

    printf("  n_obj      : The total number of objects (positive integer).\n");
    printf("  n_obj_incr : The increment in the number of objects per step (positive integer).\n");
    printf("  n_attr     : The number of attributes per object (positive integer).\n");
    printf("  n_attr_len : The length of each attribute (positive integer).\n");
    printf("  n_query    : The number of queries to be performed (positive integer).\n\n");

    printf("Example:\n");
    printf("  %s 100 10 5 20 50\n\n", name);
}

/**
 * Initializes a test environment for the PDC by creating a specified number of objects in a container
 * and returning their object IDs.
 *
 * @param my_rank      The rank of the current process in the MPI communicator.
 * @param proc_num     The total number of processes in the MPI communicator.
 * @param n_obj_incr   Pointer to the number of objects to be created in each iteration.
 * @param my_obj       Pointer to the number of objects assigned to the current process.
 * @param my_obj_s     Pointer to the starting object index assigned to the current process.
 * @param obj_prop     Pointer to the object property ID to be used for creating objects.
 *
 * @return obj_ids     Pointer to an array of object IDs for the created objects.
 */
pdcid_t *
init_test(int my_rank, int proc_num, uint64_t *n_obj_incr, uint64_t *my_obj, uint64_t *my_obj_s,
          pdcid_t *obj_prop)
{
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
    *obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    curr_total_obj = 0;

    if (my_rank == 0)
        printf("create obj_ids array\n");
    // Create a number of objects, add at least one tag to that object
    assign_work_to_rank(my_rank, proc_num, n_obj_incr, &my_obj, &my_obj_s);

    return (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));
}

/**
 * Creates a specified number of objects in a container and stores their object IDs in the obj_ids array.
 *
 * @param my_obj         The number of objects assigned to the current process.
 * @param my_obj_s       The starting object index assigned to the current process.
 * @param curr_total_obj The current total number of objects.
 * @param cont           The container ID in which to create the objects.
 * @param obj_prop       The object property ID to be used for creating objects.
 * @param obj_ids        Pointer to an array of object IDs for the created objects.
 */
void
create_object(uint64_t my_obj, uint64_t my_obj_s, uint64_t curr_total_obj, pdcid_t cont, pdcid_t obj_prop,
              pdcid_t *obj_ids)
{
    uint64_t i, v;
    char     obj_name[128];

    for (i = 0; i < my_obj; i++) {
        v = my_obj_s + i + curr_total_obj;
        sprintf(obj_name, "obj%llu", v);
        obj_ids[i] = PDCobj_create(cont, obj_name, obj_prop);
        if (obj_ids[i] <= 0)
            printf("Fail to create object @ line  %d!\n", __LINE__);
    }
}

/**
 * Adds n_attr tags to each object in the obj_ids array.
 *
 * @param my_obj         The number of objects assigned to the current process.
 * @param my_obj_s       The starting object index assigned to the current process.
 * @param curr_total_obj The current total number of objects.
 * @param n_obj_incr     The number of objects to be created in each iteration.
 * @param n_attr         The number of attributes (tags) to add to each object.
 * @param obj_ids        Pointer to an array of object IDs for the objects to add tags.
 */
void
add_n_tags(uint64_t my_obj, uint64_t my_obj_s, uint64_t curr_total_obj, uint64_t n_obj_incr, uint64_t n_attr,
           char **tag_values, uint64_t tag_value_len, pdcid_t *obj_ids)
{
    uint64_t i, j, v;
    char     tag_name[128];

    for (i = 0; i < my_obj; i++) {
        v = i + my_obj_s + curr_total_obj - n_obj_incr;
        for (j = 0; j < n_attr; j++) {
            sprintf(tag_name, "tag%llu.%llu", v, j);
            if (PDCobj_put_tag(obj_ids[i], tag_name, (void *)&tag_values[j], tag_value_len * sizeof(char)) <
                0)
                printf("fail to add a kvtag to o%llu\n", i + my_obj_s);
        }
    }
}

/**
 * Queries n_attr tags from the object specified by the object ID.
 *
 * @param obj_id      The ID of the object to retrieve tags for.
 * @param obj_name_v  logical object ID for object name.
 * @param n_attr      The number of tags to retrieve.
 * @param tag_values  An array of pointers to store the tag values.
 * @param value_size  An array to store the size of each tag value.
 */
void
get_object_tags(pdcid_t obj_id, uint64_t obj_name_v, int n_attr, char **tag_values, uint64_t *value_size)
{
    uint64_t i, v;
    char     tag_name[128];

    for (i = 0; i < n_attr; i++) {
        sprintf(tag_name, "tag%llu.%llu", obj_name_v, i);
        if (PDCobj_get_tag(obj_id, tag_name, (void **)&tag_values[i], (void *)&value_size[i]) < 0)
            printf("fail to get a kvtag from o%llu\n", v);
    }
}

/**
 * Sends a specified number of queries to retrieve the values of the specified tags for a set of objects.
 *
 * @param my_obj_s         The size of the current object name.
 * @param curr_total_obj   The current total number of objects.
 * @param n_obj_incr       The number of objects to increment by.
 * @param n_query          The number of queries to send.
 * @param n_attr           The number of tags to retrieve for each object.
 * @param obj_ids          An array of object IDs to retrieve tags for.
 * @param tag_values       An array of pointers to store the tag values for all queries and objects.
 *                         The caller is responsible for allocating memory for the array and the individual
 * pointers within it.
 * @param value_size       An array to store the size of each tag value.
 *                         The caller is responsible for allocating memory for the array.
 */
void
send_queries(uint64_t my_obj_s, uint64_t curr_total_obj, uint64_t n_obj_incr, int n_query, uint64_t n_attr,
             pdcid_t *obj_ids, char **tag_values, uint64_t *value_size)
{
    uint64_t i, v;
    char     tag_name[128];

    for (i = 0; i < n_query; i++) {
        v = i + my_obj_s + curr_total_obj - n_obj_incr;
        get_object_tags(obj_ids[i], v, n_attr, &tag_values[i * n_attr], &value_size[i * n_attr]);
    }
}

int
main(int argc, char *argv[])
{
    pdcid_t              pdc, cont_prop, cont, obj_prop;
    pdcid_t             *obj_ids;
    uint64_t             n_obj, n_obj_incr, my_obj, my_obj_s, curr_total_obj;
    uint64_t             n_attr, n_attr_len, n_query;
    uint64_t             i, j, k, v;
    int                  proc_num, my_rank, attr_value;
    char                 obj_name[128];
    char                 tag_name[128];
    double               stime, total_time;
    int                 *value_to_add;
    char               **query_rst_cache;
    uint64_t            *value_size;
    obj_handle          *oh;
    struct pdc_obj_info *info;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif
    if (argc < 6) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj      = atoui64(argv[1]);
    n_obj_incr = atoui64(argv[2]);
    n_attr     = atoui64(argv[3]);
    n_attr_len = atoui64(argv[4]);
    n_query    = atoui64(argv[5]);

    if (n_obj_incr > n_obj) {
        if (my_rank == 0)
            printf("n_obj_incr cannot be larger than n_obj! Exiting...\n");
        goto done;
    }

    if (my_rank == 0)
        printf("Create %llu obj, %llu tags, query %llu\n", n_obj, n_obj, n_obj);

    // making necessary preparation for the test.
    obj_ids = init_test(my_rank, proc_num, &n_obj_incr, &my_obj, &my_obj_s, &obj_prop);

    do {

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        stime = MPI_Wtime();
#endif
        if (my_rank == 0)
            printf("starting creating %llu objects... \n", my_obj);

        // creating objects. Here, my_obj and my_obj_s has been calculated for each process based on
        // n_obj_incr.
        create_object(my_obj, my_obj_s, curr_total_obj, cont, obj_prop, obj_ids);
        // therefore, after 'create_objects' function, we should add 'curr_total_obj' by 'n_obj_incr'.
        curr_total_obj += n_obj_incr;

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        total_time = MPI_Wtime() - stime;
#endif
        if (my_rank == 0)
            printf("Created %llu objects in total now in %.4f seconds.\n", curr_total_obj, total_time);

        char **tag_values = gen_strings(n_attr, n_attr_len);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        stime = MPI_Wtime();
#endif

        add_n_tags(my_obj, my_obj_s, curr_total_obj, n_obj_incr, n_attr, tag_values, obj_ids);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        total_time = MPI_Wtime() - stime;
#endif
        if (my_rank == 0)
            printf("Total time to add tags to %llu objects: %.4f\n", my_obj, total_time);

        query_rst_cache = (char **)malloc(n_query * n_attr * sizeof(char *));
        value_size = malloc(n_query * n_attr * sizeof(uint64_t));
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        stime = MPI_Wtime();
#endif

        send_queries(my_obj_s, curr_total_obj, n_obj_incr, n_query, n_attr, obj_ids, query_rst_cache,
                     value_size);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        total_time = MPI_Wtime() - stime;
#endif
        if (my_rank == 0)
            printf("Total time to retrieve tags from %llu objects: %.4f\n", my_obj, total_time);

        fflush(stdout);

        // for (i = 0; i < my_obj * n_attr; i++) {
        //     v = i + my_obj_s + curr_total_obj - n_obj_incr;
        //     if (*(int *)(values[i]) != v)
        //         printf("Error with retrieved tag from o%llu\n", v);
        //     free(values[i]);
        // }
        // free(values);

        // close  objects
        for (i = 0; i < my_obj; i++) {
            v = i + my_obj_s + curr_total_obj - n_obj_incr;
            if (PDCobj_close(obj_ids[i]) < 0)
                printf("fail to close object o%llu\n", v);
        }

    } while (curr_total_obj < n_obj);

    // for (i = 0; i < my_obj; i++) {
    //     free(values[i]);
    // }
    // free(values);
    free(obj_ids);

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
