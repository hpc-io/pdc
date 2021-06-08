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
#include <sys/time.h>
#include <ctype.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

static char *
rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK...";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            int key = rand() % (int)(sizeof(charset) - 1);
            str[n]  = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

void
print_usage()
{
    printf("Usage: srun -n ./creat_obj -r num_of_obj_per_rank\n");
}

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1;
    int     count = -1;
    int     i;
    pdcid_t pdc, cont_prop, cont, obj_prop;

    struct timeval ht_total_start;
    struct timeval ht_total_end;
    long long      ht_total_elapsed;
    double         ht_total_sec;

    char           name_mode[6][32] = {"Random Obj Names", "INVALID!", "One Obj Name",
                             "INVALID!",         "INVALID!", "Four Obj Names"};
    char           filename[128], pdc_server_tmp_dir_g[128];
    char *         env_str;
    char **        obj_names;
    int *          obj_ts;
    char *         tmp_dir;
    int            n_entry;
    int            use_name = -1;
    pdc_metadata_t entry;
    uint32_t *     hash_key;
    int            j, read_count = 0, tmp_count;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    while ((i = getopt(argc, argv, "r:")) != EOF)
        switch (i) {
            case 'r':
                count = atoi(optarg);
                break;
            case '?':
                if (optopt == 'r')
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                else if (isprint(optopt))
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                else
                    fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
                return 1;
            default:
                print_usage();
                exit(-1);
        }

    if (count == -1) {
        print_usage();
        exit(-1);
    }

    if (rank == 0) {
        printf("%d clients in total\n", size);
    }
    count /= size;

    // create a pdc
    pdc = PDC_init("pdc");
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

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

    env_str = getenv("PDC_OBJ_NAME");
    if (env_str != NULL) {
        use_name = atoi(env_str);
    }

    srand(rank + 1);

    if (rank == 0) {
        printf("Using %s\n", name_mode[use_name + 1]);
    }

    obj_names = (char **)malloc(count * sizeof(char *));
    obj_ts    = (int *)malloc(count * sizeof(int));
    for (i = 0; i < count; i++) {
        obj_names[i] = (char *)malloc(128 * sizeof(char));
    }

    // Set up tmp dir
    tmp_dir = getenv("PDC_TMPDIR");
    if (tmp_dir == NULL)
        strcpy(pdc_server_tmp_dir_g, "./pdc_tmp");
    else
        strcpy(pdc_server_tmp_dir_g, tmp_dir);

    sprintf(filename, "%s/metadata_checkpoint.%d", pdc_server_tmp_dir_g, rank);
    /* printf("file name: %s\n", filename); */
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        fputs("Checkpoint file not available\n", stderr);
        return -1;
    }

    fread(&n_entry, sizeof(int), 1, file);
    /* printf("%d entries\n", n_entry); */

    while (n_entry > 0) {
        fread(&tmp_count, sizeof(int), 1, file);
        /* printf("Count:%d\n", tmp_count); */

        hash_key = (uint32_t *)malloc(sizeof(uint32_t));
        fread(hash_key, sizeof(uint32_t), 1, file);
        /* printf("Hash key is %u\n", *hash_key); */

        // read each metadata
        for (j = 0; j < tmp_count; j++) {
            if (read_count >= count) {
                n_entry = 0;
                break;
            }
            fread(&entry, sizeof(pdc_metadata_t), 1, file);
            sprintf(obj_names[read_count], "%s", entry.obj_name);
            obj_ts[i] = entry.time_step;
            /* printf("Read name %s\n", obj_names[read_count]); */
            read_count++;
        }
        n_entry--;
    }

    fclose(file);
    /* printf("Finished read\n"); */
    /* fflush(stdout); */

    /* count = read_count; */

    if (rank == 0)
        printf("Searching %d objects per MPI rank\n", count);
    fflush(stdout);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_start, 0);

    for (i = 0; i < count; i++) {

        pdc_metadata_t *res = NULL;

        /* printf("Proc %d search %s\n", rank, obj_name); */
        /* PDC_Client_query_metadata_with_name(obj_names[i], &res); */
        /* PDC_Client_query_metadata_with_name(obj_names[0], &res); */
        int name_id = 0;
        PDC_Client_query_metadata_name_only(obj_names[name_id], &res);
        if (res == NULL) {
            printf("No result found for current query with name [%s]\n", obj_names[i]);
        }
        /* else { */
        /*     printf("Got response from server.\n"); */
        /*     PDC_print_metadata(res); */
        /*     fflush(stdout); */
        /* } */

        // Print progress
        int progress_factor = count < 20 ? 1 : 20;
        if (i > 0 && i % (count / progress_factor) == 0) {
            gettimeofday(&ht_total_end, 0);
            ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL +
                               ht_total_end.tv_usec - ht_total_start.tv_usec;
            ht_total_sec = ht_total_elapsed / 1000000.0;

            if (rank == 0) {
                printf("searched %10d ... %.2f\n", i * size, ht_total_sec);
                fflush(stdout);
            }

#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
#endif
        }
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        /* printf("Time to full query %d obj/rank with %d ranks: %.6f\n\n\n", count, size, ht_total_sec); */
        printf("Time to partial query %d obj/rank with %d ranks: %.6f\n\n\n", count, size, ht_total_sec);
        fflush(stdout);
    }

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
