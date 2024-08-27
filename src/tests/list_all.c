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
#include <ctype.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

static char *
rand_string(char *str, size_t size)
{
    size_t     n;
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    if (size) {
        --size;
        for (n = 0; n < size; n++) {
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
    int              rank = 0, size = 1;
    int              i;
    pdcid_t          pdc, cont_prop, cont, obj_prop;
    int              count = -1;
    struct timeval   ht_total_start;
    struct timeval   ht_total_end;
    long long        ht_total_elapsed;
    double           ht_total_sec;
    char             obj_name[512];
    char             obj_prefix[4][10] = {"x", "y", "z", "energy"};
    char             tmp_str[128];
    int              use_name = -1;
    uint64_t         dims[3]  = {100, 200, 700};
    pdcid_t          test_obj = -1;
    char            *env_str;
    char             name_mode[6][32] = {"Random Obj Names", "INVALID!", "One Obj Name",
                                         "INVALID!",         "INVALID!", "Four Obj Names"};
    pdc_metadata_t **out;
    int              n_obj;

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

    count /= size;

    if (rank == 0)
        printf("Creating %d objects per MPI rank\n", count);
    fflush(stdout);

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

    PDCprop_set_obj_dims(obj_prop, 3, dims);

    env_str = getenv("PDC_OBJ_NAME");
    if (env_str != NULL) {
        use_name = atoi(env_str);
    }

    if (rank == 0) {
        printf("Using %s\n", name_mode[use_name + 1]);
    }

    srand(rank + 1);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_start, 0);

    for (i = 0; i < count; i++) {
        if (use_name == -1) {
            sprintf(obj_name, "%s", rand_string(tmp_str, 16));
            PDCprop_set_obj_time_step(obj_prop, i);
        }
        else if (use_name == 1) {
            sprintf(obj_name, "%s", obj_prefix[0]);
            PDCprop_set_obj_time_step(obj_prop, i + rank * count);
        }
        else if (use_name == 4) {
            sprintf(obj_name, "%s", obj_prefix[i % 4]);
            PDCprop_set_obj_time_step(obj_prop, i / 4 + rank * count);
        }
        else {
            printf("Unsupported name choice\n");
            goto done;
        }
        PDCprop_set_obj_user_id(obj_prop, getuid());
        PDCprop_set_obj_app_name(obj_prop, "test_app");
        PDCprop_set_obj_tags(obj_prop, "tag0=1");

        test_obj = PDCobj_create(cont, obj_name, obj_prop);
        if (test_obj == 0) {
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            exit(-1);
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
        printf("Time to create %d obj/rank with %d ranks: %.5e\n", count, size, ht_total_sec);
        fflush(stdout);
    }

    printf("Listing all objects\n");
    PDC_Client_list_all(&n_obj, &out);
    printf("Received %d metadata objects\n", n_obj);

    for (i = 0; i < n_obj; i++) {
        PDC_print_metadata(out[i]);
    }

    PDCprop_set_obj_tags(obj_prop, "tag1=2");
    PDCprop_set_obj_time_step(obj_prop, 0);
    test_obj = PDCobj_create(cont, "test_obj_name0", obj_prop);
    test_obj = PDCobj_create(cont, "test_obj_name1", obj_prop);
    printf("Searching for objects with tag1=2\n");
    PDC_partial_query(0, -1, NULL, NULL, -1, -1, -1, "tag1=2", &n_obj, &out);
    printf("Received %d metadata objects\n", n_obj);
    for (i = 0; i < n_obj; i++) {
        PDC_print_metadata(out[i]);
    }

    printf("Searching for objects with timestep from 2 to 4\n");
    PDC_partial_query(0, -1, NULL, NULL, 2, 4, -1, NULL, &n_obj, &out);
    printf("Received %d metadata objects\n", n_obj);
    for (i = 0; i < n_obj; i++) {
        PDC_print_metadata(out[i]);
    }

done:
    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
