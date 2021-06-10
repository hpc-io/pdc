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
#include <sys/time.h>
#include <inttypes.h>
#include "pdc.h"

static char *
rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
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

int
main(int argc, char **argv)
{
    int            rank = 0, size = 1;
    pdcid_t        pdc_id, cont_prop, cont_id;
    pdcid_t        obj_prop1, obj_prop2, obj_prop3;
    pdcid_t        obj1, obj2, obj3;
    pdcid_t        r1, r2, r3;
    perr_t         ret;
    struct timeval ht_total_start;
    struct timeval ht_total_end;
    long long      ht_total_elapsed;
    double         ht_total_sec;
    int            use_name = -1;
    char           srank[10];
    char           obj_name1[512];
    char           obj_name2[512];
    char           obj_name3[512];
    char           tmp_str[128];
    int            myArray1[3][3] = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
    int            myArray2[3][3];
    int            myArray3[3][3];
    uint64_t       dims[2]          = {3, 3};
    uint64_t       offset[2]        = {1, 1};
    uint64_t       rdims[2]         = {2, 2};
    char *         env_str          = getenv("PDC_OBJ_NAME");
    char           name_mode[6][32] = {"Random Obj Names", "INVALID!", "One Obj Name",
                             "INVALID!",         "INVALID!", "Four Obj Names"};

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc_id = PDC_init("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("c1", cont_prop);
    if (cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    obj_prop3 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    if (env_str != NULL) {
        use_name = atoi(env_str);
    }
    printf("Using %s\n", name_mode[use_name + 1]);

    srand(rank + 1);
    sprintf(srank, "%d", rank);
    sprintf(obj_name1, "%s%s", rand_string(tmp_str, 16), srank);
    sprintf(obj_name2, "%s%s", rand_string(tmp_str, 16), srank);
    sprintf(obj_name3, "%s%s", rand_string(tmp_str, 16), srank);

    PDCprop_set_obj_dims(obj_prop1, 2, dims);
    PDCprop_set_obj_dims(obj_prop2, 2, dims);
    PDCprop_set_obj_dims(obj_prop3, 2, dims);

    PDCprop_set_obj_type(obj_prop1, PDC_INT);
    PDCprop_set_obj_type(obj_prop2, PDC_INT);
    PDCprop_set_obj_type(obj_prop3, PDC_INT);

    PDCprop_set_obj_buf(obj_prop1, &myArray1[0][0]);
    PDCprop_set_obj_time_step(obj_prop1, 0);
    PDCprop_set_obj_user_id(obj_prop1, getuid());
    PDCprop_set_obj_app_name(obj_prop1, "test_app");
    PDCprop_set_obj_tags(obj_prop1, "tag0=1");

    PDCprop_set_obj_buf(obj_prop2, &myArray2[0][0]);
    PDCprop_set_obj_time_step(obj_prop2, 0);
    PDCprop_set_obj_user_id(obj_prop2, getuid());
    PDCprop_set_obj_app_name(obj_prop2, "test_app");
    PDCprop_set_obj_tags(obj_prop2, "tag0=1");

    PDCprop_set_obj_buf(obj_prop3, &myArray3[0][0]);
    PDCprop_set_obj_time_step(obj_prop3, 0);
    PDCprop_set_obj_user_id(obj_prop3, getuid());
    PDCprop_set_obj_app_name(obj_prop3, "test_app");
    PDCprop_set_obj_tags(obj_prop3, "tag0=1");

    obj1 = PDCobj_create(cont_id, obj_name1, obj_prop1);
    if (obj1 == 0) {
        printf("Error getting an object id of %s from server, exit...\n", obj_name1);
        exit(-1);
    }

    obj2 = PDCobj_create(cont_id, obj_name2, obj_prop2);
    if (obj2 == 0) {
        printf("Error getting an object id of %s from server, exit...\n", obj_name2);
        exit(-1);
    }

    obj3 = PDCobj_create(cont_id, obj_name3, obj_prop3);
    if (obj3 == 0) {
        printf("Error getting an object id of %s from server, exit...\n", obj_name3);
        exit(-1);
    }

    // create a region
    r1 = PDCregion_create(2, offset, rdims);
    r2 = PDCregion_create(2, offset, rdims);
    r3 = PDCregion_create(2, offset, rdims);

    gettimeofday(&ht_total_start, 0);

    PDCobj_map(obj1, r1, obj2, r2);
    PDCobj_map(obj1, r1, obj3, r3);

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    printf("Total map overhead          : %.6f\n", ht_total_sec);
    fflush(stdout);

    gettimeofday(&ht_total_start, 0);

    ret = PDCreg_unmap(obj1, r1);
    if (ret != SUCCEED)
        printf("region unmap failed\n");

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;

    printf("Total unmap overhead        : %.6f\n", ht_total_sec);

    // close a container
    if (PDCcont_close(cont_id) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDC_close(pdc_id) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
