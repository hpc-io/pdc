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

#define NCONT 10

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

void
print_usage()
{
    printf("Usage: srun -n ./creat_obj -r num_of_obj_per_rank\n");
}

int
main(int argc, char **argv)
{
    int      rank = 0, size = 1;
    int      count = -1;
    int      i;
    pdcid_t  pdc, cont_prop, cont[NCONT], obj_prop;
    char     cont_name[128];
    uint64_t dims[3] = {100, 200, 700};
    pdcid_t *obj_ids = NULL;

    char *env_str;
    char  obj_name[512];
    char  obj_prefix[4][10] = {"x", "y", "z", "energy"};
    char  tmp_str[128];
    char  name_mode[6][32] = {"Random Obj Names", "INVALID!", "One Obj Name",
                             "INVALID!",         "INVALID!", "Four Obj Names"};

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    count = 10;

    obj_ids = (pdcid_t *)calloc(count, sizeof(pdcid_t));

    if (rank == 0)
        printf("Creating %d objects per MPI rank\n", count);
    fflush(stdout);

    // create a pdc
    pdc = PDC_init("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    i       = 0 sprintf(cont_name, "Cont%d", i);
    cont[i] = PDCcont_create(cont_name, cont_prop);
    if (cont[i] <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    PDCprop_set_obj_type(obj_prop, PDC_INT);
    PDCprop_set_obj_dims(obj_prop, 3, dims);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_app_name(obj_prop, "test_app");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // Create a few objects with same name but diff timestep
    sprintf(obj_name, "%s", obj_prefix[0]);
    for (i = 0; i < count; i++) {
        PDCprop_set_obj_time_step(obj_prop, i);

        obj_ids[i] = PDCobj_create(cont[0], obj_name, obj_prop);
        if (obj_ids[i] < 0) {
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            goto done;
        }
        printf("[%d] create an obj with name: %s, ts: %d\n", rank, obj_name, i);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    /* // Add object to container */
    /* PDC_Client_add_objects_to_container(count, obj_ids, cont[0]); */

    /* // Delete object to container */
    /* PDC_Client_del_objects_to_container(count, obj_ids, cont[0]); */

done:
    // close all container
    if (PDCcont_close(cont[0]) < 0)
        printf("fail to close container c1\n");

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
