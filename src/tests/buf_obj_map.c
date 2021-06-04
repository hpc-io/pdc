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
#include "pdc.h"

int
main(int argc, char **argv)
{
    int      rank = 0, size = 1;
    perr_t   ret;
    pdcid_t  pdc_id, cont_prop, cont_id, obj_prop2;
    pdcid_t  obj2       = 0, r1, r2;
    uint64_t dims[2]    = {4, 4};
    uint64_t offset[2]  = {1, 2};
    uint64_t offset1[2] = {0, 0};
    uint64_t rdims[2]   = {3, 2};
    char     obj_name2[512];
    int      myArray1[3][2] = {{107, 108}, {111, 112}, {115, 116}};

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("c1", cont_prop);
    if (cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop2, 2, dims);
    PDCprop_set_obj_type(obj_prop2, PDC_INT);
    PDCprop_set_obj_time_step(obj_prop2, 0);
    PDCprop_set_obj_user_id(obj_prop2, getuid());
    PDCprop_set_obj_app_name(obj_prop2, "test_app");
    PDCprop_set_obj_tags(obj_prop2, "tag0=1");

    sprintf(obj_name2, "test_obj2");
    obj2 = PDCobj_create(cont_id, obj_name2, obj_prop2);

    // create a region
    r1 = PDCregion_create(2, offset1, rdims);
    r2 = PDCregion_create(2, offset, rdims);

    ret = PDCbuf_obj_map(&myArray1[0], PDC_INT, r1, obj2, r2);
    if (ret < 0)
        printf("PDCbuf_obj_map failed\n");

    ret = PDCreg_obtain_lock(obj2, r2, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region\n");

    // update buffer
    myArray1[0][0] = 117;
    myArray1[0][1] = 118;

    ret = PDCreg_release_lock(obj2, r2, PDC_WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region\n");

    PDCbuf_obj_unmap(obj2, r2);

    // close region
    if (PDCregion_close(r1) < 0)
        printf("fail to close region r1\n");

    if (PDCregion_close(r2) < 0)
        printf("fail to close region r2\n");

    // close object
    if (PDCobj_close(obj2) < 0)
        printf("fail to close obj2\n");

    // close a container
    if (PDCcont_close(cont_id) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc_id) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
