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
#include "pdc.h"
#include "test_helper.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop;
    pdcid_t obj1, obj2, open11, open12, open21;
    int     rank = 0, size = 1;
    char    cont_name[128], obj_name1[128], obj_name2[128];
    int     ret_value = TSUCCEED;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");

    // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
    sprintf(cont_name, "c%d", rank);
    TASSERT((cont = PDCcont_create(cont_name, cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");
    // create an object property
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create first object
    sprintf(obj_name1, "o1");
#ifdef ENABLE_MPI
    TASSERT((obj1 = PDCobj_create_coll(cont, obj_name1, obj_prop, 0, MPI_COMM_WORLD)) != 0,
            "Call to PDCobj_create succeeded", "Call to PDCobj_create failed");
#else
    TASSERT((obj1 = PDCobj_create(cont, obj_name1, obj_prop)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
#endif
    LOG_INFO("Checkpoint 1 rank %d\n", rank);
    // create second object
    sprintf(obj_name2, "o2");
#ifdef ENABLE_MPI
    TASSERT((obj2 = PDCobj_create_coll(cont, obj_name2, obj_prop, 0, MPI_COMM_WORLD)) != 0,
            "Call to PDCobj_create succeeded", "Call to PDCobj_create failed");
#else
    TASSERT((obj2 = PDCobj_create(cont, obj_name2, obj_prop)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
#endif
    LOG_INFO("Checkpoint 2 rank %d\n", rank);
    // open first object twice
    TASSERT((open11 = PDCobj_open(obj_name1, pdc)) != 0, "Call to PDCobj_open succeeded",
            "Call to PDCobj_open failed");
    TASSERT((open12 = PDCobj_open(obj_name1, pdc)) != 0, "Call to PDCobj_open succeeded",
            "Call to PDCobj_open failed");
    // open second object once
    TASSERT((open21 = PDCobj_open(obj_name2, pdc)) != 0, "Call to PDCobj_open succeeded",
            "Call to PDCobj_open failed");
    // open second object twice
    TASSERT((open21 = PDCobj_open(obj_name2, pdc)) != 0, "Call to PDCobj_open succeeded",
            "Call to PDCobj_open failed");
    // close object
    TASSERT(PDCobj_close(obj1) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(open11) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(open12) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(obj2) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(open21) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    // close a container
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    // close a object property
    TASSERT(PDCprop_close(obj_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close a container property
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
