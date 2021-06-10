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

int
main(int argc, char **argv)
{
    pdcid_t              pdc, cont_prop, cont, obj_prop;
    pdcid_t              obj1, obj2;
    int                  rank = 0, size = 1, i, ret;
    int                  ret_value = 0;
    char                 cont_name[128], obj_name1[128], obj_name2[128];
    struct pdc_obj_info *obj1_info, *obj2_info;

    size_t   ndim = 3;
    uint64_t dims[3];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    dims[0] = rank * 2 + 1;
    dims[1] = rank * 3 + 2;
    dims[2] = rank * 5 + 3;

    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop > 0) {
        printf("Rank %d Create a container property\n", rank);
    }
    else {
        printf("Rank %d Fail to create container property @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    // create a container
    sprintf(cont_name, "c");
    cont = PDCcont_create_col(cont_name, cont_prop);
    // cont = PDCcont_create(cont_name, cont_prop);
    if (cont > 0) {
        printf("Rank %d Create a container %s\n", rank, cont_name);
    }
    else {
        printf("Rank %d Fail to create container @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop > 0) {
        printf("Rank %d Create an object property\n", rank);
    }
    else {
        printf("Rank %d Fail to create object property @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    ret = PDCprop_set_obj_dims(obj_prop, ndim, dims);
    if (ret != SUCCEED) {
        printf("Fail to set obj time step @ line %d\n", __LINE__);
        ret_value = 1;
    }
    ret = PDCprop_set_obj_type(obj_prop, PDC_DOUBLE);
    if (ret != SUCCEED) {
        printf("Fail to set obj time step @ line %d\n", __LINE__);
        ret_value = 1;
    }

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    obj1 = PDCobj_create(cont, obj_name1, obj_prop);
    if (obj1 > 0) {
        printf("Rank %d Create an object %s\n", rank, obj_name1);
    }
    else {
        printf("Rank %d Fail to create object @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }

    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_create(cont, obj_name2, obj_prop);
    if (obj2 > 0) {
        printf("Rank %d Create an object %s\n", rank, obj_name2);
    }
    else {
        printf("Rank %d Fail to create object @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }

    // close created objects
    if (PDCobj_close(obj1) < 0) {
        printf("Rank %d fail to close object o1\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object o1\n", rank);
    }
    if (PDCobj_close(obj2) < 0) {
        printf("Rank %d fail to close object o2\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object o2\n", rank);
    }
// Wait for all processes to finish their object creation
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    for (i = 1; i < size; ++i) {
        sprintf(obj_name1, "o1_%d", (rank + i) % size);
        obj1 = PDCobj_open(obj_name1, pdc);
        if (obj1 == 0) {
            printf("Rank %d Fail to open object %s\n", rank, obj_name1);
            ret_value = 1;
        }
        else {
            printf("Rank %d Opened object %s\n", rank, obj_name1);
        }
        sprintf(obj_name2, "o2_%d", (rank + i) % size);
        obj2 = PDCobj_open(obj_name2, pdc);
        if (obj2 == 0) {
            printf("Rank %d Fail to open object %s\n", rank, obj_name2);
            ret_value = 1;
        }
        else {
            printf("Rank %d Open object %s\n", rank, obj_name2);
        }
        obj1_info = PDCobj_get_info(obj1);
        obj2_info = PDCobj_get_info(obj2);
        if (strcmp(obj1_info->name, obj_name1) != 0) {
            printf("Object 1 name is wrong at rank %d\n", rank);
            ret_value = 1;
        }

        if (obj1_info->obj_pt->type != PDC_DOUBLE) {
            printf("Type is not properly inherited from object 1 property at rank %d.\n", rank);
            ret_value = 1;
        }

        if (obj1_info->obj_pt->ndim != ndim) {
            printf("Number of dimensions is not properly inherited from object 1 property at rank %d.\n",
                   rank);
            ret_value = 1;
        }
        if (obj1_info->obj_pt->dims[0] != (unsigned)(i + rank) % size * 2 + 1) {
            printf("First dimension is not properly inherited from object 1 property at rank %d.\n", rank);
            ret_value = 1;
        }
        if (obj1_info->obj_pt->dims[1] != (unsigned)(i + rank) % size * 3 + 2) {
            printf("Second dimension is not properly inherited from object 1 property at rank %d.\n", rank);
            ret_value = 1;
        }
        if (obj1_info->obj_pt->dims[2] != (unsigned)(i + rank) % size * 5 + 3) {
            printf("Third dimension is not properly inherited from object 1 property at rank %d.\n", rank);
            ret_value = 1;
        }
        if (strcmp(obj2_info->name, obj_name2) != 0) {
            printf("Object 2 name is wrong\n");
            ret_value = 1;
        }

        if (obj2_info->obj_pt->type != PDC_DOUBLE) {
            printf("Type is not properly inherited from object property.\n");
            ret_value = 1;
        }

        if (obj2_info->obj_pt->ndim != ndim) {
            printf("Number of dimensions is not properly inherited from object property at rank %d.\n", rank);
            ret_value = 1;
        }
        if (obj2_info->obj_pt->dims[0] != (unsigned)(i + rank) % size * 2 + 1) {
            printf("First dimension is not properly inherited from object property at rank %d.\n", rank);
            ret_value = 1;
        }
        if (obj2_info->obj_pt->dims[1] != (unsigned)(i + rank) % size * 3 + 2) {
            printf("Second dimension is not properly inherited from object property at rank %d.\n", rank);
            ret_value = 1;
        }
        if (obj2_info->obj_pt->dims[2] != (unsigned)(i + rank) % size * 5 + 3) {
            printf("Third dimension is not properly inherited from object property at rank %d.\n", rank);
            ret_value = 1;
        }
        if (PDCobj_close(obj1) < 0) {
            printf("Rank %d fail to close object %s\n", rank, obj_name1);
            ret_value = 1;
        }
        else {
            printf("Rank %d successfully close object %s\n", rank, obj_name1);
        }
        if (PDCobj_close(obj2) < 0) {
            printf("Rank %d fail to close object %s\n", rank, obj_name2);
            ret_value = 1;
        }
        else {
            printf("Rank %d successfully close object %s\n", rank, obj_name2);
        }
    }

    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("Rank %d fail to close container c1\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close container c1\n", rank);
    }
    // close a object property
    if (PDCprop_close(obj_prop) < 0) {
        printf("Rank %d Fail to close property @ line %d\n", rank, __LINE__);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object property\n", rank);
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Rank %d Fail to close property @ line %d\n", rank, __LINE__);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close container property\n", rank);
    }
    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("Rank %d fail to close PDC\n", rank);
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
