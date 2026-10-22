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
    pdcid_t obj1, obj2;
    int     rank = 0, size = 1, i, j, ret, target_rank;
    int     ret_value = TSUCCEED;
    char    cont_name[128], obj_name1[128], obj_name2[128];

    size_t   ndim;
    uint64_t dims[3];

    uint64_t *offset;
    uint64_t *mysize;

    pdc_var_type_t var_type  = PDC_UNKNOWN;
    size_t         type_size = 1;

    uint64_t my_data_size;

    char *mydata, *data_read;

    pdcid_t local_region, global_region;
    pdcid_t transfer_request;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (!strcmp(argv[1], "float")) {
        var_type  = PDC_FLOAT;
        type_size = sizeof(float);
    }
    else if (!strcmp(argv[1], "int")) {
        var_type  = PDC_INT;
        type_size = sizeof(int);
    }
    else if (!strcmp(argv[1], "double")) {
        var_type  = PDC_DOUBLE;
        type_size = sizeof(double);
    }
    else if (!strcmp(argv[1], "char")) {
        var_type  = PDC_CHAR;
        type_size = sizeof(char);
    }
    else if (!strcmp(argv[1], "uint")) {
        var_type  = PDC_UINT;
        type_size = sizeof(unsigned);
    }
    else if (!strcmp(argv[1], "int64")) {
        var_type  = PDC_INT64;
        type_size = sizeof(int64_t);
    }
    else if (!strcmp(argv[1], "uint64")) {
        var_type  = PDC_UINT64;
        type_size = sizeof(uint64_t);
    }
    else if (!strcmp(argv[1], "int16")) {
        var_type  = PDC_INT16;
        type_size = sizeof(int16_t);
    }
    else if (!strcmp(argv[1], "int8")) {
        var_type  = PDC_INT8;
        type_size = sizeof(int8_t);
    }

    ndim = atoi(argv[2]);

    dims[0]      = rank * 2 + 16;
    dims[1]      = rank * 3 + 16;
    dims[2]      = rank * 5 + 16;
    my_data_size = 1;

    for (i = 0; i < (int)ndim; ++i)
        my_data_size *= dims[i];

    mydata = (char *)malloc(my_data_size * type_size);

    offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize    = (uint64_t *)malloc(sizeof(uint64_t));
    offset[0] = 0;
    offset[1] = 0;
    offset[2] = 0;
    mysize[0] = my_data_size;

    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
    sprintf(cont_name, "c");
#ifdef ENABLE_MPI
    TASSERT((cont = PDCcont_create_coll(cont_name, cont_prop, MPI_COMM_WORLD)) != 0,
            "Call to PDCcont_create_coll succeeded", "Call to PDCcont_create_coll failed");
#else
    TASSERT((cont = PDCcont_create(cont_name, cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");
#endif
    // create an object property
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");

    TASSERT(PDCprop_set_obj_dims(obj_prop, ndim, dims) >= 0, "Call to PDCprop_set_obj_dims succeeded",
            "Call to PDCprop_set_obj_dims failed");
    TASSERT(PDCprop_set_obj_type(obj_prop, var_type) >= 0, "Call to PDCprop_set_obj_type succeeded",
            "Call to PDCprop_set_obj_type failed");

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    TASSERT((local_region = PDCregion_create(1, offset, mysize)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    TASSERT((global_region = PDCregion_create(ndim, offset, dims)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");

    TASSERT((obj1 = PDCobj_create(cont, obj_name1, obj_prop)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");

    for (i = 0; i < (int)my_data_size; i++) {
        for (j = 0; j < (int)type_size; ++j)
            mydata[i * type_size + j] = (char)(i * type_size + j + rank);
    }

    TASSERT((transfer_request =
                 PDCregion_transfer_create(mydata, PDC_WRITE, obj1, local_region, global_region)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_create(cont, obj_name2, obj_prop);

    TASSERT((local_region = PDCregion_create(1, offset, mysize)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    TASSERT((global_region = PDCregion_create(ndim, offset, dims)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");

    for (i = 0; i < (int)my_data_size; i++) {
        for (j = 0; j < (int)type_size; ++j)
            mydata[i * type_size + j] = (char)(i * type_size + j + rank * 5 + 3);
    }

    TASSERT((transfer_request =
                 PDCregion_transfer_create(mydata, PDC_WRITE, obj2, local_region, global_region)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    TASSERT(PDCregion_close(local_region) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");
    TASSERT(PDCregion_close(global_region) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");

    // close created objects
    TASSERT(PDCobj_close(obj1) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(obj2) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");

// Wait for all processes to finish their object creation
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    for (i = 1; i < size; ++i) {
        target_rank = (rank + i) % size;
        sprintf(obj_name1, "o1_%d", target_rank);
        TASSERT((obj1 = PDCobj_open(obj_name1, pdc)) != 0, "Call to PDCobj_open succeeded",
                "Call to PDCobj_open failed");
        sprintf(obj_name2, "o2_%d", target_rank);
        TASSERT((obj2 = PDCobj_open(obj_name2, pdc)) != 0, "Call to PDCobj_open succeeded",
                "Call to PDCobj_open failed");

        dims[0]      = target_rank * 2 + 16;
        dims[1]      = target_rank * 3 + 16;
        dims[2]      = target_rank * 5 + 16;
        my_data_size = 1;
        for (j = 0; j < (int)ndim; ++j) {
            my_data_size *= dims[j];
        }

        mysize[0] = my_data_size;
        TASSERT((local_region = PDCregion_create(1, offset, mysize)) != 0,
                "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");
        TASSERT((global_region = PDCregion_create(ndim, offset, dims)) != 0,
                "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");
        data_read = (char *)malloc(my_data_size * type_size);

        TASSERT((transfer_request =
                     PDCregion_transfer_create(data_read, PDC_READ, obj2, local_region, global_region)) != 0,
                "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
        TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
                "Call to PDCregion_transfer_start failed");
        TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
                "Call to PDCregion_transfer_wait failed");
        TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
                "Call to PDCregion_transfer_close failed");

        for (j = 0; j < (int)(my_data_size * type_size); ++j) {
            if (data_read[j] != (char)(j + target_rank * 5 + 3)) {
                TGOTO_ERROR(TFAIL, "Rank %d, i = %d, j = %d, wrong value %d!=%d", rank, i, j, data_read[j],
                            (char)(j + target_rank * 5 + 3));
            }
        }

        free(data_read);

        TASSERT(PDCregion_close(local_region) >= 0, "Call to PDCregion_close succeeded",
                "Call to PDCregion_close failed");
        TASSERT(PDCregion_close(global_region) >= 0, "Call to PDCregion_close succeeded",
                "Call to PDCregion_close failed");

        TASSERT(PDCobj_close(obj1) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
        TASSERT(PDCobj_close(obj2) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    }

    // close a container
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    // close a object property
    TASSERT(PDCprop_close(obj_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close a container property
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

    free(mydata);
    free(offset);
    free(mysize);

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
