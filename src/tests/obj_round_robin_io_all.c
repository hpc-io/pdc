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

#define WRITE_REQ_SIZE 2

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop;
    pdcid_t obj1, obj2, *obj1_list, *obj2_list;
    int     rank = 0, size = 1, i, j, ret, target_rank;
    int     ret_value = 0;
    char    cont_name[128], obj_name1[128], obj_name2[128];
    // struct pdc_obj_info *obj1_info, *obj2_info;

    size_t   ndim;
    uint64_t dims[3];

    uint64_t *offset;
    uint64_t *mysize;

    pdc_var_type_t var_type  = PDC_UNKNOWN;
    size_t         type_size = 1;

    uint64_t my_data_size;

    char **mydata, **data_read;

    pdcid_t  local_region, global_region;
    pdcid_t *transfer_request;

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
    for (i = 0; i < (int)ndim; ++i) {
        my_data_size *= dims[i];
    }

    mydata    = (char **)malloc(size * WRITE_REQ_SIZE);
    mydata[0] = (char *)malloc(my_data_size * type_size);
    mydata[1] = mydata[0] + my_data_size * type_size;

    offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize    = (uint64_t *)malloc(sizeof(uint64_t));
    offset[0] = 0;
    offset[1] = 0;
    offset[2] = 0;
    mysize[0] = my_data_size;

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
    PDCprop_set_obj_type(obj_prop, var_type);
    if (ret != SUCCEED) {
        printf("Fail to set obj time step @ line %d\n", __LINE__);
        ret_value = 1;
    }

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    local_region  = PDCregion_create(1, offset, mysize);
    global_region = PDCregion_create(ndim, offset, dims);

    obj1 = PDCobj_create(cont, obj_name1, obj_prop);
    if (obj1 > 0) {
        printf("Rank %d Create an object %s\n", rank, obj_name1);
    }
    else {
        printf("Rank %d Fail to create object @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    for (i = 0; i < (int)my_data_size; i++) {
        for (j = 0; j < (int)type_size; ++j) {
            mydata[0][i * type_size + j] = (char)(i * type_size + j + rank);
        }
    }
    for (i = 0; i < (int)my_data_size; i++) {
        for (j = 0; j < (int)type_size; ++j) {
            mydata[1][i * type_size + j] = (char)(i * type_size + j + rank * 5 + 3);
        }
    }
    transfer_request = (pdcid_t *)malloc(sizeof(pdcid_t) * size * 2);

    transfer_request[0] = PDCregion_transfer_create(mydata[0], PDC_WRITE, obj1, local_region, global_region);
    if (transfer_request[0] == 0) {
        printf("PDCregion_transfer_create failed @ line %d\n", __LINE__);
        ret_value = 1;
    }
    if (PDCregion_close(local_region) < 0) {
        printf("fail to close local region\n");
        ret_value = 1;
    }

    if (PDCregion_close(global_region) < 0) {
        printf("fail to close global region\n");
        ret_value = 1;
    }

    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_create(cont, obj_name2, obj_prop);

    local_region  = PDCregion_create(1, offset, mysize);
    global_region = PDCregion_create(ndim, offset, dims);
    if (obj2 > 0) {
        printf("Rank %d Create an object %s\n", rank, obj_name2);
    }
    else {
        printf("Rank %d Fail to create object @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }

    transfer_request[1] = PDCregion_transfer_create(mydata[1], PDC_WRITE, obj2, local_region, global_region);
    if (transfer_request[1] == 0) {
        printf("PDCregion_transfer_create failed @ line %d\n", __LINE__);
        ret_value = 1;
    }
    ret = PDCregion_transfer_start_all(transfer_request, WRITE_REQ_SIZE);
    if (ret != SUCCEED) {
        printf("Failed to region_transfer_start for region @ line %d\n", __LINE__);
        ret_value = 1;
    }

    ret = PDCregion_transfer_wait_all(transfer_request, WRITE_REQ_SIZE);
    if (ret != SUCCEED) {
        printf("Failed to region_transfer_wait for region @ line %d\n", __LINE__);
        ret_value = 1;
    }

    ret = PDCregion_transfer_close(transfer_request[0]);
    if (ret != SUCCEED) {
        printf("PDCregion_transfer_close failed @ line %d\n", __LINE__);
        ret_value = 1;
    }

    ret = PDCregion_transfer_close(transfer_request[1]);
    if (ret != SUCCEED) {
        printf("PDCregion_transfer_close failed @ line %d\n", __LINE__);
        ret_value = 1;
    }

    if (PDCregion_close(local_region) < 0) {
        printf("fail to close local region %d\n", __LINE__);
        ret_value = 1;
    }

    if (PDCregion_close(global_region) < 0) {
        printf("fail to close global region %d\n", __LINE__);
        ret_value = 1;
    }

    // close created objects
    if (PDCobj_close(obj1) < 0) {
        printf("Rank %d fail to close object o1_%d %d\n", rank, rank, __LINE__);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object o1_%d\n", rank, rank);
    }
    if (PDCobj_close(obj2) < 0) {
        printf("Rank %d fail to close object o2_%d %d\n", rank, rank, __LINE__);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object o2_%d\n", rank, rank);
    }
// Wait for all processes to finish their object creation
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    data_read = (char **)malloc(sizeof(char *) * (size - 1) * 2);
    obj1_list = (pdcid_t *)malloc(sizeof(pdcid_t) * (size - 1) * 2);
    obj2_list = (pdcid_t *)malloc(sizeof(pdcid_t) * (size - 1) * 2);

    for (i = 1; i < size; ++i) {
        target_rank = (rank + i) % size;
        sprintf(obj_name1, "o1_%d", target_rank);
        obj1_list[i - 1] = PDCobj_open(obj_name1, pdc);
        if (obj1_list[i - 1] == 0) {
            printf("Rank %d Fail to open object %s %d\n", rank, obj_name1, __LINE__);
            ret_value = 1;
        }
        else {
            printf("Rank %d Opened object %s\n", rank, obj_name1);
        }

        dims[0]      = target_rank * 2 + 16;
        dims[1]      = target_rank * 3 + 16;
        dims[2]      = target_rank * 5 + 16;
        my_data_size = 1;
        for (j = 0; j < (int)ndim; ++j) {
            my_data_size *= dims[j];
        }

        mysize[0]          = my_data_size;
        local_region       = PDCregion_create(1, offset, mysize);
        global_region      = PDCregion_create(ndim, offset, dims);
        data_read[(i - 1)] = (char *)malloc(my_data_size * type_size);

        transfer_request[(i - 1)] =
            PDCregion_transfer_create(data_read[(i - 1)], PDC_READ, obj2, local_region, global_region);
        if (transfer_request[(i - 1)] == 0) {
            printf("PDCregion_transfer_create for read obj2 failed %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(local_region) < 0) {
            printf("fail to close local region %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(global_region) < 0) {
            printf("fail to close global region %d\n", __LINE__);
            ret_value = 1;
        }

        sprintf(obj_name2, "o2_%d", target_rank);
        obj2_list[i - 1] = PDCobj_open(obj_name2, pdc);
        if (obj2_list[i - 1] == 0) {
            printf("Rank %d Fail to open object %s %d\n", rank, obj_name2, __LINE__);
            ret_value = 1;
        }
        else {
            printf("Rank %d Open object %s\n", rank, obj_name2);
        }

        mysize[0]                     = my_data_size;
        local_region                  = PDCregion_create(1, offset, mysize);
        global_region                 = PDCregion_create(ndim, offset, dims);
        data_read[(i - 1) + size - 1] = (char *)malloc(my_data_size * type_size);

        transfer_request[(i - 1) + size - 1] = PDCregion_transfer_create(
            data_read[(i - 1) + size - 1], PDC_READ, obj2, local_region, global_region);
        if (transfer_request[(i - 1) + size - 1] == 0) {
            printf("PDCregion_transfer_create for read obj2 failed %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(local_region) < 0) {
            printf("fail to close local region %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(global_region) < 0) {
            printf("fail to close global region %d\n", __LINE__);
            ret_value = 1;
        }
    }

    ret = PDCregion_transfer_start_all(transfer_request, (size - 1) * 2);
    if (ret != SUCCEED) {
        printf("Failed to region_transfer_start_all for region @ line %d\n", __LINE__);
        ret_value = 1;
    }

    ret = PDCregion_transfer_wait_all(transfer_request, (size - 1) * 2);
    if (ret != SUCCEED) {
        printf("Failed to region_transfer_wait_all for region @ line %d\n", __LINE__);
        ret_value = 1;
    }

    for (i = 1; i < size; ++i) {
        target_rank = (rank + i) % size;
        ret         = PDCregion_transfer_close(transfer_request[(i - 1)]);
        if (ret != SUCCEED) {
            printf("PDCregion_transfer_close failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        ret = PDCregion_transfer_close(transfer_request[(i - 1) + size - 1]);
        if (ret != SUCCEED) {
            printf("PDCregion_transfer_close failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        for (j = 0; j < (int)(my_data_size * type_size); ++j) {
            if (data_read[(i - 1)][j] != (char)(j + target_rank)) {
                printf("rank %d, i = %d, j = %d, wrong value %d!=%d %d\n", rank, i, j, data_read[(i - 1)][j],
                       (char)(j + target_rank), __LINE__);
                ret_value = 1;
                break;
            }
        }

        for (j = 0; j < (int)(my_data_size * type_size); ++j) {
            if (data_read[(i - 1) + size - 1][j] != (char)(j + target_rank * 5 + 3)) {
                printf("rank %d, i = %d, j = %d, wrong value %d!=%d %d\n", rank, i, j,
                       data_read[(i - 1) + size - 1][j], (char)(j + target_rank * 5 + 3), __LINE__);
                ret_value = 1;
                break;
            }
        }
        free(data_read[(i - 1)]);
        free(data_read[(i - 1) + size - 1]);
        if (PDCobj_close(obj1_list[i - 1]) < 0) {
            printf("Rank %d fail to close object %d\n", rank, __LINE__);
            ret_value = 1;
        }
        else {
            printf("Rank %d successfully close object\n", rank);
        }
        if (PDCobj_close(obj2_list[i - 1]) < 0) {
            printf("Rank %d fail to close object %d\n", rank, __LINE__);
            ret_value = 1;
        }
        else {
            printf("Rank %d successfully close object\n", rank);
        }
    }

    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("Rank %d fail to close container c %d\n", rank, __LINE__);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close container c\n", rank);
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

    printf("total number of read request = %d\n", (size - 1) * 2);
    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("Rank %d fail to close PDC %d\n", rank, __LINE__);
        ret_value = 1;
    }

    free(transfer_request);
    free(obj1_list);
    free(obj2_list);
    free(data_read);
    free(mydata);
    free(offset);
    free(mysize);
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
