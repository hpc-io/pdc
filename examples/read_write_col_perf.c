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
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/time.h>
#include "pdc.h"
#include "pdc_analysis.h"

#include "pdc_timing.h"

#define BUF_LEN 128

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop, reg, reg_global;
    perr_t  ret;
    pdcid_t obj1;
    char    cont_name[128], obj_name1[128];

    int rank = 0, size = 1, i, ndim = 0, ret_value = 0;

    uint64_t offset[3], offset_length[3], local_offset[3], dims[3];
    int      data_size, data_size_array[3], n_objects;

    double start, write_reg_transfer_start_time = 0, write_reg_transfer_wait_time = 0,
                  read_reg_transfer_start_time = 0, read_reg_transfer_wait_time = 0;

    pdcid_t transfer_request;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    data_size_array[0] = 0;
    data_size_array[1] = 0;
    data_size_array[2] = 0;

    if (argc == 3) {
        ndim = 1;
    }
    else if (argc == 4) {
        ndim = 2;
    }
    else if (argc == 5) {
        ndim = 3;
    }
    else {
        printf("usage: ./read_write_perf n_objects data_size1 datasize2 datasize3 @ line %d\n", __LINE__);
    }

    local_offset[0]    = 0;
    data_size_array[0] = atoi(argv[2]);
    if (ndim == 1) {
        offset[0]        = rank * data_size_array[0] * 1048576;
        offset_length[0] = data_size_array[0] * 1048576;
        data_size        = data_size_array[0] * 1048576;
    }
    else {
        offset[0]        = rank * data_size_array[0];
        offset_length[0] = data_size_array[0];
        data_size        = data_size_array[0];
    }
    dims[0] = offset_length[0] * size;
    if (ndim == 2) {
        local_offset[1]    = 0;
        offset[1]          = 0;
        data_size_array[1] = atoi(argv[3]);
        offset_length[1]   = data_size_array[1] * 1048576;
        data_size *= offset_length[1];
        dims[1] = offset_length[1];
    }
    else if (ndim == 3) {
        local_offset[1]    = 0;
        offset[1]          = 0;
        data_size_array[1] = atoi(argv[3]);
        offset_length[1]   = data_size_array[1];

        local_offset[2]    = 0;
        offset[2]          = 0;
        data_size_array[2] = atoi(argv[4]);
        offset_length[2]   = data_size_array[2] * 1048576;
        data_size *= offset_length[2] * offset_length[1];

        dims[1] = offset_length[1];
        dims[2] = offset_length[2];
    }
    n_objects = atoi(argv[1]);
    int *data = (int *)malloc(sizeof(int) * data_size);

    char hostname[256];
    gethostname(hostname, 256);
    if (rank == 0) {
        printf("number of dimensions in this test is %d\n", ndim);
        printf("data size = %llu\n", (long long unsigned)data_size);
        printf("first dim has size %" PRIu64 "\n", dims[0]);
        if (ndim >= 2) {
            printf("second dim has size %" PRIu64 "\n", dims[1]);
        }
        if (ndim == 3) {
            printf("third dim has size %" PRIu64 "\n", dims[2]);
        }
    }

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);
    if (cont <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0) {
        printf("Fail to create object property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    ret = PDCprop_set_obj_type(obj_prop, PDC_INT);
    if (ret != SUCCEED) {
        printf("Fail to set obj type @ line %d\n", __LINE__);
        ret_value = 1;
    }
    PDCprop_set_obj_dims(obj_prop, ndim, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    for (i = 0; i < n_objects; ++i) {
        // create first object
        sprintf(obj_name1, "o1_%d", i);
#ifdef ENABLE_MPI
        obj1 = PDCobj_create_mpi(cont, obj_name1, obj_prop, 0, MPI_COMM_WORLD);
#else
        obj1 = PDCobj_create(cont, obj_name1, obj_prop);
#endif

        if (obj1 <= 0) {
            printf("Fail to create object @ line  %d!\n", __LINE__);
            ret_value = 1;
        }
        reg        = PDCregion_create(ndim, local_offset, offset_length);
        reg_global = PDCregion_create(ndim, offset, offset_length);

        memset(data, (char)i, sizeof(int) * data_size);

        transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global);
        if (transfer_request == 0) {
            printf("PDCregion_transfer_create failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        start = MPI_Wtime();
        ret   = PDCregion_transfer_start(transfer_request);
        write_reg_transfer_start_time += MPI_Wtime() - start;
        if (ret != SUCCEED) {
            printf("PDCregion_transfer_start failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        start = MPI_Wtime();
        ret   = PDCregion_transfer_wait(transfer_request);
        write_reg_transfer_wait_time += MPI_Wtime() - start;
        if (ret != SUCCEED) {
            printf("PDCregion_transfer_wait failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        ret = PDCregion_transfer_close(transfer_request);

        if (ret != SUCCEED) {
            printf("PDCregion_transfer_close failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(reg) < 0) {
            printf("fail to close local region @ line %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(reg_global) < 0) {
            printf("fail to close global region @ line %d\n", __LINE__);
            ret_value = 1;
        }
        if (PDCobj_close(obj1) < 0) {
            printf("fail to close object o1 @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
#if PDC_TIMING == 1
    PDC_timing_report("write");
#endif
    for (i = 0; i < n_objects; ++i) {
        sprintf(obj_name1, "o1_%d", i);
        obj1 = PDCobj_open(obj_name1, pdc);
        if (obj1 <= 0) {
            printf("Fail to open object @ line  %d!\n", __LINE__);
            ret_value = 1;
        }

        reg        = PDCregion_create(ndim, local_offset, offset_length);
        reg_global = PDCregion_create(ndim, offset, offset_length);

        memset(data, 0, sizeof(int) * data_size);

        transfer_request = PDCregion_transfer_create(data, PDC_READ, obj1, reg, reg_global);

        if (transfer_request == 0) {
            printf("PDCregion_transfer_create failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        start = MPI_Wtime();
        ret   = PDCregion_transfer_start(transfer_request);
        read_reg_transfer_start_time += MPI_Wtime() - start;

        if (ret != SUCCEED) {
            printf("PDCregion_transfer_start failed @ line %d\n", __LINE__);
            exit(-1);
        }

        start = MPI_Wtime();
        ret   = PDCregion_transfer_wait(transfer_request);
        read_reg_transfer_wait_time += MPI_Wtime() - start;

        if (ret != SUCCEED) {
            printf("PDCregion_transfer_wait failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        ret = PDCregion_transfer_close(transfer_request);

        if (ret != SUCCEED) {
            printf("PDCregion_transfer_close failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        if (ret != SUCCEED) {
            printf("PDCbuf_obj_unmap failed @ line %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(reg) < 0) {
            printf("fail to close local region @ line %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCregion_close(reg_global) < 0) {
            printf("fail to close global region @ line %d\n", __LINE__);
            ret_value = 1;
        }

        if (PDCobj_close(obj1) < 0) {
            printf("fail to close object o1 @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
#if PDC_TIMING == 1
    PDC_timing_report("read");
#endif
    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container c1 @ line %d\n", __LINE__);
        ret_value = 1;
    }
    // close a object property
    if (PDCprop_close(obj_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("fail to close PDC @ line %d\n", __LINE__);
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Reduce(&write_reg_transfer_start_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    write_reg_transfer_start_time = start;
    if (!rank) {
        printf("write transfer start time: %lf\n", write_reg_transfer_start_time);
    }

    MPI_Reduce(&write_reg_transfer_wait_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    write_reg_transfer_wait_time = start;
    if (!rank) {
        printf("write transfer wait time: %lf\n", write_reg_transfer_wait_time);
    }

    MPI_Reduce(&read_reg_transfer_start_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    read_reg_transfer_start_time = start;
    if (!rank) {
        printf("read transfer start time: %lf\n", read_reg_transfer_start_time);
    }

    MPI_Reduce(&read_reg_transfer_wait_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    read_reg_transfer_wait_time = start;
    if (!rank) {
        printf("read region transfer wait time: %lf\n", read_reg_transfer_wait_time);
    }

    free(data);
#endif

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
