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
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"
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
#ifdef ENABLE_MPI
    double start, write_buf_map_time = 0, write_lock_time = 0, write_release_time = 0,
                  write_unbuf_map_time = 0, read_buf_map_time = 0, read_lock_time = 0, read_release_time = 0,
                  read_unbuf_map_time = 0;
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
    offset[0]          = 0;
    data_size_array[0] = atoi(argv[2]);
    if (ndim == 1) {
        offset_length[0] = data_size_array[0] * 1048576;
        data_size        = data_size_array[0] * 1048576;
    }
    else {
        offset_length[0] = data_size_array[0];
        data_size        = data_size_array[0];
    }
    if (ndim == 2) {
        local_offset[1]    = 0;
        offset[1]          = 0;
        data_size_array[1] = atoi(argv[3]);
        offset_length[1]   = data_size_array[1] * 1048576;
        data_size *= data_size_array[1] * 1048576;
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
        data_size *= (data_size_array[1] * data_size_array[2] * 1048576);
    }
    n_objects      = atoi(argv[1]);
    int *data      = (int *)malloc(sizeof(int) * data_size);
    int *data_read = (int *)malloc(sizeof(int) * data_size);
    int *obj_data  = (int *)calloc(data_size, sizeof(int));

    memcpy(dims, offset_length, sizeof(uint64_t) * ndim);
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    char hostname[256];
    gethostname(hostname, 256);
    printf("Client program read_write_perf Rank %d at hostname %s\n", rank, hostname);
    if (rank == 0) {
        printf("number of dimensions in this test is %d\n", ndim);
        printf("data size = %llu\n", (long long unsigned)data_size);
        printf("first dim has size %d\n", data_size_array[0]);
        if (ndim >= 2) {
            printf("second dim has size %d\n", data_size_array[1]);
        }
        if (ndim == 3) {
            printf("third dim has size %d\n", data_size_array[2]);
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
    PDCprop_set_obj_buf(obj_prop, obj_data);
    PDCprop_set_obj_dims(obj_prop, ndim, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    for (i = 0; i < n_objects; ++i) {
        // create first object
        sprintf(obj_name1, "o1_%d_%d", rank, i);
        obj1 = PDCobj_create(cont, obj_name1, obj_prop);
        if (obj1 <= 0) {
            printf("Fail to create object @ line  %d!\n", __LINE__);
            ret_value = 1;
        }

        reg        = PDCregion_create(ndim, offset, offset_length);
        reg_global = PDCregion_create(ndim, offset, offset_length);

        memset(data, (char)i, sizeof(int) * data_size);
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        start = MPI_Wtime();
        ret   = PDCbuf_obj_map(data, PDC_INT, reg, obj1, reg_global);
        write_buf_map_time += MPI_Wtime() - start;
#endif
        if (ret != SUCCEED) {
            printf("PDCbuf_obj_map failed @ line %d\n", __LINE__);
            ret_value = 1;
        }
#ifdef ENABLE_MPI
        start = MPI_Wtime();
        ret   = PDCreg_obtain_lock(obj1, reg_global, PDC_WRITE, PDC_BLOCK);
        write_lock_time += MPI_Wtime() - start;
#endif
        if (ret != SUCCEED) {
            printf("PDCreg_obtain_lock failed @ line %d\n", __LINE__);
            exit(-1);
        }
#ifdef ENABLE_MPI
        start = MPI_Wtime();
        ret   = PDCreg_release_lock(obj1, reg_global, PDC_WRITE);
        write_release_time += MPI_Wtime() - start;
#endif
        if (ret != SUCCEED) {
            printf("PDCreg_release_lock failed @ line %d\n", __LINE__);
            ret_value = 1;
        }
#ifdef ENABLE_MPI
        start = MPI_Wtime();
        ret   = PDCbuf_obj_unmap(obj1, reg_global);
        write_unbuf_map_time += MPI_Wtime() - start;
#endif
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
    MPI_Barrier(MPI_COMM_WORLD);
    PDC_timing_report("write");
#endif
    for (i = 0; i < n_objects; ++i) {
        sprintf(obj_name1, "o1_%d_%d", rank, i);
        obj1 = PDCobj_open(obj_name1, pdc);
        if (obj1 <= 0) {
            printf("Fail to open object @ line  %d!\n", __LINE__);
            ret_value = 1;
        }

        reg        = PDCregion_create(ndim, local_offset, offset_length);
        reg_global = PDCregion_create(ndim, offset, offset_length);

        memset(data, 0, sizeof(int) * data_size);
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        start = MPI_Wtime();
        ret   = PDCbuf_obj_map(data_read, PDC_INT, reg, obj1, reg_global);
        read_buf_map_time += MPI_Wtime() - start;
#endif
        if (ret != SUCCEED) {
            printf("PDCbuf_obj_map failed @ line %d\n", __LINE__);
            ret_value = 1;
        }
#ifdef ENABLE_MPI
        start = MPI_Wtime();
        ret   = PDCreg_obtain_lock(obj1, reg_global, PDC_READ, PDC_BLOCK);
        read_lock_time += MPI_Wtime() - start;
#endif
        if (ret != SUCCEED) {
            printf("PDCreg_obtain_lock failed @ line %d\n", __LINE__);
            ret_value = 1;
        }
#ifdef ENABLE_MPI
        start = MPI_Wtime();
        ret   = PDCreg_release_lock(obj1, reg_global, PDC_READ);
        read_release_time += MPI_Wtime() - start;
#endif
        if (ret != SUCCEED) {
            printf("PDCreg_release_lock failed @ line %d\n", __LINE__);
            ret_value = 1;
        }
#ifdef ENABLE_MPI
        start = MPI_Wtime();
        ret   = PDCbuf_obj_unmap(obj1, reg_global);
        read_unbuf_map_time += MPI_Wtime() - start;
#endif
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
    MPI_Reduce(&write_buf_map_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    write_buf_map_time = start;
    if (!rank) {
        printf("write buf map obj time: %lf\n", write_buf_map_time);
    }
    MPI_Reduce(&write_lock_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    write_lock_time = start;
    if (!rank) {
        printf("write lock obtain time: %lf\n", write_lock_time);
    }

    MPI_Reduce(&write_release_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    write_release_time = start;
    if (!rank) {
        printf("write lock release time: %lf\n", write_release_time);
    }

    MPI_Reduce(&write_unbuf_map_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    write_unbuf_map_time = start;
    if (!rank) {
        printf("write buf unmap obj time: %lf\n", write_unbuf_map_time);
    }

    MPI_Reduce(&read_buf_map_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    read_buf_map_time = start;
    if (!rank) {
        printf("read buf map obj time: %lf\n", read_buf_map_time);
    }
    MPI_Reduce(&read_lock_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    read_lock_time = start;
    if (!rank) {
        printf("read lock obtain time: %lf\n", read_lock_time);
    }

    MPI_Reduce(&read_release_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    read_release_time = start;
    if (!rank) {
        printf("read lock release time: %lf\n", read_release_time);
    }

    MPI_Reduce(&read_unbuf_map_time, &start, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    read_unbuf_map_time = start;
    if (!rank) {
        printf("read buf unmap obj time: %lf\n", read_unbuf_map_time);
    }
#endif
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
