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
#include <math.h>
#include <sys/time.h>

#include "pdc.h"

int
main(int argc, char **argv)
{
    int      i, ndim, cnt, len;
    uint64_t dims[4], offset_local[4] = {0, 0, 0, 0}, offset_remote[4], count[4], total_size, unit_size = 4;
    uint64_t obj_dims[4];
    pdcid_t  pdc_id, cont_prop, cont_id, obj_prop, region_local, region_remote, obj_id, transfer_request;
    char     obj_name[256], *fname;
    void *   data;
    perr_t   ret;
    pdc_var_type_t dtype;
    struct timeval t0;
    struct timeval t1;
    double         elapsed;

    fname = argv[1];

    FILE *file = fopen(fname, "rb");
    if (file == NULL) {
        printf("Failed to open file for writing: %s", fname);
        exit(-1);
    }

    // Read header metadata
    fread(&len, sizeof(int), 1, file);
    memset(obj_name, 0, len);
    fread(obj_name, sizeof(char), len, file);
    fread(&dtype, sizeof(int), 1, file);
    fread(&ndim, sizeof(int), 1, file);
    fread(obj_dims, sizeof(uint64_t), ndim, file);
    fread(offset_remote, sizeof(uint64_t), ndim, file);
    fread(count, sizeof(uint64_t), ndim, file);
    fread(&total_size, sizeof(uint64_t), 1, file);

    data = (void *)malloc(total_size);

    fread(data, sizeof(char), total_size, file);
    fclose(file);

    printf("Transfer obj name [%s]\ndtype:%s\noffsets: ", obj_name, get_enum_name_by_dtype(dtype));
    for (i = 0; i < ndim; i++)
        printf("%llu ", offset_remote[i]);
    printf("\ncounts: ");
    for (i = 0; i < ndim; i++)
        printf("%llu ", count[i]);
    printf("\n");

    pdc_id = PDCinit("pdc");

    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        return FAIL;
    }
    cont_id = PDCcont_create("c1", cont_prop);
    if (cont_id <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        return FAIL;
    }

    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    PDCprop_set_obj_dims(obj_prop, ndim, obj_dims);
    PDCprop_set_obj_type(obj_prop, dtype);
    PDCprop_set_obj_user_id(obj_prop, getuid());

    obj_id = PDCobj_create(cont_id, obj_name, obj_prop);

    region_local  = PDCregion_create(ndim, offset_local, count);
    region_remote = PDCregion_create(ndim, offset_remote, count);

    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj_id, region_local, region_remote);

    gettimeofday(&t0, NULL);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        printf("Failed to start transfer for region_xx\n");
        exit(-1);
    }

    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        printf("Failed to wait transfer\n");
        exit(-1);
    }

    gettimeofday(&t1, NULL);
    elapsed = t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1000000.0;
    printf("Pull data to PDC took %.2fs\n", elapsed);

    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        printf("Failed to close region transfer\n");
        exit(-1);
    }

    free(data);
    ret = PDCobj_close(obj_id);
    if (ret < 0) {
        printf("fail to close obj\n");
        exit(-1);
    }

    ret = PDCregion_close(region_local);
    if (ret < 0) {
        printf("fail to close region local\n");
        exit(-1);
    }

    ret = PDCregion_close(region_remote);
    if (ret < 0) {
        printf("fail to close region remote\n");
        exit(-1);
    }

    ret = PDCcont_close(cont_id);
    if (ret < 0) {
        printf("fail to close container\n");
        exit(-1);
    }

    ret = PDCclose(pdc_id);
    if (ret < 0) {
        printf("fail to close PDC\n");
        exit(-1);
    }

    return 0;
}
