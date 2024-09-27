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

#include "pdc.h"

int
main(int argc, char **argv)
{
    int       i, ndim, cnt;
    uint64_t *obj_dims, dims[4], offset_local[4] = {0, 0, 0, 0}, offset_remote[4], count[4], total_size,
                                 unit_size = 4;
    pdcid_t        pdc_id, cont_prop, cont_id, region_local, region_remote, obj_id, transfer_request;
    char *         cont_name = "c1", *obj_name = "x", out_path[256], out_name[256];
    void *         data;
    perr_t         ret;
    pdc_var_type_t dtype;

    if (argc > 1) {
        cont_name = argv[1];
        obj_name  = argv[2];
    }
    else {
        printf("Usage:\n ./pdc_region_push cont_name, obj_name, ndim, offsets[], counts[]");
        return 0;
    }

    pdc_id = PDCinit("pdc");

    cont_id = PDCcont_open(cont_name, pdc_id);
    if (cont_id == 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        exit(-1);
    }

    obj_id = PDCobj_open(obj_name, pdc_id);
    if (obj_id == 0) {
        printf("Error when open object %s\n", "obj-var-xx");
        exit(-1);
    }

    dtype     = PDCobj_get_dtype(obj_id);
    unit_size = PDC_get_var_type_size(dtype);

    PDCobj_get_dims(obj_id, &ndim, &obj_dims);

    ndim = atoi(argv[3]);
    cnt  = 4;
    for (i = 0; i < ndim; i++)
        offset_remote[i] = atoll(argv[cnt++]);

    total_size = unit_size;
    for (i = 0; i < ndim; i++) {
        count[i] = atoll(argv[cnt++]);
        total_size *= count[i];
    }

    printf("Transfer obj name [%s]\ndtype:%s\noffsets: ", obj_name, get_enum_name_by_dtype(dtype));
    for (i = 0; i < ndim; i++)
        printf("%llu ", offset_remote[i]);
    printf("\ncounts: ");
    for (i = 0; i < ndim; i++)
        printf("%llu ", count[i]);
    printf("\n");

    data = (void *)malloc(total_size);

    region_local  = PDCregion_create(ndim, offset_local, count);
    region_remote = PDCregion_create(ndim, offset_remote, count);

    transfer_request = PDCregion_transfer_create(data, PDC_READ, obj_id, region_local, region_remote);

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

    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        printf("Failed to close region transfer\n");
        exit(-1);
    }

    // Write data to a bin file
    sprintf(out_path, "./pdc_region_push_data"); // harded coded for now
    mkdir(out_path, 0755);
    sprintf(out_name, "%s/data.bin", out_path); // harded coded for now

    FILE *file = fopen(out_name, "wb");
    if (file == NULL) {
        printf("Failed to open file for writing: %s", out_name);
        exit(-1);
    }

    // Write header metadata
    int len = strlen(obj_name);
    fwrite(&len, sizeof(int), 1, file);
    fwrite(obj_name, sizeof(char), len, file);
    fwrite(&dtype, sizeof(int), 1, file);
    fwrite(&ndim, sizeof(int), 1, file);
    fwrite(obj_dims, sizeof(uint64_t), ndim, file);
    fwrite(offset_remote, sizeof(uint64_t), ndim, file);
    fwrite(count, sizeof(uint64_t), ndim, file);

    // Write data
    printf("Writing %llu bytes\n", total_size);
    fwrite(&total_size, sizeof(uint64_t), 1, file);
    fwrite(data, sizeof(char), total_size, file);
    fclose(file);

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
