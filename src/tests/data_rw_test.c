#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

int     rank = 0, size = 1;
pdcid_t pdc;
pdcid_t cont;
pdcid_t cont_prop;

void
print_usage()
{
    printf("Usage: srun -n ./data_rw_test\n");
}

pdcid_t
create_obj(char *obj_name, uint32_t ndim, uint64_t *dims)
{
    pdcid_t obj_id = 0;

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    PDCprop_set_obj_dims(obj_prop, ndim, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // Create a object with only rank 0
    if (rank == 0) {
        obj_id = PDCobj_create(cont, obj_name, obj_prop);
        if (obj_id <= 0) {
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            exit(-1);
        }
    }

    return obj_id;
}

int
print_data(int ndim, uint64_t *start, uint64_t *count, void *data)
{
    uint64_t i, j;
    char *   data_1d, **data_2d, ***data_3d;

    if (ndim == 1) {
        data_1d = (char *)data;
        printf("[%.*s]\n", (int)count[0], data_1d + start[0]);
    }
    else if (ndim == 2) {
        data_2d = (char **)data;
        for (j = 0; j < count[1]; j++) {
            printf("[%.*s]\n", (int)count[0], data_2d[j + start[1]] + start[0]);
        }
    }
    else if (ndim == 3) {
        data_3d = (char ***)data;
        for (i = 0; i < count[2]; i++) {
            for (j = 0; j < count[1]; j++) {
                printf("[%.*s]\n", (int)count[0], data_3d[i + start[2]][j + start[1]] + start[0]);
            }
            printf("\n");
        }
    }
    return 1;
}

int
data_verify(int ndim, uint64_t *start, uint64_t *count, void *data, uint64_t *truth_start, void *truth_data)
{
    uint64_t i, j, k;
    char *   data_1d, **data_2d, ***data_3d;
    char *   truth_data_1d, **truth_data_2d, ***truth_data_3d;

    if (ndim == 1) {
        data_1d       = (char *)data;
        truth_data_1d = (char *)truth_data;
        for (k = 0; k < count[0]; k++) {
            if (data_1d[k + start[0]] != truth_data_1d[k + truth_start[0]]) {
                printf("(%" PRIu64 ") %c / %c\n", k, data_1d[k + start[0]],
                       truth_data_1d[k + truth_start[0]]);
                return -1;
            }
        }
    }
    else if (ndim == 2) {
        data_2d       = (char **)data;
        truth_data_2d = (char **)truth_data;
        for (j = 0; j < count[1]; j++) {
            for (k = 0; k < count[0]; k++) {
                if (data_2d[j + start[1]][k + start[0]] !=
                    truth_data_2d[j + truth_start[1]][k + truth_start[0]]) {
                    printf("(%" PRIu64 ", %" PRIu64 ") %c / %c\n", j, k, data_2d[j + start[1]][k + start[0]],
                           truth_data_2d[j + truth_start[1]][k + truth_start[0]]);
                    return -1;
                }
            }
        }
    }
    else if (ndim == 3) {
        data_3d       = (char ***)data;
        truth_data_3d = (char ***)truth_data;
        for (i = 0; i < count[2]; i++) {
            for (j = 0; j < count[1]; j++) {
                for (k = 0; k < count[0]; k++) {
                    if (data_3d[i + start[2]][j + start[1]][k + start[0]] !=
                        truth_data_3d[i + truth_start[2]][j + truth_start[1]][k + truth_start[0]]) {
                        printf("(%" PRIu64 ", %" PRIu64 ", %" PRIu64 ") %c / %c\n", i, j, k,
                               data_3d[i + start[2]][j + start[1]][k + start[0]],
                               truth_data_3d[i + truth_start[2]][j + truth_start[1]][k + truth_start[0]]);
                        return -1;
                    }
                }
            }
        }
    }
    return 1;
}

int
test1d(char *obj_name)
{

    uint64_t               dims[3];
    uint64_t               i = 0;
    pdcid_t                obj_id;
    pdc_metadata_t *       metadata;
    struct pdc_region_info region_a, region_b, region_c, region_d, read_region;
    uint64_t storage_offset_a, storage_offset_b, storage_offset_c, storage_offset_d, read_offset;
    uint64_t storage_size_a, storage_size_b, storage_size_c, storage_size_d, read_size;
    uint32_t ndim = 1;
    uint64_t total_size;
    char *   data, *data0, *data1, *data2, *data3, *read_data;
    uint64_t data_offset_real[3];
    uint64_t data_start[3] = {0, 0, 0};
    int      is_correct = 0, is_all_correct = 0;

    dims[0] = 120;
    dims[1] = 0;
    dims[2] = 0;
    obj_id  = create_obj(obj_name, ndim, dims);
    if (obj_id == 0) {
        printf("[%d]: Error creating an object [%s]\n", rank, obj_name);
        exit(-1);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL) {
        printf("Error getting metadata from server!\n");
        exit(-1);
    }

    storage_offset_a = 10 + rank * dims[0];
    storage_offset_b = 25 + rank * dims[0];
    storage_offset_c = 45 + rank * dims[0];
    storage_offset_d = 70 + rank * dims[0];

    storage_size_a = 15;
    storage_size_b = 20;
    storage_size_c = 25;
    storage_size_d = 30;

    region_a.ndim = ndim;
    region_b.ndim = ndim;
    region_c.ndim = ndim;
    region_d.ndim = ndim;

    region_a.offset = &storage_offset_a;
    region_b.offset = &storage_offset_b;
    region_c.offset = &storage_offset_c;
    region_d.offset = &storage_offset_d;

    region_a.size = &storage_size_a;
    region_b.size = &storage_size_b;
    region_c.size = &storage_size_c;
    region_d.size = &storage_size_d;

    total_size = storage_size_a + storage_size_b + storage_size_c + storage_size_d;
    data       = (char *)calloc(total_size, sizeof(char));

    data0 = data;
    data1 = data + storage_size_a;
    data2 = data + storage_size_a + storage_size_b;
    data3 = data + storage_size_a + storage_size_b + storage_size_c;

    for (i = 0; i < total_size; i++) {
        if (i < storage_size_a)
            data[i] = i % 13 + 'A';
        else if (i < storage_size_a + storage_size_b)
            data[i] = i % 13 + 'a';
        else if (i < storage_size_a + storage_size_b + storage_size_c)
            data[i] = i % 13 + 'A' + 13;
        else
            data[i] = i % 13 + 'a' + 13;
    }

    PDC_Client_write_wait_notify(metadata, &region_a, data0);
    PDC_Client_write_wait_notify(metadata, &region_b, data1);
    PDC_Client_write_wait_notify(metadata, &region_c, data2);
    PDC_Client_write_wait_notify(metadata, &region_d, data3);

    read_offset        = 15 + rank * dims[0];
    read_size          = 80;
    read_region.ndim   = ndim;
    read_region.offset = &read_offset;
    read_region.size   = &read_size;

    read_data = (char *)calloc(sizeof(char), read_size + 1);

    PDC_Client_read_wait_notify(metadata, &read_region, read_data);

    read_data[read_size] = 0;

    data_offset_real[0] = read_region.offset[0] - region_a.offset[0];

    is_correct = data_verify(ndim, data_start, read_region.size, read_data, data_offset_real, data);
    if (is_correct != 1)
        printf("proc %d: verification failed!\n", rank);
    is_all_correct = is_correct;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&is_correct, &is_all_correct, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#endif

    if (rank == 0) {
        printf("1D data verfication ...");
        if (is_all_correct != size)
            printf("FAILED!\n");
        else
            printf("SUCCEED!\n");
    }
    free(data);

    return 1;
}

int
test2d(char *obj_name)
{
    pdcid_t                obj_id;
    pdc_metadata_t *       metadata;
    struct pdc_region_info region_a, region_b, region_c, region_d, read_region;
    uint64_t               storage_offset_a[3], storage_offset_b[3], storage_offset_c[3], storage_offset_d[3],
        read_offset[3];
    uint64_t storage_size_a[3], storage_size_b[3], storage_size_c[3], storage_size_d[3], read_size[3];
    uint64_t prob_domain[3], storage_domain[3];
    uint64_t i, j, data_total_size;
    uint64_t data_start[3] = {0, 0, 0};
    uint64_t data_offset_real[3];
    int      is_correct = 0, is_all_correct = 0;
    char *   data, *read_data;
    char **  data_2d, **data_a_2d, **data_b_2d, **data_c_2d, **data_d_2d, **read_data_2d;
    uint32_t ndim = 2;

    prob_domain[0] = 25 * size;
    prob_domain[1] = 25 * size;
    prob_domain[2] = 0;

    obj_id = create_obj(obj_name, ndim, prob_domain);
    if (obj_id == 0) {
        printf("[%d]: Error creating an object [%s]\n", rank, obj_name);
        exit(-1);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL) {
        printf("Error getting metadata from server!\n");
        exit(-1);
    }

    // Storage Regions  start     size
    //            a      5,10     4, 6
    //            b      9,10    11, 6
    //            c      5,16     4, 9
    //            d      9,16    11, 9
    //
    //         5       9               25                 8             18
    //    |-----------------------------|        |-----------------------------|
    //    |                             |        |                             |
    //    |                             |        |                             |
    // 10 |    |-------|----------------|        |                             |
    //    |    |   a   |       b        |     14 |        |--------------|     |
    // 16 |    |-------|----------------|        |        |  selection   |     |
    //    |    |       |                |     18 |        |--------------|     |
    //    |    |   c   |       d        |        |                             |
    //    |    |       |                |        |                             |
    // 25 |----|-------|----------------|        |------------ ----------------|

    storage_offset_a[0] = 5 + rank * prob_domain[0];
    storage_offset_b[0] = 9 + rank * prob_domain[0];
    storage_offset_c[0] = 5 + rank * prob_domain[0];
    storage_offset_d[0] = 9 + rank * prob_domain[0];

    storage_offset_a[1] = 10 + rank * prob_domain[1];
    storage_offset_b[1] = 10 + rank * prob_domain[1];
    storage_offset_c[1] = 16 + rank * prob_domain[1];
    storage_offset_d[1] = 16 + rank * prob_domain[1];

    storage_size_a[0] = 4;
    storage_size_b[0] = 16;
    storage_size_c[0] = 4;
    storage_size_d[0] = 16;

    storage_size_a[1] = 6;
    storage_size_b[1] = 6;
    storage_size_c[1] = 9;
    storage_size_d[1] = 9;

    storage_domain[0] = (storage_size_a[0] + storage_size_b[0]) * size; // 20
    storage_domain[1] = (storage_size_a[1] + storage_size_c[1]) * size; // 15
    storage_domain[2] = 1;

    region_a.ndim = ndim;
    region_b.ndim = ndim;
    region_c.ndim = ndim;
    region_d.ndim = ndim;

    region_a.offset = storage_offset_a;
    region_b.offset = storage_offset_b;
    region_c.offset = storage_offset_c;
    region_d.offset = storage_offset_d;

    region_a.size = storage_size_a;
    region_b.size = storage_size_b;
    region_c.size = storage_size_c;
    region_d.size = storage_size_d;

    data_total_size = storage_domain[0] * storage_domain[1];
    data            = (char *)calloc(sizeof(char), data_total_size);
    data_2d         = (char **)calloc(sizeof(char *), storage_domain[1]);
    for (i = 0; i < storage_domain[1]; i++)
        data_2d[i] = data + i * storage_domain[0];

    data_a_2d = (char **)calloc(sizeof(char *), storage_size_a[1]);
    data_b_2d = (char **)calloc(sizeof(char *), storage_size_b[1]);
    data_c_2d = (char **)calloc(sizeof(char *), storage_size_c[1]);
    data_d_2d = (char **)calloc(sizeof(char *), storage_size_d[1]);

    for (i = 0; i < storage_size_a[1]; i++) {
        data_a_2d[i] = data_2d[i];
        for (j = 0; j < storage_size_a[0]; j++)
            data_a_2d[i][j] = (i + j) % 4 + 'A';
    }

    for (i = 0; i < storage_size_b[1]; i++) {
        data_b_2d[i] = data_2d[i] + storage_size_a[0];
        for (j = 0; j < storage_size_b[0]; j++)
            data_b_2d[i][j] = (i + j) % 12 + 'a' + 6;
    }

    for (i = 0; i < storage_size_c[1]; i++) {
        data_c_2d[i] = data_2d[i + storage_size_a[1]];
        for (j = 0; j < storage_size_c[0]; j++)
            data_c_2d[i][j] = (i + j) % 4 + 'a' + 14;
    }

    for (i = 0; i < storage_size_d[1]; i++) {
        data_d_2d[i] = data_2d[i + storage_size_a[1]] + storage_size_a[0];
        for (j = 0; j < storage_size_d[0]; j++)
            data_d_2d[i][j] = (i + j) % 5 + 'A' + 20;
    }

    PDC_Client_write_wait_notify(metadata, &region_a, data_a_2d);
    PDC_Client_write_wait_notify(metadata, &region_b, data_b_2d);
    PDC_Client_write_wait_notify(metadata, &region_c, data_c_2d);
    PDC_Client_write_wait_notify(metadata, &region_d, data_d_2d);

    read_offset[0]     = 8 + rank * prob_domain[0];
    read_offset[1]     = 14 + rank * prob_domain[1];
    read_size[0]       = 10;
    read_size[1]       = 4;
    read_region.ndim   = ndim;
    read_region.offset = read_offset;
    read_region.size   = read_size;

    read_data    = (char *)calloc(sizeof(char), read_size[0] * read_size[1] + 1);
    read_data_2d = (char **)calloc(sizeof(char *), storage_domain[1]);
    for (i = 0; i < read_size[1]; i++)
        read_data_2d[i] = read_data + i * read_size[0];

    PDC_Client_read_wait_notify(metadata, &read_region, read_data_2d);

    read_data[read_size[0] * read_size[1]] = 0;

    data_offset_real[0] = read_region.offset[0] - region_a.offset[0];
    data_offset_real[1] = read_region.offset[1] - region_a.offset[1];

    is_correct = data_verify(ndim, data_start, read_region.size, read_data_2d, data_offset_real, data_2d);
    if (is_correct != 1)
        printf("proc %d: verification failed!\n", rank);
    is_all_correct = is_correct;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&is_correct, &is_all_correct, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#endif

    if (rank == 0) {
        printf("2D data verfication ...");
        if (is_all_correct != size)
            printf("FAILED!\n");
        else
            printf("SUCCEED!\n");
    }

    free(data);
    free(data_2d);

    free(read_data);
    free(read_data_2d);

    return 1;
}

int
test3d(char *obj_name)
{
    uint32_t ndim = 3;

    pdcid_t                obj_id;
    pdc_metadata_t *       metadata;
    struct pdc_region_info region_a;
    struct pdc_region_info region_b;
    struct pdc_region_info region_c;
    struct pdc_region_info region_d;
    struct pdc_region_info region_e;
    struct pdc_region_info region_f;
    struct pdc_region_info region_g;
    struct pdc_region_info region_h;
    struct pdc_region_info read_region;
    uint64_t               storage_offset_a[3], storage_offset_b[3], storage_offset_c[3], storage_offset_d[3];
    uint64_t               storage_offset_e[3], storage_offset_f[3], storage_offset_g[3], storage_offset_h[3];
    uint64_t               storage_size_a[3], storage_size_b[3], storage_size_c[3], storage_size_d[3];
    uint64_t               storage_size_e[3], storage_size_f[3], storage_size_g[3], storage_size_h[3];

    uint64_t read_offset[3], read_size[3];
    uint64_t prob_domain[3], storage_domain[3];
    uint64_t data_start[3] = {0, 0, 0};
    uint64_t data_offset_real[3];
    uint64_t i, j, data_total_size;

    char ***data_3d, ***data_a_3d, ***data_b_3d, ***data_c_3d, ***data_d_3d, ***data_e_3d, ***data_f_3d,
        ***data_g_3d, ***data_h_3d, ***read_data_3d;
    char *read_data, *data;
    int   is_correct = 0, is_all_correct = 0;

    prob_domain[0] = 22 * size;
    prob_domain[1] = 21 * size;
    prob_domain[2] = 20 * size;

    obj_id = create_obj(obj_name, ndim, prob_domain);
    if (obj_id == 0) {
        printf("[%d]: Error creating an object [%s]\n", rank, obj_name);
        exit(-1);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL) {
        printf("Error getting metadata from server!\n");
        exit(-1);
    }

    /*   Storage Regions   start       size
     *              a     3, 4, 5     4, 5, 6
     *              b     7, 4, 5    15, 5, 6
     *              c     3, 9, 5     4,12, 6
     *              d     7, 9, 5    15,12, 6
     *              e     3, 4,11     4, 5, 9
     *              f     7, 4,11    15, 5, 9
     *              g     3, 9,11     4,12, 9
     *              h     7, 9,11    15,12, 9
     *
     *                   z:20______________________________________
     *                      /                                    /|
     *                    /                                    /  |
     *            z:11_ /                                    /    |
     *                /|                                   /      /
     *              /  |                                 /      / |
     *            /    |                               /      /   |
     *          /      |x:7                    x:22  /      /     |
     *    z:5 /--------|---------------------------/      /       |
     *        |        |___________________________|____/   f   / |
     *        |        / a   /        b            |  / |     /   |
     *    y:4-|      /_____/_______________________|/   |   /     |
     *        |     |     |                        | b  | /       |
     *        |     |  a  |           b            |    /         |
     *        |     |     |                        |  / |         |
     *    y:9-|     -------------------------------|/   |   h     |
     *        |     |     |                        |    |       /
     *        |     |     |                        |    |     /
     *        |     |     |                        | d  |   /
     *        |     |  c  |           d            |    | /
     *        |     |     |                        |    /
     *        |     |     |                        |  /
     *    y:21--------------------------------------/
     *            x:3    x:7                     x:22
     */
    storage_offset_a[0] = 3 + rank * prob_domain[0];
    storage_offset_a[1] = 4 + rank * prob_domain[1];
    storage_offset_a[2] = 5 + rank * prob_domain[2];
    storage_size_a[0]   = 4;
    storage_size_a[1]   = 5;
    storage_size_a[2]   = 6;

    storage_offset_b[0] = 7 + rank * prob_domain[0];
    storage_offset_b[1] = 4 + rank * prob_domain[1];
    storage_offset_b[2] = 5 + rank * prob_domain[2];
    storage_size_b[0]   = 15;
    storage_size_b[1]   = 5;
    storage_size_b[2]   = 6;

    storage_offset_c[0] = 3 + rank * prob_domain[0];
    storage_offset_c[1] = 9 + rank * prob_domain[1];
    storage_offset_c[2] = 5 + rank * prob_domain[2];
    storage_size_c[0]   = 4;
    storage_size_c[1]   = 12;
    storage_size_c[2]   = 6;

    storage_offset_d[0] = 7 + rank * prob_domain[0];
    storage_offset_d[1] = 9 + rank * prob_domain[1];
    storage_offset_d[2] = 5 + rank * prob_domain[2];
    storage_size_d[0]   = 15;
    storage_size_d[1]   = 12;
    storage_size_d[2]   = 6;

    storage_offset_e[0] = 3 + rank * prob_domain[0];
    storage_offset_e[1] = 4 + rank * prob_domain[1];
    storage_offset_e[2] = 11 + rank * prob_domain[2];
    storage_size_e[0]   = 4;
    storage_size_e[1]   = 5;
    storage_size_e[2]   = 9;

    storage_offset_f[0] = 7 + rank * prob_domain[0];
    storage_offset_f[1] = 4 + rank * prob_domain[1];
    storage_offset_f[2] = 11 + rank * prob_domain[2];
    storage_size_f[0]   = 15;
    storage_size_f[1]   = 5;
    storage_size_f[2]   = 9;

    storage_offset_g[0] = 3 + rank * prob_domain[0];
    storage_offset_g[1] = 9 + rank * prob_domain[1];
    storage_offset_g[2] = 11 + rank * prob_domain[2];
    storage_size_g[0]   = 4;
    storage_size_g[1]   = 12;
    storage_size_g[2]   = 9;

    storage_offset_h[0] = 7 + rank * prob_domain[0];
    storage_offset_h[1] = 9 + rank * prob_domain[1];
    storage_offset_h[2] = 11 + rank * prob_domain[2];
    storage_size_h[0]   = 15;
    storage_size_h[1]   = 12;
    storage_size_h[2]   = 9;

    region_a.ndim = ndim;
    region_b.ndim = ndim;
    region_c.ndim = ndim;
    region_d.ndim = ndim;
    region_e.ndim = ndim;
    region_f.ndim = ndim;
    region_g.ndim = ndim;
    region_h.ndim = ndim;

    region_a.offset = storage_offset_a;
    region_b.offset = storage_offset_b;
    region_c.offset = storage_offset_c;
    region_d.offset = storage_offset_d;
    region_e.offset = storage_offset_e;
    region_f.offset = storage_offset_f;
    region_g.offset = storage_offset_g;
    region_h.offset = storage_offset_h;

    region_a.size = storage_size_a;
    region_b.size = storage_size_b;
    region_c.size = storage_size_c;
    region_d.size = storage_size_d;
    region_e.size = storage_size_e;
    region_f.size = storage_size_f;
    region_g.size = storage_size_g;
    region_h.size = storage_size_h;

    // Storage domain
    storage_domain[0] = (storage_size_a[0] + storage_size_b[0]) * size; // 19
    storage_domain[1] = (storage_size_a[1] + storage_size_c[1]) * size; // 17
    storage_domain[2] = (storage_size_a[2] + storage_size_e[2]) * size; // 15

    data_total_size = storage_domain[0] * storage_domain[1] * storage_domain[2];

    data = (char *)calloc(sizeof(char), data_total_size);

    for (i = 0; i < data_total_size; i++) {
        data[i] = i % 26 + 'A';
    }

    data_3d = (char ***)calloc(sizeof(char **), storage_domain[2]);

    for (j = 0; j < storage_domain[2]; j++) {
        data_3d[j] = (char **)calloc(sizeof(char *), storage_domain[1]);
        for (i = 0; i < storage_domain[1]; i++) {
            data_3d[j][i] = data + i * storage_domain[0] + j * storage_domain[0] * storage_domain[1];
        }
    }

    data_a_3d = (char ***)calloc(sizeof(char **), storage_size_a[1] * storage_size_a[2]);
    data_b_3d = (char ***)calloc(sizeof(char **), storage_size_b[1] * storage_size_b[2]);
    data_c_3d = (char ***)calloc(sizeof(char **), storage_size_c[1] * storage_size_c[2]);
    data_d_3d = (char ***)calloc(sizeof(char **), storage_size_d[1] * storage_size_d[2]);
    data_e_3d = (char ***)calloc(sizeof(char **), storage_size_e[1] * storage_size_e[2]);
    data_f_3d = (char ***)calloc(sizeof(char **), storage_size_f[1] * storage_size_f[2]);
    data_g_3d = (char ***)calloc(sizeof(char **), storage_size_g[1] * storage_size_g[2]);
    data_h_3d = (char ***)calloc(sizeof(char **), storage_size_h[1] * storage_size_h[2]);

    for (i = 0; i < storage_size_a[2]; i++) {
        data_a_3d[i] = (char **)calloc(sizeof(char *), storage_size_a[1]);
        for (j = 0; j < storage_size_a[1]; j++) {
            data_a_3d[i][j] = data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1];
        }
    }

    for (i = 0; i < storage_size_b[2]; i++) {
        data_b_3d[i] = (char **)calloc(sizeof(char *), storage_size_b[1]);
        for (j = 0; j < storage_size_b[1]; j++) {
            data_b_3d[i][j] =
                data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1] + storage_size_a[0];
        }
    }

    for (i = 0; i < storage_size_c[2]; i++) {
        data_c_3d[i] = (char **)calloc(sizeof(char *), storage_size_c[1]);
        for (j = 0; j < storage_size_c[1]; j++) {
            data_c_3d[i][j] = data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1] +
                              storage_size_a[1] * storage_domain[0];
        }
    }

    for (i = 0; i < storage_size_d[2]; i++) {
        data_d_3d[i] = (char **)calloc(sizeof(char *), storage_size_d[1]);
        for (j = 0; j < storage_size_d[1]; j++) {
            data_d_3d[i][j] = data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1] +
                              storage_size_a[0] + storage_size_a[1] * storage_domain[0];
        }
    }

    for (i = 0; i < storage_size_e[2]; i++) {
        data_e_3d[i] = (char **)calloc(sizeof(char *), storage_size_e[1]);
        for (j = 0; j < storage_size_e[1]; j++) {
            data_e_3d[i][j] = data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1] +
                              storage_size_a[2] * storage_domain[0] * storage_domain[1];
        }
    }

    for (i = 0; i < storage_size_f[2]; i++) {
        data_f_3d[i] = (char **)calloc(sizeof(char *), storage_size_f[1]);
        for (j = 0; j < storage_size_f[1]; j++) {
            data_f_3d[i][j] = data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1] +
                              storage_size_a[2] * storage_domain[0] * storage_domain[1] + storage_size_a[0];
        }
    }

    for (i = 0; i < storage_size_g[2]; i++) {
        data_g_3d[i] = (char **)calloc(sizeof(char *), storage_size_g[1]);
        for (j = 0; j < storage_size_g[1]; j++) {
            data_g_3d[i][j] = data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1] +
                              storage_size_a[2] * storage_domain[0] * storage_domain[1] +
                              storage_size_a[1] * storage_domain[0];
        }
    }

    for (i = 0; i < storage_size_h[2]; i++) {
        data_h_3d[i] = (char **)calloc(sizeof(char *), storage_size_h[1]);
        for (j = 0; j < storage_size_h[1]; j++) {
            data_h_3d[i][j] = data + j * storage_domain[0] + i * storage_domain[0] * storage_domain[1] +
                              storage_size_a[2] * storage_domain[0] * storage_domain[1] +
                              storage_size_a[1] * storage_domain[0] + storage_size_a[0];
        }
    }

    PDC_Client_write_wait_notify(metadata, &region_a, data_a_3d);
    PDC_Client_write_wait_notify(metadata, &region_b, data_b_3d);
    PDC_Client_write_wait_notify(metadata, &region_c, data_c_3d);
    PDC_Client_write_wait_notify(metadata, &region_d, data_d_3d);
    PDC_Client_write_wait_notify(metadata, &region_e, data_e_3d);
    PDC_Client_write_wait_notify(metadata, &region_f, data_f_3d);
    PDC_Client_write_wait_notify(metadata, &region_g, data_g_3d);
    PDC_Client_write_wait_notify(metadata, &region_h, data_h_3d);

    // Now data selection
    read_offset[0] = 5 + rank * prob_domain[0];
    read_offset[1] = 7 + rank * prob_domain[1];
    read_offset[2] = 9 + rank * prob_domain[2];
    read_size[0]   = 6;
    read_size[1]   = 5;
    read_size[2]   = 4;

    read_region.ndim   = ndim;
    read_region.offset = read_offset;
    read_region.size   = read_size;

    read_data =
        (char *)calloc(sizeof(char), read_region.size[0] * read_region.size[1] * read_region.size[2] + 1);

    read_data_3d = (char ***)calloc(sizeof(char **), read_region.size[0] * read_region.size[1]);

    for (j = 0; j < read_region.size[2]; j++) {
        read_data_3d[j] = (char **)calloc(sizeof(char *), read_region.size[1]);
        for (i = 0; i < read_region.size[1]; i++) {
            read_data_3d[j][i] =
                read_data + i * read_region.size[0] + j * read_region.size[0] * read_region.size[1];
        }
    }

    PDC_Client_read_wait_notify(metadata, &read_region, read_data_3d);
    read_data[read_size[0] * read_size[1] * read_size[2]] = 0;

    data_offset_real[0] = read_region.offset[0] - region_a.offset[0];
    data_offset_real[1] = read_region.offset[1] - region_a.offset[1];
    data_offset_real[2] = read_region.offset[2] - region_a.offset[2];

    is_correct = data_verify(ndim, data_start, read_region.size, read_data_3d, data_offset_real, data_3d);
    if (is_correct != 1)
        printf("proc %d: verification failed!\n", rank);

    is_all_correct = is_correct;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&is_correct, &is_all_correct, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#endif

    if (rank == 0) {
        printf("3D data verfication ...");
        if (is_all_correct != size)
            printf("FAILED!\n");
        else
            printf("SUCCEED!\n");
    }

    free(data);
    for (j = 0; j < storage_domain[2]; j++)
        free(data_3d[j]);
    free(data_3d);

    for (i = 0; i < storage_size_a[2]; i++)
        free(data_a_3d[i]);

    for (i = 0; i < storage_size_b[2]; i++)
        free(data_b_3d[i]);

    for (i = 0; i < storage_size_c[2]; i++)
        free(data_c_3d[i]);

    for (i = 0; i < storage_size_d[2]; i++)
        free(data_d_3d[i]);

    for (i = 0; i < storage_size_e[2]; i++)
        free(data_e_3d[i]);

    for (i = 0; i < storage_size_f[2]; i++)
        free(data_f_3d[i]);

    for (i = 0; i < storage_size_g[2]; i++)
        free(data_g_3d[i]);

    for (i = 0; i < storage_size_h[2]; i++)
        free(data_h_3d[i]);

    free(data_a_3d);
    free(data_b_3d);
    free(data_c_3d);
    free(data_d_3d);
    free(data_e_3d);
    free(data_f_3d);
    free(data_g_3d);
    free(data_h_3d);

    free(read_data);
    for (j = 0; j < read_region.size[2]; j++)
        free(read_data_3d[j]);

    return 1;
}

int
main(int argc, char **argv)
{
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    test1d("test_obj_1d");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    test2d("test_obj_2d");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    test3d("test_obj_3d");

    if (rank == 0)
        printf("\n\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container\n");

    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
