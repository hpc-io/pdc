#include <stdio.h>
#include <stdlib.h>
#include <float.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>

#include <hdf5.h>
#include "pdc.h"

#define NVAR 4
/* #define NVAR 7 */

void
print_usage(char *name)
{
    printf("Usage: %s /path/to/file region_size_MB\n", name);
}

int
main(int argc, char *argv[])
{
    int      my_rank = 0, num_procs = 1, i, ndim, region_MB = 0;
    char *   file_name, *group_name;
    char *   dset_names[7] = {"Energy", "x", "y", "z", "Ux", "Uy", "Uz"};
    void *   data;
    hid_t    file_id, group_id, dset_ids[NVAR], filespace, memspace, fapl;
    hsize_t  dims[1], my_elem_off, my_nelem, region_offset, region_size;
    hsize_t  elem_offset, elem_count, total_elem, my_nregion, total_region, j;
    uint64_t pdc_dims[1] = {1};
    herr_t   hg_ret;

    pdcid_t                pdc_id, cont_prop, cont_id, obj_ids[NVAR], obj_prop;
    perr_t                 ret;
    pdc_metadata_t *       obj_meta;
    struct PDC_region_info obj_region;

#ifdef ENABLE_MPI
    MPI_Info info = MPI_INFO_NULL;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
#endif

    if (argc < 3) {
        print_usage(argv[0]);
        return 0;
    }

    file_name   = argv[1];
    region_MB   = atoi(argv[2]);
    region_size = region_MB * 1048576;

    if (my_rank == 0) {
        printf("Region size = %d MB, reading from file [%s], \n", region_MB, file_name);
    }

    fapl = H5Pcreate(H5P_FILE_ACCESS);

#ifdef ENABLE_MPI
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
#endif

    file_id = H5Fopen(file_name, H5F_ACC_RDONLY, fapl);
    if (file_id < 0) {
        printf("Error opening file [%s]!\n", file_name);
        goto done;
    }

    group_name = "Step#0";
    group_id   = H5Gopen(file_id, group_name, H5P_DEFAULT);
    if (group_id < 0) {
        printf("Error opening group [%s]!\n", group_name);
        goto done;
    }

    for (i = 0; i < NVAR; i++) {
        dset_ids[i] = H5Dopen(group_id, dset_names[i], H5P_DEFAULT);
        if (dset_ids[i] < 0) {
            printf("Error opening dataset [%s]!\n", dset_names[i]);
            goto done;
        }
    }

    filespace = H5Dget_space(dset_ids[0]);
    ndim      = H5Sget_simple_extent_ndims(filespace);
    H5Sget_simple_extent_dims(filespace, dims, NULL);

    total_elem   = dims[0];
    total_region = ceil(sizeof(float) * (1.0 * dims[0] / region_size));
    my_nregion   = ceil(1.0 * total_region / num_procs);
    my_nelem     = my_nregion * (region_size / sizeof(float));
    if (my_rank == num_procs - 1)
        my_nregion = total_region - my_nregion * (num_procs - 1);

    my_elem_off = my_rank * my_nelem;
    data        = (float *)malloc(region_size);

    // PDC
    pdc_id    = PDC_init("pdc");
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    cont_id   = PDCcont_create("VPIC_cont", cont_prop);
    obj_prop  = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop, ndim, pdc_dims);
    PDCprop_set_obj_type(obj_prop, PDC_FLOAT);
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_app_name(obj_prop, "VPIC");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    obj_region.ndim   = ndim;
    obj_region.offset = (uint64_t *)calloc(ndim, sizeof(uint64_t));
    obj_region.size   = (uint64_t *)calloc(ndim, sizeof(uint64_t));

    for (i = 0; i < NVAR; i++) {

        if (my_rank == 0) {
            obj_ids[i] = PDCobj_create(cont_id, dset_names[i], obj_prop);
            if (obj_ids[i] <= 0) {
                printf("Error getting an object %s from server, exit...\n", dset_names[i]);
                goto done;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        ret = PDC_Client_query_metadata_name_timestep_agg(dset_names[i], 0, &obj_meta);
#else
        ret = PDC_Client_query_metadata_name_timestep(dset_names[i], 0, &obj_meta);
#endif
        if (ret != SUCCEED || obj_meta == NULL || obj_meta->obj_id == 0) {
            printf("Error with metadata!\n");
            exit(-1);
        }

        elem_count = region_size / sizeof(float);
        for (j = 0; j < my_nregion; j++) {
            elem_offset = my_elem_off + j * elem_count;
            if (my_rank == num_procs - 1 && j == my_nregion - 1 && total_elem % elem_count != 0) {
                elem_count = total_elem % elem_count;
            }
            memspace = H5Screate_simple(1, &elem_count, NULL);
            H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &elem_offset, NULL, &elem_count, NULL);
            if (elem_offset + elem_count > dims[0]) {
                printf("%d ERROR - off %llu count %llu\n", my_rank, elem_offset, elem_count);
            }
            hg_ret = H5Dread(dset_ids[i], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, data);
            H5Sclose(memspace);

            obj_region.offset[0] = elem_offset * sizeof(float);
            obj_region.size[0]   = elem_count * sizeof(float);

            /* printf("%d HDF5 off %llu count %llu, PDC off %lu count %lu\n", */
            /*         my_rank, elem_offset, elem_count, obj_region.offset[0], obj_region.size[0]); */

            ret = PDC_Client_write(obj_meta, &obj_region, data);
            if (ret != SUCCEED) {
                printf("Error with PDC_Client_write!\n");
                exit(-1);
            }

            if ((my_rank == 0 || my_rank == num_procs - 1) && j > 0 && j % (my_nregion / 10) == 0)
                printf("Rank %d -  obj [%s] Imported %lld/%lld regions\n", my_rank, dset_names[i], j,
                       my_nregion);

        } // End for j

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        if (my_rank == 0)
            printf("\n\nFinished import object %s\n\n", dset_names[i]);
        fflush(stdout);
    } // End for i

    H5Pclose(fapl);
    H5Sclose(filespace);
    for (i = 0; i < NVAR; i++)
        H5Dclose(dset_ids[i]);
    H5Gclose(group_id);
    H5Fclose(file_id);

    PDCprop_close(obj_prop);
    PDCcont_close(cont_id);
    PDCprop_close(cont_prop);
    PDC_close(pdc_id);

done:
    if (data != NULL)
        free(data);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
