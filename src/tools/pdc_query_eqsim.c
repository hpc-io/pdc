#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "hdf5.h"
#include "pdc.h"
#include "pdc_client_server_common.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop, obj, local_reg, remote_reg, transfer_req, *transfer_batch;

    hid_t  file, grp, dset, fapl, dxpl, dspace, mspace;
    herr_t status;

    int i, j, count, total_count, rank, nproc, ssi_downsample, rec_downsample, batchsize, iter, opensees_size;
    int start_x[4096], start_y[4096];
    hsize_t  offset[4], size[4], local_offset[4], local_size[4];
    hsize_t  dims[4] = {4634, 19201, 12801, 1}, chunk_size[4] = {400, 600, 400, 1};
    uint64_t pdc_dims[3], pdc_offset[3], pdc_size[3], pdc_local_offset[3], pdc_local_size[3];
    uint32_t value_size;
    // 12x, 32x, 32x
    char *  fname, *dname = "vel_0 ijk layout", tag_name[128];
    double *data = NULL, t0, t1, t2, data_max, data_min, *ssi_data = NULL, *rec_data = NULL,
           *opensees_data = NULL, tag_value[4];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);
#endif

    fname = argv[1];

    fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);

    dxpl = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxpl, H5FD_MPIO_COLLECTIVE);

    file = H5Fopen(fname, H5F_ACC_RDONLY, fapl);
    if (file < 0)
        fprintf(stderr, "Failed to open file [%s]\n", fname);

    dset   = H5Dopen(file, dname, H5P_DEFAULT);
    dspace = H5Dget_space(dset);
    H5Sget_simple_extent_dims(dspace, dims, NULL);

    // Assign chunks to each rank
    count = 0;
    for (i = 0; i < dims[1] / chunk_size[1]; i++) {
        for (j = 0; j < dims[2] / chunk_size[2]; j++) {
            start_x[count] = i;
            start_y[count] = j;
            count++;
        }
    }

    offset[0] = 0;
    offset[1] = chunk_size[1] * start_x[rank];
    offset[2] = chunk_size[2] * start_y[rank];
    offset[3] = 0;

    /* size[0] = chunk_size[0]; */
    size[0] = dims[0];
    size[1] = chunk_size[1];
    size[2] = chunk_size[2];
    size[3] = 1;

    H5Sselect_hyperslab(dspace, H5S_SELECT_SET, offset, NULL, size, NULL);

    local_offset[0] = 0;
    local_offset[1] = 0;
    local_offset[2] = 0;
    local_offset[3] = 0;

    /* local_size[0] = chunk_size[0]; */
    local_size[0] = dims[0];
    local_size[1] = chunk_size[1];
    local_size[2] = chunk_size[2];
    local_size[3] = 1;

    mspace = H5Screate_simple(4, local_size, NULL);

    data = (double *)malloc(sizeof(double) * local_size[0] * local_size[1] * local_size[2]);

    if (nproc <= 16)
        fprintf(stderr, "Rank %d: offset %llu, %llu, %llu size %llu, %llu, %llu\n", rank, offset[0],
                offset[1], offset[2], size[0], size[1], size[2]);

#ifdef ENABLE_MPI
    t0 = MPI_Wtime();
#endif

    H5Dread(dset, H5T_NATIVE_DOUBLE, mspace, dspace, dxpl, data);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t1 = MPI_Wtime();
    if (rank == 0)
        fprintf(stderr, "Read from HDF5 took %.4lf\n", t1 - t0);
#endif

    H5Sclose(mspace);
    H5Sclose(dspace);
    H5Pclose(fapl);
    H5Pclose(dxpl);
    H5Dclose(dset);
    H5Fclose(file);
    H5close();

    // End of HDF5 operations

    for (i = 0; i < 3; i++) {
        pdc_dims[i]         = PDC_SIZE_UNLIMITED;
        pdc_offset[i]       = (uint64_t)offset[i];
        pdc_size[i]         = (uint64_t)size[i];
        pdc_local_offset[i] = (uint64_t)local_offset[i];
        pdc_local_size[i]   = (uint64_t)local_size[i];
    }

    pdc = PDCinit("pdc");

    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    cont      = PDCcont_create("ssioutput", cont_prop);
    obj_prop  = PDCprop_create(PDC_OBJ_CREATE, pdc);

    PDCprop_set_obj_dims(obj_prop, 3, pdc_dims);
    PDCprop_set_obj_type(obj_prop, PDC_DOUBLE);
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_app_name(obj_prop, "EQSIM");
    /* PDCprop_set_obj_transfer_region_type(obj_prop, PDC_REGION_LOCAL); */

    obj = PDCobj_create_mpi(cont, "run1", obj_prop, 0, MPI_COMM_WORLD);
    /* obj = PDCobj_create(cont, "run1", obj_prop); */
    if (obj <= 0)
        fprintf(stderr, "Fail to create object @ line  %d!\n", __LINE__);

    remote_reg = PDCregion_create(3, pdc_offset, pdc_size);

    local_reg = PDCregion_create(3, pdc_local_offset, pdc_local_size);

    // Create a tag for each region, using its x/y offset as name
    sprintf(tag_name, "%llu-%llu\n", pdc_offset[1], pdc_offset[2]);
    // Dummy value
    tag_value[0] = rank * 0.0;
    tag_value[1] = rank * 1.0;
    tag_value[2] = rank * 10.0;
    tag_value[3] = rank * 11.0;
    value_size   = 4 * sizeof(double);

    if (PDCobj_put_tag(obj, tag_name, tag_value, PDC_DOUBLE, value_size) < 0)
        fprintf(stderr, "Rank %d fail to put tag @ line  %d!\n", rank, __LINE__);

    // Query the created object
    pdc_metadata_t *metadata;
    uint32_t        metadata_server_id;
    PDC_Client_query_metadata_name_timestep("run1", 0, &metadata, &metadata_server_id);
    if (metadata == NULL || metadata->obj_id == 0) {
        printf("Error with metadata!\n");
    }

    int                    ndim = 3;
    struct pdc_region_info region;
    region.ndim   = ndim;
    region.offset = pdc_offset;
    region.size   = pdc_size;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t0 = MPI_Wtime();
#endif

    PDC_Client_write(metadata, &region, data);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t1 = MPI_Wtime();
    if (rank == 0)
        fprintf(stderr, "Write data to server took %.4lf\n", t1 - t0);
#endif

    // Construct query constraints
    double       query_val = 0.9;
    pdc_query_t *q         = PDCquery_create(obj, PDC_GT, PDC_INT, &query_val);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t0 = MPI_Wtime();
#endif

    pdc_selection_t sel;

    if (rank == 0)
        PDCquery_get_selection(q, &sel);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t1 = MPI_Wtime();
    if (rank == 0)
        fprintf(stderr, "Query data took %.4lf\n", t1 - t0);
#endif

    /* PDCselection_print(&sel); */

    PDCquery_free_all(q);
    PDCselection_free(&sel);

    free(data);

    PDCregion_close(remote_reg);
    PDCregion_close(local_reg);

    if (PDCobj_close(obj) < 0)
        fprintf(stderr, "fail to close object\n");

    if (PDCcont_close(cont) < 0)
        fprintf(stderr, "fail to close container c1\n");

    if (PDCprop_close(obj_prop) < 0)
        fprintf(stderr, "Fail to close property @ line %d\n", __LINE__);

    if (PDCprop_close(cont_prop) < 0)
        fprintf(stderr, "Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc) < 0)
        fprintf(stderr, "fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
