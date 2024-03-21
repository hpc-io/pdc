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
    // 12x, 32x, 32x
    char *  fname, *dname = "vel_0 ijk layout";
    double *data = NULL, t0, t1, t2, data_max, data_min, *ssi_data = NULL, *rec_data = NULL,
           *opensees_data = NULL;

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
    PDCprop_set_obj_transfer_region_type(obj_prop, PDC_REGION_LOCAL);

    obj = PDCobj_create_mpi(cont, "run1", obj_prop, 0, MPI_COMM_WORLD);
    /* obj = PDCobj_create(cont, "run1", obj_prop); */
    if (obj <= 0)
        fprintf(stderr, "Fail to create object @ line  %d!\n", __LINE__);

    remote_reg = PDCregion_create(3, pdc_offset, pdc_size);

    local_reg = PDCregion_create(3, pdc_local_offset, pdc_local_size);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t0 = MPI_Wtime();
#endif

    transfer_req = PDCregion_transfer_create(data, PDC_WRITE, obj, local_reg, remote_reg);
    PDCregion_transfer_start(transfer_req);
    PDCregion_transfer_wait(transfer_req);
    PDCregion_transfer_close(transfer_req);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t1 = MPI_Wtime();
    if (rank == 0)
        fprintf(stderr, "Write data to server took %.4lf\n", t1 - t0);
#endif

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
