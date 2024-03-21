#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "mpi.h"

#include "hdf5.h"

int
main(int argc, char **argv)
{

    hid_t  file, grp, dset, fapl, dxpl, dspace, mspace;
    herr_t status;

    int i, j, r, round = 1, count, total_count, rank = 0, nproc = 1, ssi_downsample, rec_downsample,
                 batchsize, iter, opensees_size;
    int     start_x[4096], start_y[4096];
    hsize_t offset[4], size[4], local_offset[4], local_size[4], stride[4];
    hsize_t dims[4] = {4634, 19201, 12801, 1}, chunk_size[4] = {400, 600, 400, 1};
    // 12x, 32x, 32x
    char *  fname, *dname = "vel_0 ijk layout";
    double *data = NULL, t0, t1, t2, data_max, data_min, *ssi_data = NULL, *rec_data = NULL,
           *opensees_data = NULL;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);

    fname = argv[1];
    if (argc > 2)
        round = atoi(argv[2]);

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

    /* //=============PATTERN 1=============== */
    /* // Read entire chunks */
    /* offset[0] = 0; */
    /* offset[1] = chunk_size[1] * start_x[rank]; */
    /* offset[2] = chunk_size[2] * start_y[rank]; */
    /* offset[3] = 0; */

    /* /1* size[0] = chunk_size[0]; *1/ */
    /* size[0] = dims[0]; */
    /* size[1] = chunk_size[1]; */
    /* size[2] = chunk_size[2]; */
    /* size[3] = 1; */

    /* H5Sselect_hyperslab(dspace, H5S_SELECT_SET, offset, NULL, size, NULL); */

    /* local_offset[0] = 0; */
    /* local_offset[1] = 0; */
    /* local_offset[2] = 0; */
    /* local_offset[3] = 0; */

    /* /1* local_size[0] = chunk_size[0]; *1/ */
    /* local_size[0] = dims[0]; */
    /* local_size[1] = chunk_size[1]; */
    /* local_size[2] = chunk_size[2]; */
    /* local_size[3] = 1; */

    /* mspace = H5Screate_simple(4, local_size, NULL); */

    /* data = (double *)malloc(sizeof(double) * local_size[0] * local_size[1] * local_size[2]); */

    /* if (nproc <= 16) */
    /*     fprintf(stderr, "Rank %d: offset %llu, %llu, %llu size %llu, %llu, %llu\n", rank, offset[0], */
    /*             offset[1], offset[2], size[0], size[1], size[2]); */

    /* t0 = MPI_Wtime(); */

    /* H5Dread(dset, H5T_NATIVE_DOUBLE, mspace, dspace, dxpl, data); */

    /* MPI_Barrier(MPI_COMM_WORLD); */
    /* t1 = MPI_Wtime(); */
    /* if (rank == 0) */
    /*     fprintf(stderr, "Read from HDF5 took %.4lf\n", t1 - t0); */

    /* H5Sclose(mspace); */

    //=============PATTERN 2===============
    // OpenSees access pattern: 1 rank read subregion 200x200m (32 grids)
    opensees_size = 32; // 32 * 6.25m = 200m
    local_size[0] = dims[0];
    local_size[1] = opensees_size;
    local_size[2] = opensees_size;
    local_size[3] = 1;

    offset[0] = 0;
    offset[1] = start_x[rank] * chunk_size[1];
    offset[2] = start_y[rank] * chunk_size[2];
    offset[3] = 0;
    size[0]   = dims[0];
    size[1]   = opensees_size;
    size[2]   = opensees_size;
    size[3]   = 1;

    mspace = H5Screate_simple(4, local_size, NULL);

    if (nproc <= 16)
        fprintf(stderr, "Rank %d: offset %llu, %llu, %llu size %llu, %llu, %llu\n", rank, offset[0],
                offset[1], offset[2], size[0], size[1], size[2]);

    if (rank == 0)
        opensees_data = (double *)malloc(sizeof(double) * dims[0] * opensees_size * opensees_size);

    for (r = 0; r < round; r++) {

        if (rank == 0)
            H5Sselect_hyperslab(dspace, H5S_SELECT_SET, offset, NULL, size, NULL);
        else {
            H5Sselect_none(mspace);
            H5Sselect_none(dspace);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();

        H5Dread(dset, H5T_NATIVE_DOUBLE, mspace, dspace, dxpl, opensees_data);

        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: rank 0 read OpenSees 200x200m data took %.6lf\n", r, t1 - t0);

    } // End for round

    H5Sclose(mspace);
    free(opensees_data);

    //=============PATTERN 3===============
    // Generating movie: all rank downsample in space to 156.25x156.25m (downsample factor 25)
    ssi_downsample = 25;
    local_size[0]  = dims[0];
    local_size[1]  = chunk_size[1] / ssi_downsample;
    local_size[2]  = chunk_size[2] / ssi_downsample;
    local_size[3]  = 1;

    offset[0] = 0;
    offset[1] = start_x[rank] * chunk_size[1];
    offset[2] = start_y[rank] * chunk_size[2];
    offset[3] = 0;
    size[0]   = dims[0];
    size[1]   = chunk_size[1] / ssi_downsample;
    size[2]   = chunk_size[2] / ssi_downsample;
    size[3]   = 1;
    stride[0] = 1;
    stride[1] = ssi_downsample;
    stride[2] = ssi_downsample;
    stride[3] = 1;

    mspace = H5Screate_simple(4, local_size, NULL);

    batchsize = chunk_size[1] * chunk_size[2] / ssi_downsample / ssi_downsample;
    ssi_data  = (double *)malloc(sizeof(double) * dims[0] * batchsize);

    for (r = 0; r < round; r++) {
        H5Sselect_hyperslab(dspace, H5S_SELECT_SET, offset, stride, size, NULL);

        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();

        H5Dread(dset, H5T_NATIVE_DOUBLE, mspace, dspace, dxpl, ssi_data);

        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: all ranks read ssi_downsample data took %.6lf\n", r, t1 - t0);

    } // End for round

    H5Sclose(mspace);
    free(ssi_data);

    //=============PATTERN 4===============
    // Building response: all rank downsample in space to every 1250m (downsample factor 200)
    rec_downsample = 200;
    local_size[0]  = dims[0];
    local_size[1]  = chunk_size[1] / rec_downsample;
    local_size[2]  = chunk_size[2] / rec_downsample;
    local_size[3]  = 1;

    offset[0] = 0;
    offset[1] = start_x[rank] * chunk_size[1];
    offset[2] = start_y[rank] * chunk_size[2];
    offset[3] = 0;
    size[0]   = dims[0];
    size[1]   = chunk_size[1] / rec_downsample;
    size[2]   = chunk_size[2] / rec_downsample;
    size[3]   = 1;
    stride[0] = 1;
    stride[1] = rec_downsample;
    stride[2] = rec_downsample;
    stride[3] = 1;

    mspace = H5Screate_simple(4, local_size, NULL);

    batchsize = chunk_size[1] * chunk_size[2] / rec_downsample / rec_downsample;
    rec_data  = (double *)malloc(sizeof(double) * dims[0] * batchsize);

    for (r = 0; r < round; r++) {
        H5Sselect_hyperslab(dspace, H5S_SELECT_SET, offset, stride, size, NULL);

        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();

        H5Dread(dset, H5T_NATIVE_DOUBLE, mspace, dspace, dxpl, rec_data);

        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: all ranks read rec_downsample data took %.6lf\n", r, t1 - t0);

    } // End for round

    H5Sclose(mspace);

    //=============PATTERN 5===============
    // Single rank singele time history access
    local_size[0] = dims[0];
    local_size[1] = 1;
    local_size[2] = 1;
    local_size[3] = 1;

    offset[0] = 0;
    offset[1] = start_x[rank] * chunk_size[1];
    offset[2] = start_y[rank] * chunk_size[2];
    offset[3] = 0;
    size[0]   = dims[0];
    size[1]   = 1;
    size[2]   = 1;
    size[3]   = 1;

    mspace = H5Screate_simple(4, local_size, NULL);

    rec_data = (double *)malloc(sizeof(double) * dims[0] * batchsize);

    for (r = 0; r < round; r++) {
        if (rank == 0)
            H5Sselect_hyperslab(dspace, H5S_SELECT_SET, offset, NULL, size, NULL);
        else {
            H5Sselect_none(mspace);
            H5Sselect_none(dspace);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();

        H5Dread(dset, H5T_NATIVE_DOUBLE, mspace, dspace, dxpl, rec_data);

        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: rank 0 read 1 time-history took %.6lf\n", r, t1 - t0);

    } // End for round

    H5Sclose(mspace);
    free(rec_data);

    H5Sclose(dspace);

    H5Pclose(fapl);
    H5Pclose(dxpl);
    H5Dclose(dset);
    H5Fclose(file);
    H5close();

    MPI_Finalize();
    return 0;
}
