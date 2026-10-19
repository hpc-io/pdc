/**
 * Parallel HDF5 posthoc benchmark, phase 1 of 2: write the three
 * vector-component datasets (vx, vy, vz) into a fresh file and exit. See
 * hdf5_bench_posthoc_analyze.c for phase 2 -- the two are run as
 * separate srun steps (separate job launches) so the pair models a real
 * posthoc workflow instead of one continuous process, matching the PDC
 * side's bench_write_components / bench_posthoc_analyze split.
 *
 * Like the PDC-side benchmarks, writes N_TIMESTEPS distinct sets of
 * vx/vy/vz, one HDF5 dataset per variable per timestep ("vx_0", "vx_1",
 * ...), since HDF5 datasets are named entities within one file rather
 * than PDC's separately time-stepped objects.
 *
 * Usage: hdf5_bench_write <n_elem_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,write_s,close_s
 *
 * close_s times H5Fclose(), a one-time job cost repeated on every row for
 * CSV convenience (same convention as setup_s). Previously left
 * completely untimed on the (unverified) assumption that closing a file
 * already made durable by collective H5Dwrite calls is comparatively
 * free -- now actually measured instead of assumed, the same treatment
 * PDC's own close_server cost and ADIOS2's writer.Close() get, so a
 * three-way "which system understated its own close cost" comparison
 * is possible instead of one-sided.
 */

#define N_TIMESTEPS 3

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <mpi.h>
#include <hdf5.h>

static void
check(hid_t id, const char *what)
{
    if (id < 0) {
        fprintf(stderr, "%s failed\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

/* Collective write of a rank-local buffer into its disjoint slice of a
 * dataset spanning the whole nranks*n_elem domain. Chunked with one chunk
 * exactly matching count[] (one rank's write), so each rank's collective
 * write lands on its own whole chunk instead of multiple ranks
 * contending over shared chunks. */
static void
write_dataset(hid_t file, const char *name, hid_t mem_type, hid_t file_type, void *buf, hsize_t dims[1],
              hsize_t offset[1], hsize_t count[1])
{
    hid_t filespace = H5Screate_simple(1, dims, NULL);
    check(filespace, "H5Screate_simple (file)");

    hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
    check(H5Pset_chunk(dcpl, 1, count), "H5Pset_chunk");

    hid_t dset = H5Dcreate2(file, name, file_type, filespace, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    check(dset, "H5Dcreate2");
    H5Pclose(dcpl);
    H5Sclose(filespace);

    hid_t memspace = H5Screate_simple(1, count, NULL);
    check(memspace, "H5Screate_simple (mem)");

    filespace = H5Dget_space(dset);
    check(H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, count, NULL), "H5Sselect_hyperslab");

    hid_t dxpl = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxpl, H5FD_MPIO_COLLECTIVE);

    check(H5Dwrite(dset, mem_type, memspace, filespace, dxpl, buf), "H5Dwrite");

    H5Pclose(dxpl);
    H5Sclose(filespace);
    H5Sclose(memspace);
    H5Dclose(dset);
}

int
main(int argc, char **argv)
{
    int    rank, nranks;
    long   n_elem;
    int    step;
    size_t i;

    double t_setup0, t_setup1, t_write0, t_write1;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <n_elem_per_rank> [out_file]\n", argv[0]);
        return 1;
    }
    n_elem               = atol(argv[1]);
    const char *out_file = (argc >= 3) ? argv[2] : "hdf5_bench_magnitude.h5";

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    float *vx = (float *)malloc(sizeof(float) * n_elem);
    float *vy = (float *)malloc(sizeof(float) * n_elem);
    float *vz = (float *)malloc(sizeof(float) * n_elem);

    /* Same deterministic pattern hdf5_bench_posthoc_analyze uses to
     * independently regenerate expected values for its correctness
     * check. */
    for (i = 0; i < (size_t)n_elem; ++i) {
        vx[i] = (float)((i % 1000) + 1);
        vy[i] = (float)(((i + 137) % 1000) + 1);
        vz[i] = (float)(((i + 613) % 1000) + 1);
    }

    hsize_t dims[1]   = {(hsize_t)nranks * (hsize_t)n_elem};
    hsize_t offset[1] = {(hsize_t)rank * (hsize_t)n_elem};
    hsize_t count[1]  = {(hsize_t)n_elem};

    if (rank == 0) {
        printf("hdf5_bench_write: nranks=%d n_elem=%ld chunk=%ld elements "
               "(%.3f MiB/rank as float32)\n",
               nranks, n_elem, n_elem, (double)n_elem * sizeof(float) / (1024.0 * 1024.0));
        fflush(stdout);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup0 = MPI_Wtime();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    check(H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL), "H5Pset_fapl_mpio");

    hid_t file = H5Fcreate(out_file, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    check(file, "H5Fcreate");
    H5Pclose(fapl);

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();

    double local_setup = t_setup1 - t_setup0;
    double max_setup;
    MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    /* One dataset per variable per timestep ("vx_0", "vx_1", ...), since
     * HDF5 datasets are named entities within one file. CSV printing is
     * deferred until after H5Fclose is timed below (close_s is a
     * one-time cost, so every row needs its value -- same reason
     * adios2_bench_magnitude.cpp buffers step_write_s[] instead of
     * printing inline). */
    double max_write_by_step[N_TIMESTEPS];
    for (step = 0; step < N_TIMESTEPS; ++step) {
        char vx_name[32], vy_name[32], vz_name[32];
        snprintf(vx_name, sizeof(vx_name), "vx_%d", step);
        snprintf(vy_name, sizeof(vy_name), "vy_%d", step);
        snprintf(vz_name, sizeof(vz_name), "vz_%d", step);

        t_write0 = MPI_Wtime();
        write_dataset(file, vx_name, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE, vx, dims, offset, count);
        write_dataset(file, vy_name, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE, vy, dims, offset, count);
        write_dataset(file, vz_name, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE, vz, dims, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        t_write1 = MPI_Wtime();

        double local_write = t_write1 - t_write0;
        MPI_Reduce(&local_write, &max_write_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double t_close0 = MPI_Wtime();
    H5Fclose(file);
    MPI_Barrier(MPI_COMM_WORLD);
    double t_close1    = MPI_Wtime();
    double local_close = t_close1 - t_close0;
    double max_close;
    MPI_Reduce(&local_close, &max_close, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        for (step = 0; step < N_TIMESTEPS; ++step) {
            printf("posthoc_write,%d,%d,%ld,%.6f,%.6f,%.6f\n", step, nranks, n_elem, max_setup,
                   max_write_by_step[step], max_close);
        }
        fflush(stdout);
    }

    free(vx);
    free(vy);
    free(vz);

    MPI_Finalize();
    return 0;
}
