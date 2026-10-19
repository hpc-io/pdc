/**
 * Parallel HDF5 posthoc benchmark, phase 2 of 2: opens the file a prior,
 * already-exited hdf5_bench_write run created, reads the vx/vy/vz
 * datasets back, computes magnitude client-side, and writes the result
 * back as a fourth dataset. Run as a separate srun step from
 * hdf5_bench_write (see hdf5_analysis.sbatch), so the file-open cost of a
 * genuinely new process is included rather than reusing an
 * already-open handle.
 *
 * Like hdf5_bench_write.c, reads back N_TIMESTEPS distinct sets of
 * per-timestep-named vx/vy/vz datasets ("vx_0", "vx_1", ...) and writes a
 * distinct magnitude dataset ("magnitude_0", "magnitude_1", ...) per
 * timestep.
 *
 * Usage: hdf5_bench_posthoc_analyze <n_elem_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,readback_s,compute_s,writeback_s,close_s,step_total_s,bad
 *
 * close_s times this phase's own H5Fclose() -- see hdf5_bench_write.c's
 * header comment for why this is now measured instead of assumed free.
 * Folded into step_total_s the same way PDC's own avg_close_s is folded
 * into posthoc_pdc.sbatch's total_with_close_s.
 */

#define N_TIMESTEPS 3

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <mpi.h>
#include <hdf5.h>

#define EPSILON 1e-3

static void
check(hid_t id, const char *what)
{
    if (id < 0) {
        fprintf(stderr, "%s failed\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

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

static void
read_dataset(hid_t file, const char *name, hid_t mem_type, void *buf, hsize_t offset[1], hsize_t count[1])
{
    hid_t dset = H5Dopen2(file, name, H5P_DEFAULT);
    check(dset, "H5Dopen2");

    hid_t memspace = H5Screate_simple(1, count, NULL);
    check(memspace, "H5Screate_simple (mem)");

    hid_t filespace = H5Dget_space(dset);
    check(H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, count, NULL), "H5Sselect_hyperslab");

    hid_t dxpl = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(dxpl, H5FD_MPIO_COLLECTIVE);

    check(H5Dread(dset, mem_type, memspace, filespace, dxpl, buf), "H5Dread");

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

    double t_setup0, t_setup1, t_readback0, t_readback1;
    double t_compute0, t_compute1, t_writeback0, t_writeback1;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <n_elem_per_rank> [out_file]\n", argv[0]);
        return 1;
    }
    n_elem               = atol(argv[1]);
    const char *out_file = (argc >= 3) ? argv[2] : "hdf5_bench_magnitude.h5";

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    float * vx_rb = (float *)malloc(sizeof(float) * n_elem);
    float * vy_rb = (float *)malloc(sizeof(float) * n_elem);
    float * vz_rb = (float *)malloc(sizeof(float) * n_elem);
    double *mag   = (double *)malloc(sizeof(double) * n_elem);
    memset(mag, 0, sizeof(double) * n_elem);

    hsize_t dims[1]   = {(hsize_t)nranks * (hsize_t)n_elem};
    hsize_t offset[1] = {(hsize_t)rank * (hsize_t)n_elem};
    hsize_t count[1]  = {(hsize_t)n_elem};

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup0 = MPI_Wtime();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    check(H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL), "H5Pset_fapl_mpio");

    hid_t file = H5Fopen(out_file, H5F_ACC_RDWR, fapl);
    check(file, "H5Fopen");
    H5Pclose(fapl);

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();

    double local_setup = t_setup1 - t_setup0;
    double max_setup;
    MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    /* Like hdf5_bench_write.c, vx/vy/vz/magnitude are per-timestep-named
     * datasets ("vx_0", "vx_1", ...). CSV printing is deferred until
     * after H5Fclose is timed below (close_s is a one-time cost, so
     * every row needs its value). */
    double max_readback_by_step[N_TIMESTEPS], max_compute_by_step[N_TIMESTEPS];
    double max_writeback_by_step[N_TIMESTEPS];
    int    step_bad_by_step[N_TIMESTEPS];
    int    global_bad = 0;
    for (step = 0; step < N_TIMESTEPS; ++step) {
        char vx_name[32], vy_name[32], vz_name[32], mag_name[32];
        snprintf(vx_name, sizeof(vx_name), "vx_%d", step);
        snprintf(vy_name, sizeof(vy_name), "vy_%d", step);
        snprintf(vz_name, sizeof(vz_name), "vz_%d", step);
        snprintf(mag_name, sizeof(mag_name), "magnitude_%d", step);

        t_readback0 = MPI_Wtime();
        read_dataset(file, vx_name, H5T_NATIVE_FLOAT, vx_rb, offset, count);
        read_dataset(file, vy_name, H5T_NATIVE_FLOAT, vy_rb, offset, count);
        read_dataset(file, vz_name, H5T_NATIVE_FLOAT, vz_rb, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        t_readback1 = MPI_Wtime();

        t_compute0 = MPI_Wtime();
        for (i = 0; i < (size_t)n_elem; ++i) {
            double x = (double)vx_rb[i], y = (double)vy_rb[i], z = (double)vz_rb[i];
            mag[i] = sqrt(x * x + y * y + z * z);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        t_compute1 = MPI_Wtime();

        t_writeback0 = MPI_Wtime();
        write_dataset(file, mag_name, H5T_NATIVE_DOUBLE, H5T_IEEE_F64LE, mag, dims, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        t_writeback1 = MPI_Wtime();

        /* Correctness check (not timed): vx/vy/vz were generated with the
         * same deterministic pattern by hdf5_bench_write, so it's
         * regenerated locally here rather than read back a second time. */
        int local_bad = 0;
        for (i = 0; i < (size_t)n_elem; ++i) {
            float  ex       = (float)((i % 1000) + 1);
            float  ey       = (float)(((i + 137) % 1000) + 1);
            float  ez       = (float)(((i + 613) % 1000) + 1);
            double expected = sqrt((double)ex * ex + (double)ey * ey + (double)ez * ez);
            if (fabs(mag[i] - expected) > EPSILON)
                local_bad++;
        }

        double local_readback  = t_readback1 - t_readback0;
        double local_compute   = t_compute1 - t_compute0;
        double local_writeback = t_writeback1 - t_writeback0;

        int step_bad = 0;
        MPI_Reduce(&local_readback, &max_readback_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_compute, &max_compute_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_writeback, &max_writeback_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        step_bad_by_step[step] = step_bad;
        global_bad += step_bad;
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
            double step_total = max_readback_by_step[step] + max_compute_by_step[step] +
                                max_writeback_by_step[step] + max_close;
            printf("posthoc_analyze,%d,%d,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks, n_elem,
                   max_setup, max_readback_by_step[step], max_compute_by_step[step],
                   max_writeback_by_step[step], max_close, step_total, step_bad_by_step[step]);
        }
        fflush(stdout);
    }

    free(vx_rb);
    free(vy_rb);
    free(vz_rb);
    free(mag);

    MPI_Finalize();
    return global_bad != 0;
}
