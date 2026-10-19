/**
 * Parallel HDF5 curl/vorticity-magnitude benchmark, posthoc phase 2 of 2:
 * reopens the file a prior, already-exited hdf5_bench_curl_write run
 * created, reads the u/v/w datasets back, computes curl (via the shared
 * kernel in curl_math.h -- the identical kernel the PDC side's builtin
 * and bench_curl_analyze.c use, so results are directly comparable) and
 * then vorticity magnitude, and writes both curl_x/y/z and
 * vorticity_magnitude out as four datasets, all in this one process.
 *
 * This matches the two-binary (write, then analyze) shape of the plain
 * magnitude baseline (hdf5_bench_write.c / hdf5_bench_posthoc_analyze.c)
 * rather than splitting "compute curl" and "compute magnitude" into
 * their own separate job steps -- a real posthoc analysis workload reads
 * the data once and derives whatever downstream quantities it needs in
 * that same pass. curl is still written out here (not just magnitude) to
 * keep the data volume this baseline persists comparable to the PDC
 * side's eager "Store" strategy (see curl_analysis_scripts/README.md).
 *
 * Like hdf5_bench_curl_write.c, runs N_TIMESTEPS repetitions within this
 * one session, reading back N_TIMESTEPS distinct sets of u/v/w
 * ("u_0", "u_1", ...) and producing N_TIMESTEPS distinct sets of
 * curl_x/y/z/vorticity_magnitude datasets.
 *
 * Usage: hdf5_bench_curl_analyze <nx> <ny> <nz_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,nx,ny,nz_per_rank,setup_s,readback_s,curl_compute_s,curl_writeback_s,magnitude_compute_s,magnitude_writeback_s,step_total_s,bad
 */

#define N_TIMESTEPS 3

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <mpi.h>
#include <hdf5.h>

#include "curl_math.h"

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
write_dataset(hid_t file, const char *name, hid_t mem_type, hid_t file_type, void *buf, hsize_t dims[3],
              hsize_t offset[3], hsize_t count[3])
{
    hid_t filespace = H5Screate_simple(3, dims, NULL);
    check(filespace, "H5Screate_simple (file)");

    hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
    check(H5Pset_chunk(dcpl, 3, count), "H5Pset_chunk");

    hid_t dset = H5Dcreate2(file, name, file_type, filespace, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    check(dset, "H5Dcreate2");
    H5Pclose(dcpl);
    H5Sclose(filespace);

    hid_t memspace = H5Screate_simple(3, count, NULL);
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
read_dataset(hid_t file, const char *name, hid_t mem_type, void *buf, hsize_t offset[3], hsize_t count[3])
{
    hid_t dset = H5Dopen2(file, name, H5P_DEFAULT);
    check(dset, "H5Dopen2");

    hid_t memspace = H5Screate_simple(3, count, NULL);
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
    long   nx, ny, nz_per_rank;
    int    step;
    size_t i;

    double t_setup0, t_setup1, t_readback0, t_readback1;
    double t_curl_compute0, t_curl_compute1, t_curl_wb0, t_curl_wb1;
    double t_mag_compute0, t_mag_compute1, t_mag_wb0, t_mag_wb1;

    if (argc < 4) {
        fprintf(stderr, "Usage: %s <nx> <ny> <nz_per_rank> [out_file]\n", argv[0]);
        return 1;
    }
    nx                   = atol(argv[1]);
    ny                   = atol(argv[2]);
    nz_per_rank          = atol(argv[3]);
    const char *out_file = (argc >= 5) ? argv[4] : "hdf5_bench_curl.h5";

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    size_t n_elem = (size_t)nx * (size_t)ny * (size_t)nz_per_rank;

    float * u_rb   = (float *)malloc(sizeof(float) * n_elem);
    float * v_rb   = (float *)malloc(sizeof(float) * n_elem);
    float * w_rb   = (float *)malloc(sizeof(float) * n_elem);
    double *curl_x = (double *)malloc(sizeof(double) * n_elem);
    double *curl_y = (double *)malloc(sizeof(double) * n_elem);
    double *curl_z = (double *)malloc(sizeof(double) * n_elem);
    double *mag    = (double *)malloc(sizeof(double) * n_elem);

    hsize_t dims[3]   = {(hsize_t)nx, (hsize_t)ny, (hsize_t)nranks * (hsize_t)nz_per_rank};
    hsize_t offset[3] = {0, 0, (hsize_t)rank * (hsize_t)nz_per_rank};
    hsize_t count[3]  = {(hsize_t)nx, (hsize_t)ny, (hsize_t)nz_per_rank};

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

    /* Per-timestep-unique dataset names ("u_0", "u_1", ...), matching the
     * PDC side's bench_curl_analyze.c. */
    int global_bad = 0;
    for (step = 0; step < N_TIMESTEPS; ++step) {
        char u_name[32], v_name[32], w_name[32];
        char curl_x_name[32], curl_y_name[32], curl_z_name[32], mag_name[32];
        snprintf(u_name, sizeof(u_name), "u_%d", step);
        snprintf(v_name, sizeof(v_name), "v_%d", step);
        snprintf(w_name, sizeof(w_name), "w_%d", step);
        snprintf(curl_x_name, sizeof(curl_x_name), "curl_x_%d", step);
        snprintf(curl_y_name, sizeof(curl_y_name), "curl_y_%d", step);
        snprintf(curl_z_name, sizeof(curl_z_name), "curl_z_%d", step);
        snprintf(mag_name, sizeof(mag_name), "vorticity_magnitude_%d", step);

        t_readback0 = MPI_Wtime();
        read_dataset(file, u_name, H5T_NATIVE_FLOAT, u_rb, offset, count);
        read_dataset(file, v_name, H5T_NATIVE_FLOAT, v_rb, offset, count);
        read_dataset(file, w_name, H5T_NATIVE_FLOAT, w_rb, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        t_readback1 = MPI_Wtime();

        t_curl_compute0 = MPI_Wtime();
        curl_math_compute(u_rb, v_rb, w_rb, (size_t)nx, (size_t)ny, (size_t)nz_per_rank, curl_x, curl_y,
                          curl_z);
        MPI_Barrier(MPI_COMM_WORLD);
        t_curl_compute1 = MPI_Wtime();

        t_curl_wb0 = MPI_Wtime();
        write_dataset(file, curl_x_name, H5T_NATIVE_DOUBLE, H5T_IEEE_F64LE, curl_x, dims, offset, count);
        write_dataset(file, curl_y_name, H5T_NATIVE_DOUBLE, H5T_IEEE_F64LE, curl_y, dims, offset, count);
        write_dataset(file, curl_z_name, H5T_NATIVE_DOUBLE, H5T_IEEE_F64LE, curl_z, dims, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        t_curl_wb1 = MPI_Wtime();

        /* magnitude is derived from curl_x/y/z already sitting in memory
         * from the compute step above -- no need to read curl back from
         * the file (that would only be necessary if this were a
         * genuinely separate later process, which is exactly the split
         * this benchmark deliberately avoids -- see file header
         * comment). */
        t_mag_compute0 = MPI_Wtime();
        for (i = 0; i < n_elem; ++i) {
            double cx = curl_x[i], cy = curl_y[i], cz = curl_z[i];
            mag[i] = sqrt(cx * cx + cy * cy + cz * cz);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        t_mag_compute1 = MPI_Wtime();

        t_mag_wb0 = MPI_Wtime();
        write_dataset(file, mag_name, H5T_NATIVE_DOUBLE, H5T_IEEE_F64LE, mag, dims, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        t_mag_wb1 = MPI_Wtime();

        /* Correctness check (not timed): u/v/w were generated with the
         * same deterministic pattern by hdf5_bench_curl_write -- every
         * timestep uses the identical pattern, so expected curl/magnitude
         * are regenerated locally here rather than reading anything back
         * a second time. */
        float *u = (float *)malloc(sizeof(float) * n_elem);
        float *v = (float *)malloc(sizeof(float) * n_elem);
        float *w = (float *)malloc(sizeof(float) * n_elem);
        for (i = 0; i < n_elem; ++i) {
            u[i] = (float)((i % 1000) + 1);
            v[i] = (float)(((i + 137) % 1000) + 1);
            w[i] = (float)(((i + 613) % 1000) + 1);
        }
        double *curl_x_expected = (double *)malloc(sizeof(double) * n_elem);
        double *curl_y_expected = (double *)malloc(sizeof(double) * n_elem);
        double *curl_z_expected = (double *)malloc(sizeof(double) * n_elem);
        curl_math_compute(u, v, w, (size_t)nx, (size_t)ny, (size_t)nz_per_rank, curl_x_expected,
                          curl_y_expected, curl_z_expected);

        int local_bad = 0;
        for (i = 0; i < n_elem; ++i) {
            double expected =
                sqrt(curl_x_expected[i] * curl_x_expected[i] + curl_y_expected[i] * curl_y_expected[i] +
                     curl_z_expected[i] * curl_z_expected[i]);
            if (fabs(mag[i] - expected) > EPSILON)
                local_bad++;
        }
        int step_bad = 0;
        MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        global_bad += step_bad;

        free(u);
        free(v);
        free(w);
        free(curl_x_expected);
        free(curl_y_expected);
        free(curl_z_expected);

        double local_readback  = t_readback1 - t_readback0;
        double local_curl_comp = t_curl_compute1 - t_curl_compute0;
        double local_curl_wb   = t_curl_wb1 - t_curl_wb0;
        double local_mag_comp  = t_mag_compute1 - t_mag_compute0;
        double local_mag_wb    = t_mag_wb1 - t_mag_wb0;

        double max_readback, max_curl_comp, max_curl_wb, max_mag_comp, max_mag_wb;
        MPI_Reduce(&local_readback, &max_readback, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_curl_comp, &max_curl_comp, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_curl_wb, &max_curl_wb, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_mag_comp, &max_mag_comp, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_mag_wb, &max_mag_wb, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            double step_total =
                max_setup + max_readback + max_curl_comp + max_curl_wb + max_mag_comp + max_mag_wb;
            printf("curl_posthoc_analyze,%d,%d,%ld,%ld,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step,
                   nranks, nx, ny, nz_per_rank, max_setup, max_readback, max_curl_comp, max_curl_wb,
                   max_mag_comp, max_mag_wb, step_total, step_bad);
            fflush(stdout);
        }
    }

    H5Fclose(file);

    free(u_rb);
    free(v_rb);
    free(w_rb);
    free(curl_x);
    free(curl_y);
    free(curl_z);
    free(mag);

    MPI_Finalize();
    return global_bad != 0;
}
