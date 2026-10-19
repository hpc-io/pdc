/**
 * Parallel HDF5 curl/vorticity-magnitude benchmark, posthoc phase 1 of 2:
 * write a local (nx, ny, nz_per_rank) block of a synthetic wind-velocity
 * field (u, v, w) per rank into a fresh file and exit. See
 * hdf5_bench_curl_analyze.c (phase 2: read u,v,w back, compute curl then
 * vorticity magnitude, write both out) -- two genuinely separate
 * processes, run as separate srun steps, matching the PDC side's
 * bench_curl_write / bench_curl_analyze split.
 *
 * Like hdf5_bench_write.c, runs N_TIMESTEPS repetitions within this one
 * session, each producing a distinct, uniquely named set of u/v/w
 * datasets ("u_0", "u_1", ...), matching the PDC curl benchmark's
 * multi-timestep workload shape.
 *
 * Usage: hdf5_bench_curl_write <nx> <ny> <nz_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,nx,ny,nz_per_rank,setup_s,write_s
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

/* Collective write of a rank-local (nx,ny,nz_per_rank) block into its
 * disjoint slice of a dataset spanning the whole
 * [nx, ny, nranks*nz_per_rank] domain, split along the z (level) axis.
 * Chunked with one chunk exactly matching count[] (one rank's write), so
 * each rank's collective write lands on its own whole chunk instead of
 * multiple ranks contending over shared chunks -- same convention as
 * hdf5_bench_write.c, just 3D. */
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

int
main(int argc, char **argv)
{
    int    rank, nranks;
    long   nx, ny, nz_per_rank;
    int    step;
    size_t i;

    double t_setup0, t_setup1, t_write0, t_write1;

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

    float *u = (float *)malloc(sizeof(float) * n_elem);
    float *v = (float *)malloc(sizeof(float) * n_elem);
    float *w = (float *)malloc(sizeof(float) * n_elem);

    /* Same deterministic pattern hdf5_bench_curl_analyze.c uses to
     * independently regenerate expected values for its correctness
     * check. */
    for (i = 0; i < n_elem; ++i) {
        u[i] = (float)((i % 1000) + 1);
        v[i] = (float)(((i + 137) % 1000) + 1);
        w[i] = (float)(((i + 613) % 1000) + 1);
    }

    hsize_t dims[3]   = {(hsize_t)nx, (hsize_t)ny, (hsize_t)nranks * (hsize_t)nz_per_rank};
    hsize_t offset[3] = {0, 0, (hsize_t)rank * (hsize_t)nz_per_rank};
    hsize_t count[3]  = {(hsize_t)nx, (hsize_t)ny, (hsize_t)nz_per_rank};

    if (rank == 0) {
        printf("hdf5_bench_curl_write: nranks=%d nx=%ld ny=%ld nz_per_rank=%ld "
               "(%.3f MiB/rank/variable as float32)\n",
               nranks, nx, ny, nz_per_rank, (double)n_elem * sizeof(float) / (1024.0 * 1024.0));
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

    /* Per-timestep-unique dataset names ("u_0", "u_1", ...), matching the
     * PDC side's bench_curl_write.c. */
    for (step = 0; step < N_TIMESTEPS; ++step) {
        char u_name[32], v_name[32], w_name[32];
        snprintf(u_name, sizeof(u_name), "u_%d", step);
        snprintf(v_name, sizeof(v_name), "v_%d", step);
        snprintf(w_name, sizeof(w_name), "w_%d", step);

        t_write0 = MPI_Wtime();
        write_dataset(file, u_name, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE, u, dims, offset, count);
        write_dataset(file, v_name, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE, v, dims, offset, count);
        write_dataset(file, w_name, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE, w, dims, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        t_write1 = MPI_Wtime();

        double local_write = t_write1 - t_write0;
        double max_write;
        MPI_Reduce(&local_write, &max_write, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            printf("curl_posthoc_write,%d,%d,%ld,%ld,%ld,%.6f,%.6f\n", step, nranks, nx, ny, nz_per_rank,
                   max_setup, max_write);
            fflush(stdout);
        }
    }

    H5Fclose(file);

    free(u);
    free(v);
    free(w);

    MPI_Finalize();
    return 0;
}
