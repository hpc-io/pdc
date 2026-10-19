/**
 * HighFive (C++ header-only wrapper over the same libhdf5 the plain-C
 * hdf5_bench_write.c/hdf5_bench_posthoc_analyze.c pair already
 * benchmarks) analog of the magnitude-analysis workload: write vx/vy/vz,
 * read them back, compute magnitude client-side, and write it back --
 * all within one continuous session/process, the same single-session
 * shape as adios2_bench_magnitude.cpp and PDC's own eager/lazy
 * benchmarks, rather than the plain-HDF5 pair's separate write/analyze
 * processes.
 *
 * This exists to answer "does the C++ wrapper cost anything over the raw
 * C API", not to introduce a different storage engine -- HighFive calls
 * into the identical libhdf5 the C benchmarks use, same file format, same
 * MPI-IO path (FileAccessProps + MPIOFileAccess, DataTransferProps +
 * UseCollectiveIO -- see .highfive_src/src/examples/parallel_hdf5_collective_io.cpp).
 * Any measurable difference from hdf5_bench_write/posthoc_analyze's
 * combined numbers is wrapper overhead, not a different I/O path.
 *
 * Like hdf5_bench_write.c/hdf5_bench_posthoc_analyze.c, writes
 * N_TIMESTEPS distinct sets of per-timestep-named datasets ("vx_0",
 * "vx_1", ..., "magnitude_0", ...), and uses the identical deterministic
 * vx/vy/vz formula so results are directly comparable across all three
 * HDF5-backed benchmarks (plain-C posthoc pair and this one) plus
 * PDC/ADIOS2's own magnitude benchmarks.
 *
 * confirm_read_s (a fresh read of magnitude_<step> back from the file,
 * after writeback, used for the correctness check) is deliberately
 * excluded from plot_magnitude_comparison.py's workload total -- it's a
 * correctness check, not workload cost, the same convention every other
 * benchmark in this repo follows for its own confirm-read.
 *
 * close_s times this process's own file close (H5Fclose under the hood,
 * via the File object's destructor) -- same convention as
 * hdf5_bench_write.c's own close_s / PDC's avg_close_s / ADIOS2's
 * writer.Close() timing.
 *
 * Usage: magnitude_highfive <n_elem_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_ranks,n_elem,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,close_s,step_total_s,bad
 */

#define N_TIMESTEPS 3
#define EPSILON     1e-3

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <vector>

#include <mpi.h>
#include <highfive/highfive.hpp>

using namespace HighFive;

static DataTransferProps
collective_xfer()
{
    DataTransferProps xfer;
    xfer.add(UseCollectiveIO{});
    return xfer;
}

static void
write_f32(File &file, const std::string &name, const float *buf, size_t global, size_t offset, size_t count)
{
    DataSetCreateProps props;
    props.add(Chunking(std::vector<hsize_t>{count}));
    DataSet dset = file.createDataSet<float>(name, DataSpace({global}), props);
    auto    xfer = collective_xfer();
    dset.select({offset}, {count}).write_raw(buf, xfer);
}

static void
write_f64(File &file, const std::string &name, const double *buf, size_t global, size_t offset, size_t count)
{
    DataSetCreateProps props;
    props.add(Chunking(std::vector<hsize_t>{count}));
    DataSet dset = file.createDataSet<double>(name, DataSpace({global}), props);
    auto    xfer = collective_xfer();
    dset.select({offset}, {count}).write_raw(buf, xfer);
}

template <typename T>
static void
read_into(File &file, const std::string &name, T *buf, size_t offset, size_t count)
{
    DataSet dset = file.getDataSet(name);
    auto    xfer = collective_xfer();
    dset.select({offset}, {count}).read_raw(buf, xfer);
}

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank, nranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (argc < 2) {
        if (rank == 0)
            fprintf(stderr, "Usage: %s <n_elem_per_rank> [out_file]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }
    long        n_elem   = atol(argv[1]);
    const char *out_file = (argc >= 3) ? argv[2] : "magnitude_highfive.h5";

    std::vector<float>  vx(n_elem), vy(n_elem), vz(n_elem);
    std::vector<float>  vx_rb(n_elem), vy_rb(n_elem), vz_rb(n_elem);
    std::vector<double> mag(n_elem), mag_rb(n_elem);

    /* Same deterministic pattern hdf5_bench_write.c uses, so results are
     * directly comparable and this benchmark can regenerate its own
     * expected values without a second read. */
    for (long i = 0; i < n_elem; ++i) {
        vx[i] = (float)((i % 1000) + 1);
        vy[i] = (float)(((i + 137) % 1000) + 1);
        vz[i] = (float)(((i + 613) % 1000) + 1);
    }

    size_t global = (size_t)nranks * (size_t)n_elem;
    size_t offset = (size_t)rank * (size_t)n_elem;
    size_t count  = (size_t)n_elem;

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup0 = MPI_Wtime();

    FileAccessProps fapl;
    fapl.add(MPIOFileAccess{MPI_COMM_WORLD, MPI_INFO_NULL});
    auto file = std::unique_ptr<File>(new File(out_file, File::Truncate, fapl));

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup1  = MPI_Wtime();
    double max_setup = 0;
    {
        double local_setup = t_setup1 - t_setup0;
        MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    double max_write_by_step[N_TIMESTEPS], max_readback_by_step[N_TIMESTEPS];
    double max_compute_by_step[N_TIMESTEPS], max_writeback_by_step[N_TIMESTEPS];
    double max_confirm_by_step[N_TIMESTEPS];
    int    step_bad_by_step[N_TIMESTEPS];
    int    global_bad = 0;
    for (int step = 0; step < N_TIMESTEPS; ++step) {
        char vx_name[32], vy_name[32], vz_name[32], mag_name[32];
        snprintf(vx_name, sizeof(vx_name), "vx_%d", step);
        snprintf(vy_name, sizeof(vy_name), "vy_%d", step);
        snprintf(vz_name, sizeof(vz_name), "vz_%d", step);
        snprintf(mag_name, sizeof(mag_name), "magnitude_%d", step);

        MPI_Barrier(MPI_COMM_WORLD);
        double t_write0 = MPI_Wtime();
        write_f32(*file, vx_name, vx.data(), global, offset, count);
        write_f32(*file, vy_name, vy.data(), global, offset, count);
        write_f32(*file, vz_name, vz.data(), global, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        double t_write1 = MPI_Wtime();
        {
            double local_write = t_write1 - t_write0;
            MPI_Reduce(&local_write, &max_write_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        }

        double t_readback0 = MPI_Wtime();
        read_into(*file, vx_name, vx_rb.data(), offset, count);
        read_into(*file, vy_name, vy_rb.data(), offset, count);
        read_into(*file, vz_name, vz_rb.data(), offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        double t_readback1 = MPI_Wtime();
        {
            double local_readback = t_readback1 - t_readback0;
            MPI_Reduce(&local_readback, &max_readback_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0,
                       MPI_COMM_WORLD);
        }

        double t_compute0 = MPI_Wtime();
        for (long i = 0; i < n_elem; ++i) {
            double x = (double)vx_rb[i], y = (double)vy_rb[i], z = (double)vz_rb[i];
            mag[i] = sqrt(x * x + y * y + z * z);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        double t_compute1 = MPI_Wtime();
        {
            double local_compute = t_compute1 - t_compute0;
            MPI_Reduce(&local_compute, &max_compute_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        }

        double t_writeback0 = MPI_Wtime();
        write_f64(*file, mag_name, mag.data(), global, offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        double t_writeback1 = MPI_Wtime();
        {
            double local_writeback = t_writeback1 - t_writeback0;
            MPI_Reduce(&local_writeback, &max_writeback_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0,
                       MPI_COMM_WORLD);
        }

        /* Confirmation read of magnitude_<step> back from the file (see
         * file header comment) -- used for the correctness check below,
         * not the in-memory mag[] buffer, so the write itself is what's
         * actually validated. */
        double t_confirm0 = MPI_Wtime();
        read_into(*file, mag_name, mag_rb.data(), offset, count);
        MPI_Barrier(MPI_COMM_WORLD);
        double t_confirm1 = MPI_Wtime();
        {
            double local_confirm = t_confirm1 - t_confirm0;
            MPI_Reduce(&local_confirm, &max_confirm_by_step[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        }

        /* Correctness check (not timed): vx/vy/vz regenerated from the
         * same deterministic formula, exactly like
         * hdf5_bench_posthoc_analyze.c's own check. */
        int local_bad = 0;
        for (long i = 0; i < n_elem; ++i) {
            float  ex       = (float)((i % 1000) + 1);
            float  ey       = (float)(((i + 137) % 1000) + 1);
            float  ez       = (float)(((i + 613) % 1000) + 1);
            double expected = sqrt((double)ex * ex + (double)ey * ey + (double)ez * ez);
            if (fabs(mag_rb[i] - expected) > EPSILON)
                local_bad++;
        }
        int step_bad = 0;
        MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        step_bad_by_step[step] = step_bad;
        global_bad += step_bad;
    }

    /* Timed separately from the loop above (one-time cost, see file
     * header comment) -- file.reset() runs File's destructor
     * deterministically here rather than at end-of-scope, since HighFive
     * has no explicit close() to call directly. CSV printing is deferred
     * until now so every row can carry the real close_s value, same
     * convention as hdf5_bench_write.c. */
    MPI_Barrier(MPI_COMM_WORLD);
    double t_close0 = MPI_Wtime();
    file.reset();
    MPI_Barrier(MPI_COMM_WORLD);
    double t_close1    = MPI_Wtime();
    double local_close = t_close1 - t_close0;
    double max_close;
    MPI_Reduce(&local_close, &max_close, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        for (int step = 0; step < N_TIMESTEPS; ++step) {
            double step_total = max_setup + max_write_by_step[step] + max_readback_by_step[step] +
                                max_compute_by_step[step] + max_writeback_by_step[step] +
                                max_confirm_by_step[step] + max_close;
            printf("magnitude_highfive,%d,%d,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks,
                   n_elem, max_setup, max_write_by_step[step], max_readback_by_step[step],
                   max_compute_by_step[step], max_writeback_by_step[step], max_confirm_by_step[step],
                   max_close, step_total, step_bad_by_step[step]);
        }
        fflush(stdout);
    }

    MPI_Finalize();
    return global_bad != 0;
}
