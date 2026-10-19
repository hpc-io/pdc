/**
 * ADIOS2 analog of Data Flyway's ZFP compression transformation
 * (compression_transform.c / an_client's zfp.json path, and the
 * VPIC-IO + ZFP configuration in evaluation.tex) -- the closest ADIOS2
 * analog to Data Flyway's TF framework is ADIOS2 Operators
 * (Variable::AddOperation), attached once, applied transparently on
 * every write and inverted transparently on every read, with no
 * explicit client-side compress/decompress call. ADIOS2 ships a real
 * ZFP operator (source/adios2/operator/compress/CompressZFP.cpp) that
 * wraps the same libzfp this project already builds against.
 *
 *   var.AddOperation("zfp", {{"rate", "16"}});
 *
 * matches this project's own ZFP configuration exactly (table.tex /
 * evaluation.tex's shared experimental config: "ZFP Mode: Fixed-rate,
 * ZFP Bitrate: 16 bits/value").
 *
 * Where this differs from Data Flyway, documented rather than hidden:
 *   - ADIOS2 Operators have no dynamic CPU/GPU scheduler the way Data
 *     Flyway's TF dispatch does (table.tex: "Runtime scheduling model"
 *     is None for ADIOS2) -- the device is a fixed operator parameter
 *     ("backend": "cuda" for GPU ZFP), not chosen at runtime based on
 *     live utilization.
 *   - Like Data Flyway's own compressed round-trip tests
 *     (vector_magnitude_compressed_test.c), correctness is checked
 *     against a looser tolerance than the other workloads here: ZFP at
 *     a fixed bitrate is genuinely lossy, not a bug to be tightened
 *     away.
 *
 * Usage: adios2_bench_compression <n_elem_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,write_s,confirm_read_s,step_total_s,bad
 */

#define N_TIMESTEPS 3

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <vector>
#include <mpi.h>
#include <adios2.h>

/* ZFP at rate=16 bits/value for float32 is a real (~2x) fixed-rate
 * compression, not lossless -- same tolerance precedent as
 * src/tests/analysis/vector_magnitude_compressed_test.c's own comment:
 * "zfp is lossy compression; verify round-tripped correctly, not
 * bit-exact". */
#define EPSILON 1e-2

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
    const char *out_file = (argc >= 3) ? argv[2] : "adios2_bench_compression.bp";

    std::vector<float> data(n_elem), data_read(n_elem);
    for (long i = 0; i < n_elem; ++i)
        data[i] = (float)((i % 1000) + 1) * 1.000123f;

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup0 = MPI_Wtime();

    adios2::ADIOS adios(MPI_COMM_WORLD);
    adios2::IO    io = adios.DeclareIO("BenchCompression");

    size_t global = (size_t)nranks * (size_t)n_elem;
    size_t offset = (size_t)rank * (size_t)n_elem;
    size_t count  = (size_t)n_elem;

    auto var = io.DefineVariable<float>("data", {global}, {offset}, {count});
    var.AddOperation("zfp", {{"rate", "16"}});

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup1  = MPI_Wtime();
    double max_setup = 0;
    {
        double local_setup = t_setup1 - t_setup0;
        MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    double step_write_s[N_TIMESTEPS];
    {
        adios2::Engine writer = io.Open(out_file, adios2::Mode::Write);
        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_write0 = MPI_Wtime();

            /* Operator compression happens transparently as part of
             * this bracket -- no explicit client-side compress call,
             * matching Data Flyway's "attach once, ordinary writes
             * drive both directions" transform model. */
            writer.BeginStep();
            writer.Put(var, data.data());
            writer.EndStep();

            MPI_Barrier(MPI_COMM_WORLD);
            double t_write1    = MPI_Wtime();
            double local_write = t_write1 - t_write0;
            MPI_Reduce(&local_write, &step_write_s[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        }
        writer.Close();
    }

    int global_bad = 0;
    {
        adios2::IO     rio    = adios.DeclareIO("BenchCompressionRead");
        adios2::Engine reader = rio.Open(out_file, adios2::Mode::ReadRandomAccess);
        auto           rvar   = rio.InquireVariable<float>("data");

        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_read0 = MPI_Wtime();

            rvar.SetStepSelection({(size_t)step, 1});
            rvar.SetSelection({{offset}, {count}});
            std::fill(data_read.begin(), data_read.end(), 0.0f);
            /* Transparent decompression happens inside this Get() --
             * again, no explicit client-side decompress call. */
            reader.Get(rvar, data_read.data(), adios2::Mode::Sync);

            MPI_Barrier(MPI_COMM_WORLD);
            double t_read1    = MPI_Wtime();
            double local_read = t_read1 - t_read0;
            double max_read   = 0;
            MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            int local_bad = 0;
            for (long i = 0; i < n_elem; ++i)
                if (fabs((double)data_read[i] - (double)data[i]) > EPSILON)
                    local_bad++;
            int step_bad = 0;
            MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            global_bad += step_bad;

            if (rank == 0) {
                double step_total = max_setup + step_write_s[step] + max_read;
                printf("adios2_zfp,%d,%d,%ld,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks, n_elem, max_setup,
                       step_write_s[step], max_read, step_total, step_bad);
                fflush(stdout);
            }
        }
        reader.Close();
    }

    MPI_Finalize();
    return global_bad != 0;
}
