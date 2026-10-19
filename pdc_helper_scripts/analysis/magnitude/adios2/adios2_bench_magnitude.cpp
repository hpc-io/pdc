/**
 * ADIOS2 analog of src/tests/analysis/bench_magnitude.c's eager mode:
 * writes three vector-component variables (vx, vy, vz) and reads back a
 * "magnitude" variable computed from them, without the client ever
 * computing magnitude itself.
 *
 * The mechanism used is ADIOS2's Derived Variables feature
 * (IO::DefineDerivedVariable), the closest ADIOS2 analog to Data
 * Flyway's analysis (AN) framework -- see the "ADIOS2 derived
 * variables" column in this project's own feature-comparison table
 * (table.tex). Declared once, before the step loop:
 *
 *   io.DefineDerivedVariable("magnitude", "MAGNITUDE(vx,vy,vz)",
 *                            adios2::DerivedVarType::StoreData);
 *
 * MAGNITUDE(...) is a native pointwise operator in ADIOS2's expression
 * language (source/adios2/toolkit/derived/Function.cpp), computing
 * sqrt(vx^2+vy^2+vz^2) elementwise -- exactly Data Flyway's
 * vector_magnitude builtin kernel (src/server/analysis/pdc_an_builtin_magnitude_cpu.c).
 * DerivedVarType::StoreData makes the result a real, persisted,
 * independently-queryable variable (not just metadata/stats), matching
 * Data Flyway's "trigger: eager, persist: true" vector_magnitude.json.
 *
 * Where this genuinely differs from Data Flyway, documented rather than
 * hidden:
 *   - Computation happens at EndStep() (engine/bp5/BP5Writer.cpp), i.e.
 *     write-triggered, matching Data Flyway's eager mode -- there is no
 *     ADIOS2 equivalent of Data Flyway's *lazy* (read-triggered) trigger
 *     for derived variables; every derived variable in ADIOS2 is
 *     eager-only.
 *   - ADIOS2 has no notion of write vs. read within one still-open
 *     engine the way PDC's region-transfer API does; the write engine
 *     is closed and a separate read engine opened for the confirmation
 *     read below, rather than interleaving read/write on one handle.
 *   - MAGNITUDE's output type is hardwired to match its input type
 *     (ExprCodeStream.cpp's operator table maps OP_MAGN to
 *     SameTypeFunc, not a promote-to-double function) -- there is no
 *     API to request double output from float inputs the way Data
 *     Flyway's vector_magnitude builtin kernel explicitly does
 *     (PDC_DOUBLE regardless of PDC_FLOAT inputs). magnitude is
 *     therefore float here, confirmed against the actual written file
 *     with bpls, not double.
 *   - Timesteps are expressed with ADIOS2's native Begin/EndStep
 *     mechanism instead of Data Flyway's per-timestep-uniquely-named
 *     objects (a PDC-specific workaround, not meaningful to replicate
 *     here).
 *
 * Usage: adios2_bench_magnitude <n_elem_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,write_s,confirm_read_s,close_s,step_total_s,bad
 *
 * close_s times writer.Close() (BP5's durability flush), a one-time job
 * cost repeated on every row for CSV convenience -- same convention as
 * setup_s and directly analogous to PDC's avg_close_s (close_server's
 * checkpoint-to-disk cost). Previously left completely untimed; measured
 * externally via whole-process wall-clock time it accounts for a real,
 * non-trivial chunk of cost (~0.6-0.8s at 64 MiB/rank locally) that a
 * total omitting it would understate, the same way omitting PDC's
 * close_server cost would understate PDC's total.
 */

#define N_TIMESTEPS 3

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>
#include <mpi.h>
#include <adios2.h>

#define EPSILON 1e-3

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
    const char *out_file = (argc >= 3) ? argv[2] : "adios2_bench_magnitude.bp";

    std::vector<float> vx(n_elem), vy(n_elem), vz(n_elem);
    std::vector<float> mag(n_elem); /* float, not double -- see file header comment */
    for (long i = 0; i < n_elem; ++i) {
        vx[i] = (float)((i % 1000) + 1);
        vy[i] = (float)(((i + 137) % 1000) + 1);
        vz[i] = (float)(((i + 613) % 1000) + 1);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup0 = MPI_Wtime();

    adios2::ADIOS adios(MPI_COMM_WORLD);
    adios2::IO    io = adios.DeclareIO("BenchMagnitude");

    /* BP5's default aggregation strategy (TwoLevelShm) routes writes
     * through a subset of node-local "aggregator" ranks via shared
     * memory rather than every rank touching storage directly -- not
     * MPI-IO collective I/O the way HDF5's H5FD_MPIO_COLLECTIVE is, but
     * a comparable "fewer ranks actually do I/O" strategy. Optional
     * override so this benchmark can also be run with every rank writing
     * independently (no aggregation at all), for a genuine independent-
     * I/O comparison point -- see magnitude_analysis_everyonewrites.sbatch. */
    const char *aggregation_type = getenv("ADIOS2_AGGREGATION_TYPE");
    if (aggregation_type != nullptr) {
        io.SetParameter("AggregationType", aggregation_type);
        if (rank == 0)
            fprintf(stderr, "adios2_bench_magnitude: AggregationType=%s\n", aggregation_type);
    }

    size_t global = (size_t)nranks * (size_t)n_elem;
    size_t offset = (size_t)rank * (size_t)n_elem;
    size_t count  = (size_t)n_elem;

    auto var_vx = io.DefineVariable<float>("vx", {global}, {offset}, {count});
    auto var_vy = io.DefineVariable<float>("vy", {global}, {offset}, {count});
    auto var_vz = io.DefineVariable<float>("vz", {global}, {offset}, {count});

    /* Declared once, outside the step loop -- mirrors Data Flyway's
     * "attach the graph once, reattach fresh objects per timestep"
     * pattern in bench_magnitude.c, except ADIOS2's own step mechanism
     * makes the per-timestep re-attachment unnecessary: the same
     * variable/derived-variable declarations apply to every step. */
    io.DefineDerivedVariable("magnitude", "MAGNITUDE(vx,vy,vz)", adios2::DerivedVarType::StoreData);

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup1  = MPI_Wtime();
    double max_setup = 0;
    {
        double local_setup = t_setup1 - t_setup0;
        MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    double step_write_s[N_TIMESTEPS];
    double max_close = 0;
    {
        adios2::Engine writer = io.Open(out_file, adios2::Mode::Write);
        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_write0 = MPI_Wtime();

            /* The last Put() before EndStep() is what, for Data Flyway,
             * transparently triggers server-side eager computation in
             * the write path; here EndStep() is the equivalent trigger
             * point -- ComputeDerivedVariables() runs inside it
             * (engine/bp5/BP5Writer.cpp), so this whole bracket,
             * matching Data Flyway's own "bracket the whole step,
             * including metadata/compute, not just the data transfer"
             * timing convention, includes the derived-variable compute
             * cost. */
            writer.BeginStep();
            writer.Put(var_vx, vx.data());
            writer.Put(var_vy, vy.data());
            writer.Put(var_vz, vz.data());
            writer.EndStep();

            MPI_Barrier(MPI_COMM_WORLD);
            double t_write1    = MPI_Wtime();
            double local_write = t_write1 - t_write0;
            MPI_Reduce(&local_write, &step_write_s[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        }

        /* writer.Close() was previously left completely untimed -- measured
         * externally via whole-process wall-clock time, it accounts for
         * ~0.6-0.8s (real, not noise) at 64 MiB/rank, larger than PDC's own
         * separately-timed server close cost. This is the durability-flush
         * analog of PDC's close_server (avg_close_s in the PDC CSVs): BP5
         * finalizes/flushes whatever wasn't already written during EndStep()
         * here, so leaving it untimed understated ADIOS2's true total cost
         * the same way omitting close_server would for PDC. */
        MPI_Barrier(MPI_COMM_WORLD);
        double t_close0 = MPI_Wtime();
        writer.Close();
        MPI_Barrier(MPI_COMM_WORLD);
        double t_close1    = MPI_Wtime();
        double local_close = t_close1 - t_close0;
        MPI_Reduce(&local_close, &max_close, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    /* Confirmation read: a fresh engine, matching Data Flyway's
     * confirmation read of magnitude after the triggering write --
     * ADIOS2 has no single-handle read/write interleaving the way
     * PDCregion_transfer does, so the write engine above is fully
     * closed first (see file header comment). */
    int global_bad = 0;
    {
        adios2::IO     rio    = adios.DeclareIO("BenchMagnitudeRead");
        adios2::Engine reader = rio.Open(out_file, adios2::Mode::ReadRandomAccess);
        auto           rmag   = rio.InquireVariable<float>("magnitude");

        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_read0 = MPI_Wtime();

            rmag.SetStepSelection({(size_t)step, 1});
            rmag.SetSelection({{offset}, {count}});
            std::fill(mag.begin(), mag.end(), 0.0f);
            reader.Get(rmag, mag.data(), adios2::Mode::Sync);

            MPI_Barrier(MPI_COMM_WORLD);
            double t_read1    = MPI_Wtime();
            double local_read = t_read1 - t_read0;
            double max_read   = 0;
            MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            /* Correctness check (not timed): same deterministic pattern
             * as bench_magnitude.c's own check. */
            int local_bad = 0;
            for (long i = 0; i < n_elem; ++i) {
                double x = (double)vx[i], y = (double)vy[i], z = (double)vz[i];
                double expected = sqrt(x * x + y * y + z * z);
                if (fabs(mag[i] - expected) > EPSILON)
                    local_bad++;
            }
            int step_bad = 0;
            MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            global_bad += step_bad;

            if (rank == 0) {
                /* max_close is a one-time job cost (like max_setup), not a
                 * per-step cost -- repeated on every row for CSV
                 * convenience, matching PDC's own avg_close_s convention,
                 * and added into step_total here the same way avg_close_s
                 * is folded into total_with_close_s for PDC. */
                double step_total = max_setup + step_write_s[step] + max_read + max_close;
                printf("adios2_derived,%d,%d,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks, n_elem,
                       max_setup, step_write_s[step], max_read, max_close, step_total, step_bad);
                fflush(stdout);
            }
        }
        reader.Close();
    }

    MPI_Finalize();
    return global_bad != 0;
}
