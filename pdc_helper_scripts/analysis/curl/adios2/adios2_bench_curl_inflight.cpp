/**
 * ADIOS2 in-flight analog of src/tests/analysis/bench_curl_eager.c:
 * writes a local (nx, ny, nz_per_rank) block of a synthetic
 * wind-velocity field (u, v, w) per rank and produces "curl" and
 * "vorticity_magnitude" variables from it, both computed automatically
 * by ADIOS2 inside EndStep() -- no readback-from-storage round trip and
 * no manual client-side compute call, the way adios2_bench_curl.cpp
 * (posthoc) requires.
 *
 * This uses ADIOS2's native Derived Variables mechanism for BOTH
 * stages, registered once outside the step loop (same pattern as the
 * working adios2_bench_magnitude.cpp):
 *
 *   io.DefineDerivedVariable("curl", "CURL(u,v,w)", StoreData);
 *   io.DefineDerivedVariable("vorticity_magnitude",
 *                            "MAGNITUDE(CURL(u,v,w))", StoreData);
 *
 * adios2_bench_curl.cpp's header comment documents why this didn't work
 * in earlier testing: ADIOS2's CURL operator produced all-zero (and, on
 * closer inspection this session, sometimes just wrong, not only zero)
 * output when actually exercised -- a real bug in
 * source/adios2/toolkit/derived/Function.cpp's ApplyCurl, confirmed by
 * hand against a known analytic curl (u=y, v=z, w=x has constant curl
 * (-1,-1,-1)) and independently via bpls, not a caller-side mistake or
 * an architectural limitation: ADIOS2's own design already anticipates
 * shape-changing, multi-input derived variables (CurlDimsFunc exists
 * specifically for this), and same-shape derived variables (MAGNITUDE)
 * already worked correctly. ApplyCurl's indexing just didn't match the
 * row-major convention the rest of ADIOS2 (and this project's own
 * curl_math.h) uses. That function's body is now a direct port of
 * curl_math.h's own stencil (see
 * pdc_helper_scripts/adios2_build/patches/
 * 0001-fix-curl-derived-variable-indexing.patch, applied automatically
 * by build_adios2.sh), verified to reproduce the exact expected
 * constant curl at 1 and 4 ranks, both standalone (CURL(u,v,w)) and
 * nested exactly as used below (MAGNITUDE(CURL(u,v,w))) -- the same
 * nested expression this file's predecessor documented as broken.
 *
 * Where this still differs from Data Flyway's own curl_vorticity graph,
 * documented rather than hidden:
 *   - PDC persists curl_x, curl_y, curl_z as three independent,
 *     same-shape objects (see bench_curl_eager.c). ADIOS2's CURL
 *     operator instead packs all three components into one variable,
 *     "curl", with shape (nx,ny,nz,3) -- CurlDimsFunc's own design,
 *     not something this benchmark chooses. The total bytes persisted
 *     are the same; only the object boundary differs.
 *   - ADIOS2 has no mechanism to let one derived variable reference
 *     another (confirmed directly: DefineDerivedVariable("vorticity_"
 *     "magnitude", "MAGNITUDE(curl)", ...) throws "using undefined
 *     variable curl"), so "curl" and "vorticity_magnitude" are each
 *     their own top-level expression over u/v/w; CURL(u,v,w) is
 *     evaluated twice internally (once for "curl", once inside
 *     "vorticity_magnitude"'s own expression) rather than once and
 *     reused, unlike Data Flyway's graph which computes curl once and
 *     feeds it to the magnitude stage.
 *   - Compute happens on the writer's own rank -- ADIOS2 has no
 *     separate server process the way PDC does; the client confirmed
 *     this is an acceptable in-flight analog.
 *   - No lazy trigger, no single-handle read/write interleaving --
 *     same as adios2_bench_magnitude.cpp.
 *
 * Usage: adios2_bench_curl_inflight <nx> <ny> <nz_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,nx,ny,nz_per_rank,setup_s,write_s,confirm_read_s,close_s,step_total_s,bad
 */

#define N_TIMESTEPS 3

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>
#include <mpi.h>
#include <adios2.h>

#include "curl_math.h"

#define EPSILON 1e-3

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank, nranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (argc < 4) {
        if (rank == 0)
            fprintf(stderr, "Usage: %s <nx> <ny> <nz_per_rank> [out_file]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }
    long        nx          = atol(argv[1]);
    long        ny          = atol(argv[2]);
    long        nz_per_rank = atol(argv[3]);
    const char *out_file    = (argc >= 5) ? argv[4] : "adios2_bench_curl_inflight.bp";

    size_t n_elem = (size_t)nx * (size_t)ny * (size_t)nz_per_rank;

    std::vector<float> u(n_elem), v(n_elem), w(n_elem);
    for (size_t i = 0; i < n_elem; ++i) {
        u[i] = (float)((i % 1000) + 1);
        v[i] = (float)(((i + 137) % 1000) + 1);
        w[i] = (float)(((i + 613) % 1000) + 1);
    }

    /* Expected magnitude, computed once outside the loop with the same
     * curl_math.h ApplyCurl was ported from, purely for the correctness
     * check below -- not used anywhere in the write path itself. Same
     * deterministic u/v/w every timestep, so this applies to all of
     * them unchanged, matching bench_curl_eager.c's and
     * adios2_bench_curl.cpp's convention. */
    std::vector<double> curl_x_expected(n_elem), curl_y_expected(n_elem), curl_z_expected(n_elem);
    curl_math_compute(u.data(), v.data(), w.data(), (size_t)nx, (size_t)ny, (size_t)nz_per_rank,
                      curl_x_expected.data(), curl_y_expected.data(), curl_z_expected.data());
    std::vector<double> mag_expected(n_elem);
    for (size_t i = 0; i < n_elem; ++i) {
        double cx = curl_x_expected[i], cy = curl_y_expected[i], cz = curl_z_expected[i];
        mag_expected[i] = sqrt(cx * cx + cy * cy + cz * cz);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup0 = MPI_Wtime();

    adios2::ADIOS adios(MPI_COMM_WORLD);
    adios2::IO    io = adios.DeclareIO("BenchCurlInflight");

    /* Each rank owns a full (nx,ny) slab at a distinct z-offset, exactly
     * matching bench_curl_eager.c's and adios2_bench_curl.cpp's domain
     * decomposition. CURL's own stencil is computed purely from locally
     * held data (no halo exchange, confirmed against
     * source/adios2/toolkit/derived/Function.cpp's ApplyCurl, same
     * one-sided-at-block-edges convention as curl_math.h). */
    adios2::Dims global{(size_t)nx, (size_t)ny, (size_t)nranks * (size_t)nz_per_rank};
    adios2::Dims start{0, 0, (size_t)rank * (size_t)nz_per_rank};
    adios2::Dims count{(size_t)nx, (size_t)ny, (size_t)nz_per_rank};

    auto var_u = io.DefineVariable<float>("u", global, start, count);
    auto var_v = io.DefineVariable<float>("v", global, start, count);
    auto var_w = io.DefineVariable<float>("w", global, start, count);

    /* Declared once, outside the step loop, same pattern as
     * adios2_bench_magnitude.cpp. Both are real, registered derived
     * variables computed automatically inside EndStep() -- nothing in
     * this file's own step loop calls curl_math.h or Puts either one. */
    io.DefineDerivedVariable("curl", "CURL(u,v,w)", adios2::DerivedVarType::StoreData);
    io.DefineDerivedVariable("vorticity_magnitude", "MAGNITUDE(CURL(u,v,w))",
                             adios2::DerivedVarType::StoreData);

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

            /* u/v/w plus both derived variables, all inside one
             * BeginStep()/EndStep() bracket -- matching Data Flyway
             * Eager's "every state computed/persisted as part of the
             * same write" semantics and this project's own convention
             * (see adios2_bench_magnitude.cpp) of timing the whole
             * bracket, not just the data transfer. Neither "curl" nor
             * "vorticity_magnitude" is Put() here -- ADIOS2 computes
             * both automatically inside EndStep(). */
            writer.BeginStep();
            writer.Put(var_u, u.data());
            writer.Put(var_v, v.data());
            writer.Put(var_w, w.data());
            writer.EndStep();

            MPI_Barrier(MPI_COMM_WORLD);
            double t_write1    = MPI_Wtime();
            double local_write = t_write1 - t_write0;
            MPI_Reduce(&local_write, &step_write_s[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        }

        /* writer.Close() timed separately, same convention as
         * adios2_bench_magnitude.cpp (see that file's header comment):
         * BP5's durability flush is a real, non-trivial, one-time cost
         * that a total omitting it would understate. */
        MPI_Barrier(MPI_COMM_WORLD);
        double t_close0 = MPI_Wtime();
        writer.Close();
        MPI_Barrier(MPI_COMM_WORLD);
        double t_close1    = MPI_Wtime();
        double local_close = t_close1 - t_close0;
        MPI_Reduce(&local_close, &max_close, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    /* Confirmation read of vorticity_magnitude only, matching both
     * adios2_bench_magnitude.cpp's and bench_curl_eager.c's
     * confirm-read convention -- a fresh engine, called collectively by
     * every rank (ADIOS2's Engine::Open/Get are collective operations
     * over the IO's communicator; calling them on a subset of ranks
     * deadlocks the others at their next collective call -- confirmed
     * directly while developing this file), since ADIOS2 has no
     * single-handle read/write interleaving the way PDCregion_transfer
     * does. */
    int global_bad = 0;
    {
        adios2::IO     rio    = adios.DeclareIO("BenchCurlInflightRead");
        adios2::Engine reader = rio.Open(out_file, adios2::Mode::ReadRandomAccess);
        auto           rmag   = rio.InquireVariable<float>("vorticity_magnitude");

        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_read0 = MPI_Wtime();

            std::vector<float> mag(n_elem);
            rmag.SetStepSelection({(size_t)step, 1});
            rmag.SetSelection({start, count});
            reader.Get(rmag, mag.data(), adios2::Mode::Sync);

            MPI_Barrier(MPI_COMM_WORLD);
            double t_read1    = MPI_Wtime();
            double local_read = t_read1 - t_read0;
            double max_read   = 0;
            MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            /* Correctness check (not timed): u/v/w are the same
             * deterministic pattern every timestep, so mag_expected
             * (computed once above via curl_math.h) applies unchanged.
             * vorticity_magnitude is float here (MAGNITUDE's output
             * type matches its input, CURL's own output, which matches
             * u/v/w's float -- see adios2_bench_magnitude.cpp's header
             * comment on SameTypeFunc), unlike
             * adios2_bench_curl_inflight's earlier manual-compute
             * version, which stored it as double. */
            int local_bad = 0;
            for (size_t i = 0; i < n_elem; ++i) {
                if (fabs((double)mag[i] - mag_expected[i]) > EPSILON)
                    local_bad++;
            }
            int step_bad = 0;
            MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            global_bad += step_bad;

            if (rank == 0) {
                double step_total = max_setup + step_write_s[step] + max_read + max_close;
                printf("adios2_inflight_curl,%d,%d,%ld,%ld,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks,
                       nx, ny, nz_per_rank, max_setup, step_write_s[step], max_read, max_close, step_total,
                       step_bad);
                fflush(stdout);
            }
        }
        reader.Close();
    }

    MPI_Finalize();
    return global_bad != 0;
}
