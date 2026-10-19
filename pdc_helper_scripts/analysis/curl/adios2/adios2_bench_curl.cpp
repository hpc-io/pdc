/**
 * ADIOS2 analog of src/tests/analysis/bench_curl_eager.c: writes a
 * local (nx, ny, nz_per_rank) block of a synthetic wind-velocity field
 * (u, v, w) per rank and produces a "vorticity_magnitude" variable from
 * it.
 *
 * ADIOS2's expression language has a *native* CURL operator
 * (source/adios2/toolkit/derived/Function.cpp: Curl3DFunc/ApplyCurl) --
 * a real central-difference stencil over the 3D domain (central in the
 * interior, one-sided/clamped at each rank's own block edges, unit grid
 * spacing), the same numerical scheme curl_math.h uses -- which in
 * principle lets Data Flyway's two-stage curl_vorticity_magnitude graph
 * (curl, then vector_magnitude) be expressed as a single, nested
 * derived-variable expression: "MAGNITUDE(CURL(u,v,w))".
 *
 * THAT DOES NOT WORK IN THIS BUILD. Confirmed with a minimal,
 * single-rank, hand-verifiable reproduction (u=y, v=z, w=x, so the true
 * curl is the constant (-1,-1,-1) everywhere in the interior): the
 * written file's own "curl" variable reports min=0/max=0 across the
 * *entire* array via bpls, with no exception thrown anywhere. This is
 * not an axis-order or selection mistake on the caller's side (both
 * were checked directly against the raw file); CURL's own output shape
 * appends a trailing dimension of size 3 (Function.cpp: CurlDimsFunc),
 * unlike MAGNITUDE(vx,vy,vz), which preserves its inputs' shape exactly
 * and *does* work correctly (see adios2_bench_magnitude.cpp) -- so the
 * evidence points to a real bug specific to derived variables whose
 * output shape differs from their inputs', not to derived variables in
 * general.
 *
 * This workload therefore falls back to client-side computation for
 * the curl+magnitude step: read u/v/w back, compute vorticity_magnitude
 * with curl_math.h (the same header the PDC-side benchmark and
 * hdf5_analysis_test share, so all three sides compute the identical
 * curl), and write it back as a plain (non-derived) variable. This is
 * ADIOS2's honest current capability for this workload, not a
 * deliberately weakened comparison -- and it is itself a real,
 * meaningful comparison point: it is exactly Data Flyway's own
 * *posthoc* strategy (client-side read/compute/write-back, no
 * server-side graph), run here within one continuous session rather
 * than as two separate relaunched jobs, since ADIOS2 has no server
 * process to restart between phases the way Data Flyway's posthoc mode
 * does.
 *
 * Where this differs from Data Flyway, beyond the above:
 *   - vector_magnitude(vx,vy,vz) genuinely works via ADIOS2's native
 *     MAGNITUDE derived variable elsewhere in this directory
 *     (adios2_bench_magnitude.cpp) -- it is specifically CURL's
 *     shape-changing output that is broken here, not derived variables
 *     as a mechanism.
 *   - No fan-out production (per table.tex): even if CURL worked,
 *     curl_x/curl_y/curl_z would come back as one packed (nx,ny,nz,3)
 *     array, never three independently addressable objects the way
 *     Data Flyway's graph produces them.
 *   - No lazy trigger and no single-handle read/write interleaving --
 *     same as adios2_bench_magnitude.cpp.
 *
 * Correctness checking goes one step further here than either PDC's own
 * posthoc analog (bench_curl_analyze.c) or the HDF5 one
 * (hdf5_bench_curl_analyze.c): both of those validate the in-memory
 * computed buffer against a freshly-regenerated expected value without
 * ever reading vorticity_magnitude back from storage (documented in
 * both as fine since everything happens within one continuous
 * process/session). This benchmark instead does a real confirmation
 * read of vorticity_magnitude back from the .bp file via a fresh Engine
 * and validates *that* -- removing any doubt that the write itself, not
 * just the CPU-side curl math, round-tripped correctly.
 *
 * Usage: adios2_bench_curl <nx> <ny> <nz_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,nx,ny,nz_per_rank,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,step_total_s,bad
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
    const char *out_file    = (argc >= 5) ? argv[4] : "adios2_bench_curl.bp";

    size_t n_elem = (size_t)nx * (size_t)ny * (size_t)nz_per_rank;

    std::vector<float>  u(n_elem), v(n_elem), w(n_elem);
    std::vector<float>  u_rb(n_elem), v_rb(n_elem), w_rb(n_elem);
    std::vector<double> mag(n_elem);
    for (size_t i = 0; i < n_elem; ++i) {
        u[i] = (float)((i % 1000) + 1);
        v[i] = (float)(((i + 137) % 1000) + 1);
        w[i] = (float)(((i + 613) % 1000) + 1);
    }

    /* Recomputed once outside the loop, exactly as bench_curl_eager.c
     * does: same deterministic u/v/w every timestep, so expected curl
     * and magnitude are identical across timesteps. */
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
    adios2::IO    io = adios.DeclareIO("BenchCurl");

    /* Each rank owns a full (nx,ny) slab at a distinct z-offset, exactly
     * matching bench_curl_eager.c's domain decomposition. */
    adios2::Dims global{(size_t)nx, (size_t)ny, (size_t)nranks * (size_t)nz_per_rank};
    adios2::Dims start{0, 0, (size_t)rank * (size_t)nz_per_rank};
    adios2::Dims count{(size_t)nx, (size_t)ny, (size_t)nz_per_rank};

    auto var_u   = io.DefineVariable<float>("u", global, start, count);
    auto var_v   = io.DefineVariable<float>("v", global, start, count);
    auto var_w   = io.DefineVariable<float>("w", global, start, count);
    auto var_mag = io.DefineVariable<double>("vorticity_magnitude", global, start, count);

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup1  = MPI_Wtime();
    double max_setup = 0;
    {
        double local_setup = t_setup1 - t_setup0;
        MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    int global_bad = 0;
    {
        adios2::Engine writer = io.Open(out_file, adios2::Mode::Write);
        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_write0 = MPI_Wtime();
            writer.BeginStep();
            writer.Put(var_u, u.data());
            writer.Put(var_v, v.data());
            writer.Put(var_w, w.data());
            writer.EndStep();
            MPI_Barrier(MPI_COMM_WORLD);
            double t_write1    = MPI_Wtime();
            double local_write = t_write1 - t_write0;
            double max_write;
            MPI_Reduce(&local_write, &max_write, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            /* Posthoc-style client-side compute (see file header
             * comment): a fresh read engine on the same file to read
             * back the step just written, since ADIOS2's write engine
             * can't also read. */
            double t_readback0 = MPI_Wtime();
            {
                adios2::IO     rio    = adios.DeclareIO("BenchCurlReadback" + std::to_string(step));
                adios2::Engine reader = rio.Open(out_file, adios2::Mode::ReadRandomAccess);
                auto           ru     = rio.InquireVariable<float>("u");
                auto           rv     = rio.InquireVariable<float>("v");
                auto           rw     = rio.InquireVariable<float>("w");
                ru.SetStepSelection({(size_t)step, 1});
                rv.SetStepSelection({(size_t)step, 1});
                rw.SetStepSelection({(size_t)step, 1});
                ru.SetSelection({start, count});
                rv.SetSelection({start, count});
                rw.SetSelection({start, count});
                reader.Get(ru, u_rb.data(), adios2::Mode::Sync);
                reader.Get(rv, v_rb.data(), adios2::Mode::Sync);
                reader.Get(rw, w_rb.data(), adios2::Mode::Sync);
                reader.Close();
            }
            MPI_Barrier(MPI_COMM_WORLD);
            double t_readback1    = MPI_Wtime();
            double local_readback = t_readback1 - t_readback0;
            double max_readback;
            MPI_Reduce(&local_readback, &max_readback, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            double              t_compute0 = MPI_Wtime();
            std::vector<double> cx(n_elem), cy(n_elem), cz(n_elem);
            curl_math_compute(u_rb.data(), v_rb.data(), w_rb.data(), (size_t)nx, (size_t)ny,
                              (size_t)nz_per_rank, cx.data(), cy.data(), cz.data());
            for (size_t i = 0; i < n_elem; ++i)
                mag[i] = sqrt(cx[i] * cx[i] + cy[i] * cy[i] + cz[i] * cz[i]);
            double t_compute1    = MPI_Wtime();
            double local_compute = t_compute1 - t_compute0;
            double max_compute;
            MPI_Reduce(&local_compute, &max_compute, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            double t_writeback0 = MPI_Wtime();
            writer.BeginStep();
            writer.Put(var_mag, mag.data());
            writer.EndStep();
            MPI_Barrier(MPI_COMM_WORLD);
            double t_writeback1    = MPI_Wtime();
            double local_writeback = t_writeback1 - t_writeback0;
            double max_writeback;
            MPI_Reduce(&local_writeback, &max_writeback, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            /* Confirmation read of vorticity_magnitude back from the
             * .bp file (see file header comment) -- a fresh Engine, the
             * same pattern used for the u/v/w readback above, while the
             * writer Engine is still open for later steps. */
            double              t_confirm0 = MPI_Wtime();
            std::vector<double> mag_rb(n_elem);
            {
                adios2::IO     mrio    = adios.DeclareIO("BenchCurlMagConfirm" + std::to_string(step));
                adios2::Engine mreader = mrio.Open(out_file, adios2::Mode::ReadRandomAccess);
                auto           rmag    = mrio.InquireVariable<double>("vorticity_magnitude");
                rmag.SetStepSelection({(size_t)step, 1});
                rmag.SetSelection({start, count});
                mreader.Get(rmag, mag_rb.data(), adios2::Mode::Sync);
                mreader.Close();
            }
            MPI_Barrier(MPI_COMM_WORLD);
            double t_confirm1    = MPI_Wtime();
            double local_confirm = t_confirm1 - t_confirm0;
            double max_confirm;
            MPI_Reduce(&local_confirm, &max_confirm, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            /* Correctness check (not timed): validated against mag_rb,
             * the value actually read back from the file, not the
             * pre-write in-memory mag buffer -- u/v/w are the same
             * deterministic pattern every timestep, so mag_expected
             * (computed once above via curl_math.h) applies unchanged. */
            int local_bad = 0;
            for (size_t i = 0; i < n_elem; ++i) {
                if (fabs(mag_rb[i] - mag_expected[i]) > EPSILON)
                    local_bad++;
            }
            int step_bad = 0;
            MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            global_bad += step_bad;

            if (rank == 0) {
                double step_total =
                    max_setup + max_write + max_readback + max_compute + max_writeback + max_confirm;
                printf("adios2_posthoc_curl,%d,%d,%ld,%ld,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step,
                       nranks, nx, ny, nz_per_rank, max_setup, max_write, max_readback, max_compute,
                       max_writeback, max_confirm, step_total, step_bad);
                fflush(stdout);
            }
        }
        writer.Close();
    }

    MPI_Finalize();
    return global_bad != 0;
}
