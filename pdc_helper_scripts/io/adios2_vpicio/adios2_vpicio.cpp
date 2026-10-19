/**
 * ADIOS2 analog of src/tests/misc/vpicio.c -- writes the same 8-field
 * VPIC-style particle schema (dX, dY, dZ, Ux, Uy, Uz, q, i) using the
 * identical deterministic, index-based data formula vpicio.c uses
 * (global_i = rank*numparticles + i), so the two are directly
 * comparable and either side's data could in principle be checked
 * against the other's formula.
 *
 * Where this genuinely differs from vpicio.c, documented rather than
 * hidden:
 *   - Static variable names (dX, dY, ..., i) declared once, with
 *     ADIOS2's own native BeginStep/EndStep expressing timesteps,
 *     instead of vpicio.c's per-timestep-uniquely-named objects
 *     ("dX-0", "dX-1", ...) -- that naming scheme is a PDC-specific
 *     workaround (see bench_magnitude.c's own comment on the same
 *     issue) that isn't meaningful to replicate here, exactly the same
 *     precedent already established by adios2_bench_magnitude.cpp.
 *   - sleeptime happens strictly between EndStep() and the next
 *     BeginStep() (i.e. outside any timed bracket) rather than
 *     overlapped with an async in-flight transfer the way vpicio.c's
 *     sleep sits between transfer_start and transfer_wait -- ADIOS2's
 *     basic Put/EndStep API has no equivalent async-overlap phase to
 *     inject it into. Either way it's real time excluded from write_s,
 *     not counted as I/O cost.
 *   - No transformation_str / transform argument -- vpicio.c's "zfp" /
 *     "zfp_gpu" / "zfp_libsod" options attach PDC TF graphs; the ADIOS2
 *     analog of TF Operators (ZFP, chained ZFP+encryption) is already
 *     demonstrated separately in adios2_bench_compression.cpp /
 *     adios2_bench_compression_encryption.cpp, so it isn't duplicated
 *     here -- this program mirrors vpicio.c's plain "raw" mode only.
 *   - No correctness check -- vpicio.c itself has none either (it's a
 *     write-throughput benchmark, not a correctness benchmark); see
 *     adios2_bdcats.cpp for the read-back companion, which likewise has
 *     none, matching bdcats.c exactly.
 *
 * Usage: adios2_vpicio <numparticles> <steps> <sleeptime> [out_file]
 *
 * Prints one CSV line per step from rank 0:
 *   mode,step,n_ranks,setup_s,write_s,close_s,step_total_s
 *
 * close_s times writer.Close(), a one-time cost repeated on every row
 * for CSV convenience (same convention as setup_s and directly
 * analogous to PDC's avg_close_s) -- see adios2_bench_magnitude.cpp's
 * own header comment for why this is measured rather than assumed free.
 */

#define NUM_FIELDS 8

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <vector>
#include <mpi.h>
#include <adios2.h>

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank, nranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (argc < 4) {
        if (rank == 0)
            fprintf(stderr, "Usage: %s <numparticles> <steps> <sleeptime> [out_file]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }
    long        numparticles = atol(argv[1]);
    int         steps        = atoi(argv[2]);
    int         sleeptime    = atoi(argv[3]);
    const char *out_file     = (argc >= 5) ? argv[4] : "adios2_vpicio.bp";

    std::vector<float> dX(numparticles), dY(numparticles), dZ(numparticles);
    std::vector<float> Ux(numparticles), Uy(numparticles), Uz(numparticles), q(numparticles);
    std::vector<int>   id(numparticles);

    /* Same deterministic formula as vpicio.c's data generation. */
    for (long i = 0; i < numparticles; ++i) {
        long global_i = (long)rank * numparticles + i;
        dX[i]         = (float)((global_i % 1000) + 1);
        dY[i]         = (float)(((global_i + 137) % 1000) + 1);
        dZ[i]         = (float)(((global_i + 271) % 1000) + 1);
        Ux[i]         = (float)(((global_i + 613) % 1000) + 1);
        Uy[i]         = (float)(((global_i + 911) % 1000) + 1);
        Uz[i]         = (float)(((global_i + 1301) % 1000) + 1);
        q[i]          = (float)((global_i % 1000) + 1);
        id[i]         = (int)global_i;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup0 = MPI_Wtime();

    adios2::ADIOS adios(MPI_COMM_WORLD);
    adios2::IO    io = adios.DeclareIO("BenchVpicio");

    size_t global = (size_t)nranks * (size_t)numparticles;
    size_t offset = (size_t)rank * (size_t)numparticles;
    size_t count  = (size_t)numparticles;

    auto var_dX = io.DefineVariable<float>("dX", {global}, {offset}, {count});
    auto var_dY = io.DefineVariable<float>("dY", {global}, {offset}, {count});
    auto var_dZ = io.DefineVariable<float>("dZ", {global}, {offset}, {count});
    auto var_Ux = io.DefineVariable<float>("Ux", {global}, {offset}, {count});
    auto var_Uy = io.DefineVariable<float>("Uy", {global}, {offset}, {count});
    auto var_Uz = io.DefineVariable<float>("Uz", {global}, {offset}, {count});
    auto var_q  = io.DefineVariable<float>("q", {global}, {offset}, {count});
    auto var_id = io.DefineVariable<int>("i", {global}, {offset}, {count});

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup1  = MPI_Wtime();
    double max_setup = 0;
    {
        double local_setup = t_setup1 - t_setup0;
        MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    std::vector<double> step_write_s(steps);
    adios2::Engine      writer = io.Open(out_file, adios2::Mode::Write);
    for (int step = 0; step < steps; ++step) {
        /* Same per-step boundary overrides on q/i at index 0 and
         * numparticles-1 as vpicio.c's own step loop. */
        q[0]                 = (float)(rank + step * 2);
        id[0]                = rank + step;
        q[numparticles - 1]  = (float)(rank - step * 2);
        id[numparticles - 1] = rank - step;

        MPI_Barrier(MPI_COMM_WORLD);
        double t_write0 = MPI_Wtime();

        writer.BeginStep();
        writer.Put(var_dX, dX.data());
        writer.Put(var_dY, dY.data());
        writer.Put(var_dZ, dZ.data());
        writer.Put(var_Ux, Ux.data());
        writer.Put(var_Uy, Uy.data());
        writer.Put(var_Uz, Uz.data());
        writer.Put(var_q, q.data());
        writer.Put(var_id, id.data());
        writer.EndStep();

        MPI_Barrier(MPI_COMM_WORLD);
        double t_write1    = MPI_Wtime();
        double local_write = t_write1 - t_write0;
        MPI_Reduce(&local_write, &step_write_s[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        /* Emulate compute -- strictly outside the timed write bracket
         * above, same exclusion rationale as vpicio.c's own sleep (see
         * file header comment for why the overlap semantics differ). */
        if (sleeptime > 0 && step != steps - 1)
            sleep(sleeptime);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double t_close0 = MPI_Wtime();
    writer.Close();
    MPI_Barrier(MPI_COMM_WORLD);
    double t_close1    = MPI_Wtime();
    double local_close = t_close1 - t_close0;
    double max_close;
    MPI_Reduce(&local_close, &max_close, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        for (int step = 0; step < steps; ++step) {
            double step_total = max_setup + step_write_s[step] + max_close;
            printf("adios2_vpicio,%d,%d,%.6f,%.6f,%.6f,%.6f\n", step, nranks, max_setup, step_write_s[step],
                   max_close, step_total);
        }
        fflush(stdout);
    }

    MPI_Finalize();
    return 0;
}
