/**
 * ADIOS2 analog of src/tests/misc/bdcats.c -- reads back the 8-field
 * VPIC-style particle schema (dX, dY, dZ, Ux, Uy, Uz, q, i) that
 * adios2_vpicio already wrote, one step at a time, timing pure read
 * throughput. Run as its own process, after adios2_vpicio has fully
 * exited, against the .bp file it produced -- the direct analog of
 * bdcats.c reading the PDC objects a separately-launched vpicio already
 * wrote.
 *
 * Like bdcats.c itself, there is NO correctness check here -- bdcats.c
 * never had one (it's a read-throughput benchmark, not a correctness
 * benchmark), so this doesn't add one either; that would stop being an
 * exact replica. See vpicio_verify.c / adios2_vpicio's own verifier (if
 * one is added later) for actual data verification, which is a
 * deliberately separate concern from this benchmark.
 *
 * Usage: adios2_bdcats <numparticles> <steps> [out_file]
 *
 * Prints one CSV line per step from rank 0:
 *   mode,step,n_ranks,open_s,read_s,step_total_s
 */

#include <cstdio>
#include <cstdlib>
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

    if (argc < 3) {
        if (rank == 0)
            fprintf(stderr, "Usage: %s <numparticles> <steps> [out_file]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }
    long        numparticles = atol(argv[1]);
    int         steps        = atoi(argv[2]);
    const char *out_file     = (argc >= 4) ? argv[3] : "adios2_vpicio.bp";

    std::vector<float> dX(numparticles), dY(numparticles), dZ(numparticles);
    std::vector<float> Ux(numparticles), Uy(numparticles), Uz(numparticles), q(numparticles);
    std::vector<int>   id(numparticles);

    size_t offset = (size_t)rank * (size_t)numparticles;
    size_t count  = (size_t)numparticles;

    double         t_open0 = MPI_Wtime();
    adios2::ADIOS  adios(MPI_COMM_WORLD);
    adios2::IO     io     = adios.DeclareIO("BenchBdcats");
    adios2::Engine reader = io.Open(out_file, adios2::Mode::ReadRandomAccess);

    auto var_dX = io.InquireVariable<float>("dX");
    auto var_dY = io.InquireVariable<float>("dY");
    auto var_dZ = io.InquireVariable<float>("dZ");
    auto var_Ux = io.InquireVariable<float>("Ux");
    auto var_Uy = io.InquireVariable<float>("Uy");
    auto var_Uz = io.InquireVariable<float>("Uz");
    auto var_q  = io.InquireVariable<float>("q");
    auto var_id = io.InquireVariable<int>("i");

    MPI_Barrier(MPI_COMM_WORLD);
    double t_open1  = MPI_Wtime();
    double max_open = 0;
    {
        double local_open = t_open1 - t_open0;
        MPI_Reduce(&local_open, &max_open, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    for (int step = 0; step < steps; ++step) {
        var_dX.SetStepSelection({(size_t)step, 1});
        var_dY.SetStepSelection({(size_t)step, 1});
        var_dZ.SetStepSelection({(size_t)step, 1});
        var_Ux.SetStepSelection({(size_t)step, 1});
        var_Uy.SetStepSelection({(size_t)step, 1});
        var_Uz.SetStepSelection({(size_t)step, 1});
        var_q.SetStepSelection({(size_t)step, 1});
        var_id.SetStepSelection({(size_t)step, 1});
        var_dX.SetSelection({{offset}, {count}});
        var_dY.SetSelection({{offset}, {count}});
        var_dZ.SetSelection({{offset}, {count}});
        var_Ux.SetSelection({{offset}, {count}});
        var_Uy.SetSelection({{offset}, {count}});
        var_Uz.SetSelection({{offset}, {count}});
        var_q.SetSelection({{offset}, {count}});
        var_id.SetSelection({{offset}, {count}});

        MPI_Barrier(MPI_COMM_WORLD);
        double t_read0 = MPI_Wtime();

        reader.Get(var_dX, dX.data(), adios2::Mode::Sync);
        reader.Get(var_dY, dY.data(), adios2::Mode::Sync);
        reader.Get(var_dZ, dZ.data(), adios2::Mode::Sync);
        reader.Get(var_Ux, Ux.data(), adios2::Mode::Sync);
        reader.Get(var_Uy, Uy.data(), adios2::Mode::Sync);
        reader.Get(var_Uz, Uz.data(), adios2::Mode::Sync);
        reader.Get(var_q, q.data(), adios2::Mode::Sync);
        reader.Get(var_id, id.data(), adios2::Mode::Sync);

        MPI_Barrier(MPI_COMM_WORLD);
        double t_read1    = MPI_Wtime();
        double local_read = t_read1 - t_read0;
        double max_read;
        MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            double step_total = max_open + max_read;
            printf("adios2_bdcats,%d,%d,%.6f,%.6f,%.6f\n", step, nranks, max_open, max_read, step_total);
            fflush(stdout);
        }
    }
    reader.Close();

    MPI_Finalize();
    return 0;
}
