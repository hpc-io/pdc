/**
 * Standalone read-only verification client for adios2_bench_magnitude --
 * deliberately a SEPARATE process from the writer, run after the writer
 * has fully exited (including its own writer.Close()), so read timing
 * isn't conflated with the writer's own process/OS-cache warmth. Opens a
 * fresh Engine on the .bp file the writer already produced, reads
 * magnitude back per step, times it, and checks correctness -- the
 * ADIOS2-side analog of pdc_verify_magnitude.c.
 *
 * Usage: adios2_verify_magnitude <n_elem_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,open_s,read_s,bad
 */

#define N_TIMESTEPS 3

#include <cstdio>
#include <cstdlib>
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

    std::vector<float> vx(n_elem), vy(n_elem), vz(n_elem), mag(n_elem);
    for (long i = 0; i < n_elem; ++i) {
        vx[i] = (float)((i % 1000) + 1);
        vy[i] = (float)(((i + 137) % 1000) + 1);
        vz[i] = (float)(((i + 613) % 1000) + 1);
    }

    size_t offset = (size_t)rank * (size_t)n_elem;
    size_t count  = (size_t)n_elem;

    double         t_open0 = MPI_Wtime();
    adios2::ADIOS  adios(MPI_COMM_WORLD);
    adios2::IO     rio    = adios.DeclareIO("VerifyMagnitude");
    adios2::Engine reader = rio.Open(out_file, adios2::Mode::ReadRandomAccess);
    auto           rmag   = rio.InquireVariable<float>("magnitude");
    MPI_Barrier(MPI_COMM_WORLD);
    double t_open1    = MPI_Wtime();
    double local_open = t_open1 - t_open0;
    double max_open;
    MPI_Reduce(&local_open, &max_open, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    int global_bad = 0;
    for (int step = 0; step < N_TIMESTEPS; ++step) {
        double t_read0 = MPI_Wtime();
        rmag.SetStepSelection({(size_t)step, 1});
        rmag.SetSelection({{offset}, {count}});
        std::fill(mag.begin(), mag.end(), 0.0f);
        reader.Get(rmag, mag.data(), adios2::Mode::Sync);
        MPI_Barrier(MPI_COMM_WORLD);
        double t_read1    = MPI_Wtime();
        double local_read = t_read1 - t_read0;
        double max_read;
        MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

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
            printf("adios2_verify,%d,%d,%ld,%.6f,%.6f,%d\n", step, nranks, n_elem, max_open, max_read,
                   step_bad);
            fflush(stdout);
        }
    }
    reader.Close();

    MPI_Finalize();
    return global_bad != 0;
}
