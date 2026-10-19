/**
 * Isolates the MPI collective communication pattern of bench_curl_eager's
 * object setup phase (container + per-timestep object creation) from
 * everything else PDC does there (Mercury RPCs, server-side I/O, region
 * cache flushing), so that pattern's own scaling can be measured on its
 * own, without a PDC server involved at all.
 *
 * The pattern reproduced here is read directly off the PDC client library,
 * not guessed:
 *
 *   - PDCcont_create_col (src/api/pdc_obj/pdc_cont.c) calls
 *     PDC_Client_create_cont_id_mpi (src/api/pdc_client_connect.c):
 *     rank 0 alone performs the actual RPC, then
 *       MPI_Bcast(&cont_id, 1, MPI_LONG_LONG, 0, comm)
 *     hands the resulting id to every other rank. Called once, before
 *     bench_curl_eager's per-timestep loop.
 *
 *   - PDCobj_create_mpi (src/api/pdc_obj/pdc_mpi.c), called once per
 *     object with rank_id=0 (matching bench_curl_eager.c's call sites):
 *     rank 0 performs the actual RPC, then four broadcasts hand the
 *     resulting metadata to every other rank:
 *       MPI_Bcast(&meta_id,             1, MPI_LONG_LONG,  0, comm)
 *       MPI_Bcast(&metadata_server_id,  1, MPI_UINT32_T,   0, comm)
 *       MPI_Bcast(&data_server_id,      1, MPI_UINT32_T,   0, comm)
 *       MPI_Bcast(&region_partition,    1, MPI_UINT8_T,    0, comm)
 *     bench_curl_eager.c creates 7 objects per timestep this way: u, v,
 *     w, curl_x, curl_y, curl_z, vorticity_magnitude.
 *
 * Every rank here calls PDC_obj_create/PDC_Client_create_cont_id in the
 * real code regardless of whether it ends up broadcasting or receiving
 * (only the RPC itself is conditional on rank==0) -- that RPC's own cost
 * cannot be reproduced without a real server, so it is deliberately left
 * out; only the MPI collectives that surround it are timed here.
 *
 * Usage: mpi_object_setup_scale [n_timesteps]   (default 3, matching
 * bench_curl_eager.c's N_TIMESTEPS)
 *
 * Prints, from rank 0, one CSV line for the one-time container-create
 * broadcast and one per timestep for the 7-object create/broadcast
 * batch, in the same style as bench_curl_eager.c's own CSV output:
 *   phase,step,n_ranks,setup_s
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <mpi.h>

#define N_OBJECTS_PER_TIMESTEP 7 /* u, v, w, curl_x, curl_y, curl_z, vorticity_magnitude */
#define ROOT_RANK              0 /* rank_id passed to PDCobj_create_mpi in bench_curl_eager.c */

int
main(int argc, char **argv)
{
    int rank, nranks;
    int n_timesteps = 3;

    if (argc > 1)
        n_timesteps = atoi(argv[1]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    /* --- container create: PDCcont_create_col, once --- */
    uint64_t cont_id = 0;

    MPI_Barrier(MPI_COMM_WORLD);
    double t_cont0 = MPI_Wtime();

    if (rank == ROOT_RANK)
        cont_id = 1; /* stand-in for the real RPC's returned meta_id */
    MPI_Bcast(&cont_id, 1, MPI_LONG_LONG, ROOT_RANK, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);
    double t_cont1 = MPI_Wtime();

    double local_cont = t_cont1 - t_cont0;
    double max_cont;
    MPI_Reduce(&local_cont, &max_cont, 1, MPI_DOUBLE, MPI_MAX, ROOT_RANK, MPI_COMM_WORLD);

    if (rank == ROOT_RANK) {
        printf("phase,step,n_ranks,setup_s\n");
        printf("cont_create,-,%d,%.6f\n", nranks, max_cont);
        fflush(stdout);
    }

    /* --- per-timestep object create: PDCobj_create_mpi x 7 --- */
    for (int step = 0; step < n_timesteps; step++) {
        MPI_Barrier(MPI_COMM_WORLD);
        double t0 = MPI_Wtime();

        for (int o = 0; o < N_OBJECTS_PER_TIMESTEP; o++) {
            uint64_t meta_id            = 0;
            uint32_t metadata_server_id = 0;
            uint32_t data_server_id     = 0;
            uint8_t  region_partition   = 0;

            if (rank == ROOT_RANK) {
                meta_id            = 1;
                metadata_server_id = 0;
                data_server_id     = 0;
                region_partition   = 0;
            }

            MPI_Bcast(&meta_id, 1, MPI_LONG_LONG, ROOT_RANK, MPI_COMM_WORLD);
            MPI_Bcast(&metadata_server_id, 1, MPI_UINT32_T, ROOT_RANK, MPI_COMM_WORLD);
            MPI_Bcast(&data_server_id, 1, MPI_UINT32_T, ROOT_RANK, MPI_COMM_WORLD);
            MPI_Bcast(&region_partition, 1, MPI_UINT8_T, ROOT_RANK, MPI_COMM_WORLD);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        double t1 = MPI_Wtime();

        double local_setup = t1 - t0;
        double max_setup;
        MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, ROOT_RANK, MPI_COMM_WORLD);

        if (rank == ROOT_RANK) {
            printf("object_create,%d,%d,%.6f\n", step, nranks, max_setup);
            fflush(stdout);
        }
    }

    MPI_Finalize();
    return 0;
}
