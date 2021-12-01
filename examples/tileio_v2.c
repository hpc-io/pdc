#include "pdc.h"
#include <assert.h>
#include <getopt.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define NUM_DIMS 2

MPI_Comm g_mpi_comm;
int      g_mpi_rank, g_mpi_size;
int      g_coords[2]; // my coordinates in 2D cartesian topology

// number of tiles per row and column
int g_x_tiles, g_y_tiles;
// number of elements per row and column *in each tile*.
int g_x_ept, g_y_ept;

void
print_usage()
{
    printf("Usage: srun -N #nodes -n #procs ./tilio #x_tils #y_tiles "
           "#num_elements_x #num_elements_y\n");
    printf("\tnote: #procs should equal to x_tiles*y_tiles\n");
}

pdcid_t
create_pdc_object(pdcid_t pdc_id, pdcid_t cont_id, const char *obj_name, pdcid_t *obj_prop)
{
    // Create and set the object property
    *obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    uint64_t dims[NUM_DIMS] = {g_x_ept, g_y_ept};
    PDCprop_set_obj_dims(*obj_prop, NUM_DIMS, dims);
    PDCprop_set_obj_type(*obj_prop, PDC_DOUBLE);
    PDCprop_set_obj_time_step(*obj_prop, 0);
    PDCprop_set_obj_user_id(*obj_prop, getuid());
    PDCprop_set_obj_app_name(*obj_prop, "mpi-tile-io");
    PDCprop_set_obj_tags(*obj_prop, "tag0=1");

    pdcid_t obj_id = PDCobj_create_mpi(cont_id, obj_name, *obj_prop, 0, g_mpi_comm);
    if (obj_id == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
        exit(-1);
    }

    return obj_id;
}

void
init(int argc, char **argv)
{

    MPI_Comm_size(MPI_COMM_WORLD, &g_mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);

    if (g_mpi_rank == 0 && argc != 5) {
        print_usage();
        exit(1);
    }

    if (g_mpi_rank == 0) {
        g_x_tiles = atoi(argv[1]);
        g_y_tiles = atoi(argv[2]);
        g_x_ept   = atoi(argv[3]);
        g_y_ept   = atoi(argv[4]);
    }

    MPI_Bcast(&g_x_tiles, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&g_y_tiles, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&g_x_ept, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&g_y_ept, 1, MPI_INT, 0, MPI_COMM_WORLD);

    assert((g_x_tiles * g_y_tiles) == g_mpi_size);

    // Create a 2D Cartesian topology accorindg to x-y tiles
    int dims[NUM_DIMS]    = {g_x_tiles, g_y_tiles};
    int periods[NUM_DIMS] = {1, 1};
    MPI_Cart_create(MPI_COMM_WORLD, NUM_DIMS, dims, periods, 1, &g_mpi_comm);

    MPI_Comm_rank(g_mpi_comm, &g_mpi_rank);
    MPI_Cart_coords(g_mpi_comm, g_mpi_rank, NUM_DIMS, g_coords);
    // printf("my 2d rank: %d, coords: (%d, %d)\n", g_mpi_rank, g_coords[0], g_coords[1]);
}

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    init(argc, argv);

    perr_t  ret;
    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t obj_id, obj_prop;
    pdcid_t local_region_id, global_region_id;
    pdcid_t transfer_id;

    double *local_buffer = (double *)malloc(g_x_ept * g_y_ept * sizeof(double));

    uint64_t local_length[1]          = {g_x_ept * g_y_ept};
    uint64_t local_offsets[1]         = {0};
    uint64_t global_length[NUM_DIMS]  = {g_x_ept, g_y_ept};
    uint64_t global_offsets[NUM_DIMS] = {g_coords[0] * g_x_ept, g_coords[1] * g_y_ept};

    // Init PDC
    pdc_id    = PDCinit("pdc");
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    cont_id   = PDCcont_create_col("c1", cont_prop);
    obj_id    = create_pdc_object(pdc_id, cont_id, "tile_io_obj", &obj_prop);

    // Create region
    local_region_id  = PDCregion_create(1, local_offsets, local_length);
    global_region_id = PDCregion_create(NUM_DIMS, global_offsets, global_length);
    MPI_Barrier(MPI_COMM_WORLD);

    double time_total, time_create, time_start, time_wait;
    time_total = MPI_Wtime();

    // Create transfer request
    time_create = MPI_Wtime();
    transfer_id =
        PDCregion_transfer_create(local_buffer, PDC_WRITE, obj_id, local_region_id, global_region_id);
    MPI_Barrier(MPI_COMM_WORLD);
    time_create = MPI_Wtime() - time_create;

    // Start the transfer
    time_start = MPI_Wtime();
    PDCregion_transfer_start(transfer_id);
    MPI_Barrier(MPI_COMM_WORLD);
    time_start = MPI_Wtime() - time_start;

    // Wait for transfer to finish
    time_wait = MPI_Wtime();
    PDCregion_transfer_wait(transfer_id);
    MPI_Barrier(MPI_COMM_WORLD);
    time_wait  = MPI_Wtime() - time_wait;
    time_total = MPI_Wtime() - time_total;
    PDCregion_transfer_close(transfer_id);

    // TODO delete before close ?
    PDCobj_close(obj_id);
    PDCprop_close(obj_prop);
    PDCregion_close(local_region_id);
    PDCregion_close(global_region_id);
    PDCcont_close(cont_id);
    PDCprop_close(cont_prop);
    PDCclose(pdc_id);
    free(local_buffer);

    if (g_mpi_rank == 0) {
        double bandwidth =
            g_x_tiles * g_y_tiles * g_x_ept * g_y_ept / 1024.0 / 1024.0 * sizeof(double) / time_total;
        printf("Bandwidth: %.2fMB/s, total time: %.4f, transfer create: %.4f, transfer start: %.4f, "
               "transfert wait: %.4f\n",
               bandwidth, time_total, time_create, time_start, time_wait);
    }

    MPI_Comm_free(&g_mpi_comm);
    MPI_Finalize();

    return 0;
}
