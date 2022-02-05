#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <inttypes.h>
#include "pdc.h"

#define NUM_DIMS 1
#define NUM_VARS 9

/* 9 variables
 * ------------------
xx   | float  | 4 bytes
yy   | float  | 4 bytes
zz   | float  | 4 bytes
vx   | float  | 4 bytes
vy   | float  | 4 bytes
vz   | float  | 4 bytes
phi  | float  | 4 bytes
pid  | int64  | 8 bytes
mask | int16  | 2 bytes
 * ------------------
 */

static char *         VAR_NAMES[NUM_VARS] = {"xx", "yy", "zz", "vx", "vy", "vz", "phi", "phd", "mask"};
static pdc_var_type_t VAR_TYPES[NUM_VARS] = {PDC_FLOAT, PDC_FLOAT, PDC_FLOAT, PDC_FLOAT, PDC_FLOAT,
                                             PDC_FLOAT, PDC_FLOAT, PDC_INT64, PDC_INT16};
static int            NUM_PARTICLES       = (1 * 1024 * 1024);
void *                buffers[NUM_VARS];

MPI_Comm comm;

double
uniform_random_number()
{
    return (((double)rand()) / ((double)(RAND_MAX)));
}

void
print_usage()
{
    printf("Usage: srun -n #procs ./haccio #particles (in 10e6)\n");
}

void
allocate_buffers()
{
    int i;
    // xx, yy, zz, vx, vy, vz, phi
    for (i = 0; i < 7; i++) {
        buffers[i] = malloc(NUM_PARTICLES * sizeof(float));
    }
    // phd
    buffers[7] = malloc(NUM_PARTICLES * sizeof(int64_t));
    // mask
    buffers[8] = malloc(NUM_PARTICLES * sizeof(int16_t));
}

pdcid_t
create_pdc_object(pdcid_t pdc_id, pdcid_t cont_id, const char *obj_name, pdc_var_type_t type,
                  pdcid_t *obj_prop)
{
    // Create and set the object property
    *obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    uint64_t dims[1];
    dims[0] = NUM_PARTICLES;
    PDCprop_set_obj_dims(*obj_prop, 1, dims);
    PDCprop_set_obj_type(*obj_prop, type);
    PDCprop_set_obj_time_step(*obj_prop, 0);
    PDCprop_set_obj_user_id(*obj_prop, getuid());
    PDCprop_set_obj_app_name(*obj_prop, "HACCIO");

    pdcid_t obj_id = PDCobj_create_mpi(cont_id, obj_name, *obj_prop, 0, comm);
    if (obj_id == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
        exit(-1);
    }

    return obj_id;
}

int
main(int argc, char **argv)
{
    int mpi_rank, mpi_size;
    int i;

    pdcid_t pdc_id, cont_prop, cont_id;

    pdcid_t obj_ids[NUM_VARS], obj_props[NUM_VARS];
    pdcid_t region_ids[NUM_VARS], region_remote_ids[NUM_VARS];

    perr_t ret;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);

    if (mpi_rank == 0) {
        NUM_PARTICLES = atoi(argv[1]);
        printf("particles: %d\n", NUM_PARTICLES);
    }
    MPI_Bcast(&NUM_PARTICLES, 1, MPI_INT, 0, MPI_COMM_WORLD);

    allocate_buffers();

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);

    // create a container
    cont_id = PDCcont_create_col("c1", cont_prop);

    uint64_t offset = 0, offset_remote = mpi_rank * NUM_PARTICLES, mysize = NUM_PARTICLES;

    for (i = 0; i < NUM_VARS; i++) {
        // Craete object
        obj_ids[i] = create_pdc_object(pdc_id, cont_id, VAR_NAMES[i], VAR_TYPES[i], &obj_props[i]);

        // Create region
        region_ids[i]        = PDCregion_create(NUM_DIMS, &offset, &mysize);
        region_remote_ids[i] = PDCregion_create(NUM_DIMS, &offset_remote, &mysize);

        PDCbuf_obj_map(buffers[i], VAR_TYPES[i], region_ids[i], obj_ids[i], region_remote_ids[i]);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    double time_total, time_lock, time_io, time_release;

    time_total = MPI_Wtime();

    // Accquire the lock
    time_lock = MPI_Wtime();
    for (i = 0; i < NUM_VARS; i++) {
        ret = PDCreg_obtain_lock(obj_ids[i], region_remote_ids[i], PDC_WRITE, PDC_NOBLOCK);
        if (ret != SUCCEED) {
            printf("Fail to obtain lock @ line  %d!\n", __LINE__);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    time_lock = MPI_Wtime() - time_lock;

    // Actual I/O
    time_io = MPI_Wtime();
    /*
    float *  float_var; // xx, yy, zz, vx, vy ,vz, phi
    int64_t *int64_var; // phd
    int16_t *int16_var; // mask
    for (k = 0; k < 7; k++) {
        float_var = (float *)buffers[k];
        for (i = 0; i < NUM_PARTICLES; i++)
            float_var[i] = uniform_random_number() * 99;
    }
    int64_var = (int64_t *)buffers[7];
    for (i = 0; i < NUM_PARTICLES; i++)
        int64_var[i] = uniform_random_number() * 99;
    int16_var = (int16_t *)buffers[8];
    for (i = 0; i < NUM_PARTICLES; i++)
        int16_var[i] = uniform_random_number() * 99;
    MPI_Barrier(MPI_COMM_WORLD);
    */
    time_io = MPI_Wtime() - time_io;

    // Release lock
    time_release = MPI_Wtime();
    for (i = 0; i < NUM_VARS; i++) {
        ret = PDCreg_release_lock(obj_ids[i], region_remote_ids[i], PDC_WRITE);
        if (ret != SUCCEED) {
            printf("Fail to release lock @ line  %d!\n", __LINE__);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    time_release = MPI_Wtime() - time_release;

    // Unmap objects
    for (i = 0; i < NUM_VARS; i++) {
        ret = PDCbuf_obj_unmap(obj_ids[i], region_remote_ids[i]);
        if (ret != SUCCEED) {
            printf("Fail to unmap @ line  %d!\n", __LINE__);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    time_total = MPI_Wtime() - time_total;

    for (i = 0; i < NUM_VARS; i++) {
        // TODO delete before close ?
        PDCobj_close(obj_ids[i]);
        PDCprop_close(obj_props[i]);
        PDCregion_close(region_ids[i]);
        PDCregion_close(region_remote_ids[i]);
        free(buffers[i]);
    }

#ifdef PDC_TIMING
    PDC_timing_report("write");
#endif

    PDCcont_close(cont_id);
    PDCprop_close(cont_prop);
    PDCclose(pdc_id);

    if (mpi_rank == 0) {
        double per_particle = sizeof(float) * 7 + sizeof(int64_t) + sizeof(int16_t);
        double bandwidth    = per_particle * NUM_PARTICLES / 1024.0 / 1024.0 / time_total * mpi_size;
        printf("Bandwidth: %.2fMB/s, total time: %.4f, lock: %.4f, io: %.4f, release: %.4f\n", bandwidth,
               time_total, time_lock, time_io, time_release);
    }
    MPI_Finalize();

    return 0;
}
