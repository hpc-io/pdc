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
static int NUM_PARTICLES = (1 * 1024 * 1024);

MPI_Comm comm;

double
uniform_random_number()
{
    return (((double)rand()) / ((double)(RAND_MAX)));
}

void
print_usage()
{
    printf("Usage: srun -n ./vpicio #particles\n");
}

pdcid_t
create_pdc_object(pdcid_t pdc_id, pdcid_t cont_id, const char *obj_name, pdcid_t *obj_prop)
{
    // Create and set the object property
    *obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    uint64_t dims[1];
    dims[0] = NUM_PARTICLES;
    PDCprop_set_obj_dims(*obj_prop, 1, dims);
    PDCprop_set_obj_type(*obj_prop, PDC_FLOAT);
    PDCprop_set_obj_time_step(*obj_prop, 0);
    PDCprop_set_obj_user_id(*obj_prop, getuid());
    PDCprop_set_obj_app_name(*obj_prop, "VPICIO");
    PDCprop_set_obj_tags(*obj_prop, "tag0=1");

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
    int i, k;

    pdcid_t pdc_id, cont_prop, cont_id;

    pdcid_t obj_ids[NUM_VARS], obj_props[NUM_VARS];
    pdcid_t region_ids[NUM_VARS], region_remote_ids[NUM_VARS];

    perr_t ret;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);

    NUM_PARTICLES = atoi(argv[1]) * 1000000; // M as unit
    printf("particles: %d\n", NUM_PARTICLES);

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);

    // create a container
    cont_id = PDCcont_create_col("c1", cont_prop);

    float *  buffers[NUM_VARS];
    uint64_t offset = 0, offset_remote = mpi_rank * NUM_PARTICLES, mysize = NUM_PARTICLES;

    for (i = 0; i < NUM_VARS; i++) {
        char obj_name[10];
        sprintf(obj_name, "obj_%d", i);
        // Craete object
        obj_ids[i] = create_pdc_object(pdc_id, cont_id, obj_name, &obj_props[i]);

        // Create region
        region_ids[i]        = PDCregion_create(NUM_DIMS, &offset, &mysize);
        region_remote_ids[i] = PDCregion_create(NUM_DIMS, &offset_remote, &mysize);

        // Map local memory
        buffers[i] = (float *)malloc(NUM_PARTICLES * sizeof(float));
        PDCbuf_obj_map(buffers[i], PDC_FLOAT, region_ids[i], obj_ids[i], region_remote_ids[i]);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    double t1, t2;

    // Accquire the lock
    t1 = MPI_Wtime();
    for (i = 0; i < NUM_VARS; i++) {
        ret = PDCreg_obtain_lock(obj_ids[i], region_remote_ids[i], PDC_WRITE, PDC_NOBLOCK);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime();
    if (mpi_rank == 0)
        printf("accquire time: %.2fs\n", t2 - t1);

    // Actual I/O
    t1 = MPI_Wtime();
    for (k = 0; k < NUM_VARS; k++) {
        for (i = 0; i < NUM_PARTICLES; i++)
            buffers[k][i] = uniform_random_number() * 99;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime();
    if (mpi_rank == 0)
        printf("I/O time: %.2fs\n", t2 - t1);

    // Release lock
    t1 = MPI_Wtime();
    for (i = 0; i < NUM_VARS; i++)
        ret = PDCreg_release_lock(obj_ids[i], region_remote_ids[i], PDC_WRITE);
    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime();
    if (mpi_rank == 0)
        printf("release lock time: %.2fs\n", t2 - t1);

    // Unmap objects
    for (i = 0; i < NUM_VARS; i++)
        ret = PDCbuf_obj_unmap(obj_ids[i], region_remote_ids[i]);
    MPI_Barrier(MPI_COMM_WORLD);

    // Close everthing
    if (mpi_rank == 0)
        PDCcont_del_objids(cont_id, NUM_VARS, obj_ids);
    for (i = 0; i < NUM_VARS; i++) {
        // TODO delete before close ?
        PDCobj_close(obj_ids[i]);
        PDCprop_close(obj_props[i]);
        PDCregion_close(region_ids[i]);
        PDCregion_close(region_remote_ids[i]);
        free(buffers[i]);
    }

    PDCcont_close(cont_id);
    PDCprop_close(cont_prop);
    PDCclose(pdc_id);

    MPI_Finalize();

    return 0;
}
