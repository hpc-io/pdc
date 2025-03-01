#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

#include "mpi.h"
#include "pdc.h"

/**
 * write -> read -> wait_all()
 *
 */

int      mpi_rank, mpi_size;
MPI_Comm mpi_comm;

void
write_read_wait_all(pdcid_t obj_id, int iterations)
{
    pdcid_t region_local, region_remote;
    pdcid_t transfer_request;
    perr_t  ret;

    int      ndim          = 1;
    uint64_t offset_local  = 0;
    uint64_t offset_remote = 0;
    uint64_t chunk_size    = 2880;

    char *data_out = (char *)malloc(chunk_size * sizeof(char));
    memset(data_out, 'a', chunk_size * sizeof(char));

    double stime = MPI_Wtime();

    pdcid_t *tids = (pdcid_t *)malloc(sizeof(pdcid_t) * (iterations));
    for (int i = 0; i < iterations; i++) {
        region_local  = PDCregion_create(ndim, &offset_local, &chunk_size);
        region_remote = PDCregion_create(ndim, &offset_remote, &chunk_size);
        offset_remote += chunk_size;
        tids[i] = PDCregion_transfer_create(data_out, PDC_WRITE, obj_id, region_local, region_remote);
        if (tids[i] == 0)
            printf("transfer request creation failed\n");
        /* ret = PDCregion_transfer_start(tids[i]); */
        /* if (ret != SUCCEED) */
        /*     printf("Failed to start transfer\n"); */
    }

    /* printf("rank %d call wait_all on tids.\n", mpi_rank); */
    fprintf(stderr, "Rank %4d: create took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    MPI_Barrier(MPI_COMM_WORLD);

    stime = MPI_Wtime();

    ret = PDCregion_transfer_start_all(tids, iterations);
    if (ret != SUCCEED)
        printf("Failed to start transfer\n");

    fprintf(stderr, "Rank %4d: start all tids took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    stime = MPI_Wtime();

    MPI_Barrier(MPI_COMM_WORLD);

    ret = PDCregion_transfer_wait_all(tids, iterations);
    if (ret != SUCCEED)
        printf("Failed to wait all transfer\n");

    /* printf("rank %d read before wait_all()\n", mpi_rank); */
    fprintf(stderr, "Rank %4d: wait all tids took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    MPI_Barrier(MPI_COMM_WORLD);

    char *data_in    = (char *)malloc(chunk_size * sizeof(char));
    offset_local     = 0;
    offset_remote    = 0;
    region_local     = PDCregion_create(ndim, &offset_local, &chunk_size);
    region_remote    = PDCregion_create(ndim, &offset_remote, &chunk_size);
    pdcid_t read_tid = PDCregion_transfer_create(data_in, PDC_READ, obj_id, region_local, region_remote);
    ret              = PDCregion_transfer_start(read_tid);
    ret              = PDCregion_transfer_wait(read_tid);
    ret              = PDCregion_transfer_close(read_tid);
    /* printf("rank %d read success!\n", mpi_rank); */
    fprintf(stderr, "Rank %4d: create wait read took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    // Write more
    stime          = MPI_Wtime();
    int      N     = 10;
    pdcid_t *tids2 = (pdcid_t *)malloc(sizeof(pdcid_t) * N);
    offset_local   = 0;
    offset_remote  = iterations * chunk_size; // start from the end of the last write
    for (int i = 0; i < N; i++) {
        region_local  = PDCregion_create(ndim, &offset_local, &chunk_size);
        region_remote = PDCregion_create(ndim, &offset_remote, &chunk_size);
        offset_remote += chunk_size;
        tids2[i] = PDCregion_transfer_create(data_out, PDC_WRITE, obj_id, region_local, region_remote);
        if (tids2[i] == 0)
            printf("transfer request creation failed\n");
        /* ret = PDCregion_transfer_start(tids2[i]); */
        /* if (ret != SUCCEED) */
        /*     printf("Failed to start transfer\n"); */
    }
    fprintf(stderr, "Rank %4d: create tids2 took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();

    ret = PDCregion_transfer_start_all(tids2, N);
    if (ret != SUCCEED)
        printf("Failed to start transfer\n");

    fprintf(stderr, "Rank %4d: start tids2 took %.6f\n", mpi_rank, MPI_Wtime() - stime);
    /* ret = PDCregion_transfer_wait_all(tids, (iterations)); */
    /* if (ret != SUCCEED) */
    /*     printf("Failed to transfer wait\n"); */

    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
    /* printf("rank %d call wait_all on tids2.\n", mpi_rank); */
    ret = PDCregion_transfer_wait_all(tids2, (N));
    if (ret != SUCCEED)
        printf("Failed to transfer wait\n");

    fprintf(stderr, "Rank %4d: wait all tids2 took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();

    for (int i = 0; i < iterations; i++) {
        ret = PDCregion_transfer_close(tids[i]);
        if (ret != SUCCEED)
            printf("region transfer close failed\n");
    }
    for (int i = 0; i < N; i++) {
        ret = PDCregion_transfer_close(tids2[i]);
        if (ret != SUCCEED)
            printf("region transfer close failed\n");
    }

    fprintf(stderr, "Rank %4d: close all took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    free(data_in);
    free(data_out);
    free(tids);
    free(tids2);
}

int
main(int argc, char **argv)
{

    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t obj_prop, obj_id;

    uint64_t dims[1] = {PDC_SIZE_UNLIMITED};

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_dup(MPI_COMM_WORLD, &mpi_comm);

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
    }
    // create a container
    cont_id = PDCcont_create_col("c1", cont_prop);
    if (cont_id <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
    }

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_type(obj_prop, PDC_CHAR);
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_app_name(obj_prop, "producer");
    PDCprop_set_obj_transfer_region_type(obj_prop, PDC_REGION_LOCAL);

    char obj_name[100] = {0};
    sprintf(obj_name, "obj-var-%d", mpi_rank);
    PDCprop_set_obj_tags(obj_prop, obj_name);
    obj_id = PDCobj_create(cont_id, obj_name, obj_prop);

    MPI_Barrier(MPI_COMM_WORLD);
    double stime = MPI_Wtime();
    write_read_wait_all(obj_id, 1000);

    MPI_Barrier(MPI_COMM_WORLD);
    fprintf(stderr, "Rank %4d: Write read wait all took %.6f\n", mpi_rank, MPI_Wtime() - stime);

    if (PDCobj_close(obj_id) < 0) {
        printf("fail to close obj_id\n");
    }

    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
    }

    if (PDCclose(pdc_id) < 0) {
        printf("fail to close PDC\n");
    }

    MPI_Finalize();
}
