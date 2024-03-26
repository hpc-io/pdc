#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont, obj_prop, obj, local_reg, remote_reg, transfer_req, *transfer_batch;
    int     i, j, r, count, total_count, rank, nproc, ssi_downsample, rec_downsample, batchsize, iter,
        opensees_size, round = 1;
    int      start_x[4096], start_y[4096];
    uint64_t pdc_dims[3], pdc_offset[3], pdc_size[3], pdc_local_offset[3], pdc_local_size[3];
    uint32_t value_size;
    // 12x, 32x, 32x
    char *   fname, tag_name[128];
    uint64_t dims[4] = {4634, 19201, 12801, 1}, chunk_size[4] = {400, 600, 400, 1};
    double * data = NULL, t0, t1, t2, data_max, data_min, *ssi_data = NULL, *rec_data = NULL,
           *opensees_data = NULL, *tag_value = NULL;
    pdc_var_type_t value_type;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);
#endif

    if (argc > 1)
        round = atoi(argv[1]);

    // Assign chunks to each rank
    count = 0;
    for (i = 0; i < dims[1] / chunk_size[1]; i++) {
        for (j = 0; j < dims[2] / chunk_size[2]; j++) {
            start_x[count] = i;
            start_y[count] = j;
            count++;
        }
    }

    pdc = PDCinit("pdc");

    obj = PDCobj_open_col("run1", pdc);
    if (obj <= 0)
        fprintf(stderr, "Fail to open object @ line  %d!\n", __LINE__);

    /*     //=============PATTERN 1=============== */
    /*     // Read everything */
    /*     transfer_req = PDCregion_transfer_create(data, PDC_READ, obj, local_reg, remote_reg); */
    /*     PDCregion_transfer_start(transfer_req); */
    /*     PDCregion_transfer_wait(transfer_req); */
    /*     PDCregion_transfer_close(transfer_req); */

    /* #ifdef ENABLE_MPI */
    /*     MPI_Barrier(MPI_COMM_WORLD); */
    /*     t1 = MPI_Wtime(); */
    /*     if (rank == 0) */
    /*         fprintf(stderr, "Read whole chunk data from server took %.6lf\n", t1 - t0); */
    /* #endif */

    /*     data_max = -1; */
    /*     data_min = 1; */
    /*     for (i = 0; i < pdc_local_size[0]*pdc_local_size[1]*pdc_local_size[2]; i++) { */
    /*         if (data[i] > data_max) */
    /*             data_max = data[i]; */
    /*         else if (data[i] < data_min) */
    /*             data_min = data[i]; */
    /*     } */
    /*     fprintf(stderr, "Rank %d, chunk min/max: %lf, %lf\n", rank, data_min, data_max); */

    /*     PDCregion_close(remote_reg); */
    /*     PDCregion_close(local_reg); */

    /* free(data); */

    //=============PATTERN 2===============
    // OpenSees access pattern: 1 rank read subregion 200x200m (32 grids)
    opensees_size       = 32; // 32 * 6.25m = 200m
    pdc_local_offset[0] = 0;
    pdc_local_offset[1] = 0;
    pdc_local_offset[2] = 0;
    pdc_local_size[0]   = dims[0];
    pdc_local_size[1]   = opensees_size;
    pdc_local_size[2]   = opensees_size;
    local_reg           = PDCregion_create(3, pdc_local_offset, pdc_local_size);

    pdc_offset[0] = 0;
    pdc_offset[1] = start_x[rank] * chunk_size[1];
    pdc_offset[2] = start_y[rank] * chunk_size[2];
    pdc_size[0]   = dims[0];
    pdc_size[1]   = opensees_size;
    pdc_size[2]   = opensees_size;
    remote_reg    = PDCregion_create(3, pdc_offset, pdc_size);

    if (nproc <= 16)
        fprintf(stderr, "Rank %d: offset %llu, %llu, %llu size %llu, %llu, %llu\n", rank, pdc_offset[0],
                pdc_offset[1], pdc_offset[2], pdc_size[0], pdc_size[1], pdc_size[2]);

    // Tag retrieval
    sprintf(tag_name, "%llu-%llu\n", pdc_offset[1], pdc_offset[2]);
    for (r = 0; r < round; r++) {
        tag_value = NULL;
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
#endif
        PDCobj_get_tag(obj, tag_name, (void **)&tag_value, &value_type, &value_size);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: tag retrival query took %.6lf\n", r, t1 - t0);
#endif
        if (value_size != 4 * sizeof(double))
            fprintf(stderr, "Error: Round %d: tag retrival result size %llu / %llu \n", r, value_size,
                    4 * sizeof(double));
        if (tag_value)
            free(tag_value);
    }

    if (rank == 0)
        opensees_data = (double *)malloc(sizeof(double) * dims[0] * opensees_size * opensees_size);

    for (r = 0; r < round; r++) {
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
#endif

        if (rank == 0) {
            transfer_req = PDCregion_transfer_create(opensees_data, PDC_READ, obj, local_reg, remote_reg);
            PDCregion_transfer_start(transfer_req);
            PDCregion_transfer_wait(transfer_req);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: rank 0 read OpenSees 200x200m data from server took %.6lf\n", r,
                    t1 - t0);
#endif

        if (rank == 0)
            PDCregion_transfer_close(transfer_req);
    } // End for round

    if (rank == 0) {
        PDCregion_close(remote_reg);
        PDCregion_close(local_reg);
    }

    //=============PATTERN 3===============
    // Generating movie: all rank downsample in space to 156.25x156.25m (downsample factor 25)
    ssi_downsample      = 25;
    pdc_local_offset[0] = 0;
    pdc_local_offset[1] = 0;
    pdc_local_offset[2] = 0;
    pdc_local_size[0]   = dims[0];
    pdc_local_size[1]   = 1;
    pdc_local_size[2]   = 1;
    local_reg           = PDCregion_create(3, pdc_local_offset, pdc_local_size);

    pdc_size[0] = dims[0];
    pdc_size[1] = 1;
    pdc_size[2] = 1;

    batchsize      = chunk_size[1] * chunk_size[2] / ssi_downsample / ssi_downsample;
    transfer_batch = (pdcid_t *)malloc(sizeof(pdcid_t) * batchsize);
    ssi_data       = (double *)malloc(sizeof(double) * dims[0] * batchsize);

    for (r = 0; r < round; r++) {
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
#endif

        iter = 0;
        for (i = 0; i < chunk_size[1] / ssi_downsample; i++) {
            for (j = 0; j < chunk_size[2] / ssi_downsample; j++) {
                pdc_offset[0] = 0;
                pdc_offset[1] = i * ssi_downsample + start_x[rank] * chunk_size[1];
                pdc_offset[2] = j * ssi_downsample + start_y[rank] * chunk_size[2];
                remote_reg    = PDCregion_create(3, pdc_offset, pdc_size);

                transfer_batch[iter] = PDCregion_transfer_create(&ssi_data[dims[0] * iter], PDC_READ, obj,
                                                                 local_reg, remote_reg);
                PDCregion_close(remote_reg);
                iter++;
            }
        }

        PDCregion_transfer_start_all(transfer_batch, batchsize);
        PDCregion_transfer_wait_all(transfer_batch, batchsize);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: all ranks read ssi_downsample data from server took %.6lf\n", r,
                    t1 - t0);
#endif

        for (i = 0; i < batchsize; i++)
            PDCregion_transfer_close(transfer_batch[i]);
    } // End for round

    PDCregion_close(local_reg);
    free(ssi_data);

    //=============PATTERN 4===============
    // Building response: all rank downsample in space to every 1250m (downsample factor 200)
    rec_downsample      = 200;
    pdc_local_offset[0] = 0;
    pdc_local_offset[1] = 0;
    pdc_local_offset[2] = 0;
    pdc_local_size[0]   = dims[0];
    pdc_local_size[1]   = 1;
    pdc_local_size[2]   = 1;
    local_reg           = PDCregion_create(3, pdc_local_offset, pdc_local_size);

    pdc_size[0] = dims[0];
    pdc_size[1] = 1;
    pdc_size[2] = 1;

    batchsize      = chunk_size[1] * chunk_size[2] / rec_downsample / rec_downsample;
    transfer_batch = (pdcid_t *)malloc(sizeof(pdcid_t) * batchsize);
    rec_data       = (double *)malloc(sizeof(double) * dims[0] * batchsize);

    for (r = 0; r < round; r++) {
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
#endif

        iter = 0;
        for (i = 0; i < chunk_size[1] / rec_downsample; i++) {
            for (j = 0; j < chunk_size[2] / rec_downsample; j++) {
                pdc_offset[0] = 0;
                pdc_offset[1] = i * rec_downsample + start_x[rank] * chunk_size[1];
                pdc_offset[2] = j * rec_downsample + start_y[rank] * chunk_size[2];
                remote_reg    = PDCregion_create(3, pdc_offset, pdc_size);

                transfer_batch[iter] = PDCregion_transfer_create(&rec_data[dims[0] * iter], PDC_READ, obj,
                                                                 local_reg, remote_reg);
                PDCregion_close(remote_reg);
                iter++;
            }
        }

        PDCregion_transfer_start_all(transfer_batch, batchsize);
        PDCregion_transfer_wait_all(transfer_batch, batchsize);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: all ranks read rec_downsample data from server took %.6lf\n", r,
                    t1 - t0);
#endif

        for (i = 0; i < batchsize; i++)
            PDCregion_transfer_close(transfer_batch[i]);
    } // End for round

    PDCregion_close(local_reg);

    //=============PATTERN 5===============
    // Single rank singele time history access
    pdc_offset[0] = 0;
    pdc_offset[1] = chunk_size[1] / 2 + start_x[rank] * chunk_size[1];
    pdc_offset[2] = chunk_size[2] / 2 + start_y[rank] * chunk_size[2];
    pdc_size[0]   = dims[0];
    pdc_size[1]   = 1;
    pdc_size[2]   = 1;
    remote_reg    = PDCregion_create(3, pdc_offset, pdc_size);

    pdc_local_offset[0] = 0;
    pdc_local_offset[1] = 0;
    pdc_local_offset[2] = 0;
    pdc_local_size[0]   = dims[0];
    pdc_local_size[1]   = 1;
    pdc_local_size[2]   = 1;
    local_reg           = PDCregion_create(3, pdc_local_offset, pdc_local_size);

    for (r = 0; r < round; r++) {
#ifdef ENABLE_MPI
        t0 = MPI_Wtime();
#endif

        if (rank == 0) {
            transfer_req = PDCregion_transfer_create(rec_data, PDC_READ, obj, local_reg, remote_reg);
            PDCregion_transfer_start(transfer_req);
            PDCregion_transfer_wait(transfer_req);
            PDCregion_transfer_close(transfer_req);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            fprintf(stderr, "Round %d: rank 0 read single time series from server took %.6lf\n", r, t1 - t0);
#endif

        /* if (rank == 0) { */
        /*     data_max = -1; */
        /*     data_min = 1; */
        /*     for (i = 0; i < pdc_local_size[0]; i++) { */
        /*         if (rec_data[i] > data_max) */
        /*             data_max = rec_data[i]; */
        /*         else if (rec_data[i] < data_min) */
        /*             data_min = rec_data[i]; */
        /*     } */
        /*     fprintf(stderr, "Round %d: rank %d single series min/max: %lf, %lf\n", r, rank, data_min,
         * data_max); */
        /* } */

    } // End for round

    PDCregion_close(remote_reg);
    PDCregion_close(local_reg);

    free(rec_data);

    if (PDCobj_close(obj) < 0)
        fprintf(stderr, "fail to close object\n");

    if (PDCclose(pdc) < 0)
        fprintf(stderr, "fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
