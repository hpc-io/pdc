#ifndef PDC_TIMING_H
#define PDC_TIMING_H

#define PDC_TIMING 1
#if PDC_TIMING == 1

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int PDC_timing_init() {
    memset(&timings, 0, sizeof(pdc_timing));
}


int PDC_timing_report() {
    pdc_timing max_timings;
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Reduce(&timings, &max_timings, sizeof(pdc_timing)/sizeof(double), MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        printf("PDCbuf_obj_map_rpc = %lf, wait = %lf\n", max_timings.PDCbuf_obj_map_rpc, max_timings.PDCbuf_obj_map_rpc_wait);
        printf("PDCreg_obtain_lock_rpc = %lf, wait = %lf\n", max_timings.PDCreg_obtain_lock_rpc, max_timings.PDCreg_obtain_lock_rpc_wait);
        printf("PDCreg_release_lock_rpc = %lf, wait = %lf\n", max_timings.PDCreg_release_lock_rpc, max_timings.PDCreg_release_lock_rpc_wait);
        printf("PDCbuf_obj_unmap_rpc = %lf, wait = %lf\n", max_timings.PDCbuf_obj_unmap_rpc, max_timings.PDCbuf_obj_unmap_rpc_wait);
    }
    //free(timings);
}

int PDC_server_timing_init() {
    server_timings = calloc(1, sizeof(pdc_server_timing));
    buf_obj_map_timestamps = calloc(4, sizeof(pdc_server_timestamp));
    buf_obj_unmap_timestamps = buf_obj_map_timestamps + 1;
    obtain_lock_timestamps = buf_obj_map_timestamps + 2;
    release_lock_timestamps = buf_obj_map_timestamps + 3;
    buf_obj_map_timestamps->timestamp_size = 0;
    buf_obj_map_timestamps->timestamp_max_size = 0;
    buf_obj_unmap_timestamps->timestamp_size = 0;
    buf_obj_unmap_timestamps->timestamp_max_size = 0;
    obtain_lock_timestamps->timestamp_size = 0;
    obtain_lock_timestamps->timestamp_max_size = 0;
    release_lock_timestamps->timestamp_size = 0;
    release_lock_timestamps->timestamp_max_size = 0;
    base_time = MPI_Wtime();
}

int PDC_server_timestamp_register(pdc_server_timestamp *timestamp, double start, double end) {
    double *temp;
    start -= base_time;
    end -= base_time;
    if (timestamp->timestamp_max_size == 0) {
        timestamp->timestamp_max_size = 256;
        timestamp->start = (double*) malloc(sizeof(double) * timestamp->timestamp_max_size * 2);
        timestamp->end = timestamp->start + timestamp->timestamp_max_size;
    } else if (timestamp->timestamp_size == timestamp->timestamp_max_size) {
        temp = (double *) malloc(sizeof(double) * timestamp->timestamp_max_size * 4);
        memcpy(temp, timestamp->start, sizeof(double) * timestamp->timestamp_max_size * 2);
        timestamp->timestamp_max_size *= 2;
        timestamp->start = temp;
        timestamp->end = temp + timestamp->timestamp_max_size;
    }
    timestamp->start[timestamp->timestamp_size] = start;
    timestamp->end[timestamp->timestamp_size] = end;
    timestamp->timestamp_size += 1;
    return 0;
}

int PDC_server_timestamp_clean(pdc_server_timestamp *timestamp) {
    if (timestamp->timestamp_max_size) {
        free(timestamp->start);
    }
    return 0;
}

int server_log(FILE *stream, char* header, pdc_server_timestamp *timestamp) {
    size_t i;
    fprintf(stream, "%s" ,header);
    for ( i = 0; i < timestamp->timestamp_size; ++i ) {
        fprintf(stream, ",%4f-%4f", timestamp->start[i], timestamp->end[i]);
    }
    fprintf(stream, "\n");
}

int PDC_server_timing_report() {
    pdc_server_timing max_timings;
    int rank;
    char filename[256];
    FILE *stream;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    printf("rank = %d, PDCbuf_obj_map_rpc = %lf, PDCreg_obtain_lock_rpc = %lf, PDCreg_release_lock_rpc = %lf, PDCbuf_obj_unmap_rpc = %lf\n", rank, server_timings->PDCbuf_obj_map_rpc, server_timings->PDCreg_obtain_lock_rpc, server_timings->PDCreg_release_lock_rpc, server_timings->PDCbuf_obj_unmap_rpc);

    MPI_Reduce(server_timings, &max_timings, sizeof(pdc_server_timing)/sizeof(double), MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        printf("rank = %d, maximum timing among all processes, PDCbuf_obj_map_rpc = %lf, PDCreg_obtain_lock_rpc = %lf, PDCreg_release_lock_rpc = %lf, PDCbuf_obj_unmap_rpc = %lf\n", rank, max_timings.PDCbuf_obj_map_rpc, max_timings.PDCreg_obtain_lock_rpc, max_timings.PDCreg_release_lock_rpc, max_timings.PDCbuf_obj_unmap_rpc);
    }

    sprintf(filename, "pdc_server_log_rank_%d.csv", rank);
    stream = fopen(filename,"w");
    server_log(stream, "buf_obj_map", buf_obj_map_timestamps);
    server_log(stream, "buf_obj_unmap", buf_obj_unmap_timestamps);
    server_log(stream, "obtain_lock", obtain_lock_timestamps);
    server_log(stream, "release_lock", release_lock_timestamps);
    fclose(stream);

    free(server_timings);
    PDC_server_timestamp_clean(buf_obj_map_timestamps);
    PDC_server_timestamp_clean(buf_obj_unmap_timestamps);
    PDC_server_timestamp_clean(obtain_lock_timestamps);
    PDC_server_timestamp_clean(release_lock_timestamps);
    free(buf_obj_map_timestamps);
}

#endif

#endif
