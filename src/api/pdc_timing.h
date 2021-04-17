#ifndef PDC_TIMING_H
#define PDC_TIMING_H

#define PDC_TIMING 1
#if PDC_TIMING == 1

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct pdc_timing {
    double PDCbuf_obj_map_rpc;
    double PDCbuf_obj_unmap_rpc;
    double PDCreg_obtain_lock_rpc;
    double PDCreg_release_lock_rpc;

    double PDCbuf_obj_map_rpc_wait;
    double PDCbuf_obj_unmap_rpc_wait;
    double PDCreg_obtain_lock_rpc_wait;
    double PDCreg_release_lock_rpc_wait;
} pdc_timing;

pdc_timing timings;


typedef struct pdc_server_timing {
    double PDCbuf_obj_map_rpc;
    double PDCbuf_obj_unmap_rpc;
    double PDCreg_obtain_lock_rpc;
    double PDCreg_release_lock_rpc;
} pdc_server_timing;

typedef struct pdc_server_timestamp {
    double *start;
    double *end;
    size_t timestamp_max_size;
    size_t timestamp_size;
} pdc_server_timestamp;

pdc_server_timing *server_timings;
pdc_server_timestamp *buf_obj_map_timestamps;
pdc_server_timestamp *buf_obj_unmap_timestamps;
pdc_server_timestamp *obtain_lock_timestamps;
pdc_server_timestamp *release_lock_timestamps;
double base_time;

#endif

#endif
