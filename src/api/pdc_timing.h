#ifndef PDC_TIMING_H
#define PDC_TIMING_H

#define PDC_TIMING 0
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
    double PDCreg_release_lock_bulk_transfer_rpc;
} pdc_server_timing;

typedef struct pdc_timestamp {
    double *start;
    double *end;
    size_t  timestamp_max_size;
    size_t  timestamp_size;
} pdc_timestamp;

pdc_server_timing *server_timings;
pdc_timestamp *    buf_obj_map_timestamps;
pdc_timestamp *    buf_obj_unmap_timestamps;
pdc_timestamp *    obtain_lock_timestamps;
pdc_timestamp *    release_lock_timestamps;
pdc_timestamp *    release_lock_bulk_transfer_timestamps;

pdc_timestamp *client_buf_obj_map_timestamps;
pdc_timestamp *client_buf_obj_unmap_timestamps;
pdc_timestamp *client_obtain_lock_timestamps;
pdc_timestamp *client_release_lock_timestamps;
double         base_time;

int PDC_timing_init();
int PDC_timing_finalize();
int PDC_timing_report(const char *prefix);
int PDC_server_timing_init();
int pdc_timestamp_register(pdc_timestamp *timestamp, double start, double end);
int PDC_server_timing_report();

#endif

#endif
