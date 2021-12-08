#ifndef PDC_TIMING_H
#define PDC_TIMING_H

#define PDC_TIMING 0
#if PDC_TIMING == 1

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct pdc_timing {
    double PDCbuf_obj_map_rpc;
    double PDCbuf_obj_unmap_rpc;

    double PDCreg_obtain_lock_write_rpc;
    double PDCreg_obtain_lock_read_rpc;

    double PDCreg_release_lock_write_rpc;
    double PDCreg_release_lock_read_rpc;

    double PDCbuf_obj_map_rpc_wait;
    double PDCbuf_obj_unmap_rpc_wait;

    double PDCreg_obtain_lock_write_rpc_wait;
    double PDCreg_obtain_lock_read_rpc_wait;
    double PDCreg_release_lock_write_rpc_wait;
    double PDCreg_release_lock_read_rpc_wait;

    double PDCtransfer_request_start_write_rpc;
    double PDCtransfer_request_wait_write_rpc;
    double PDCtransfer_request_start_read_rpc;
    double PDCtransfer_request_wait_read_rpc;

    double PDCtransfer_request_start_write_rpc_wait;
    double PDCtransfer_request_start_read_rpc_wait;
    double PDCtransfer_request_wait_write_rpc_wait;
    double PDCtransfer_request_wait_read_rpc_wait;

} pdc_timing;

pdc_timing timings;

typedef struct pdc_server_timing {
    double PDCbuf_obj_map_rpc;
    double PDCbuf_obj_unmap_rpc;

    double PDCreg_obtain_lock_write_rpc;
    double PDCreg_obtain_lock_read_rpc;
    double PDCreg_release_lock_write_rpc;
    double PDCreg_release_lock_read_rpc;
    double PDCreg_release_lock_bulk_transfer_write_rpc;
    double PDCreg_release_lock_bulk_transfer_read_rpc;
    double PDCreg_release_lock_bulk_transfer_inner_write_rpc;
    double PDCreg_release_lock_bulk_transfer_inner_read_rpc;

    double PDCreg_transfer_request_start_write_rpc;
    double PDCreg_transfer_request_start_read_rpc;

    double PDCreg_transfer_request_wait_write_rpc;
    double PDCreg_transfer_request_wait_read_rpc;

    double PDCreg_transfer_request_wait_write_bulk_rpc;
    double PDCreg_transfer_request_inner_write_bulk_rpc;
    double PDCreg_transfer_request_wait_read_bulk_rpc;
    double PDCreg_transfer_request_inner_read_bulk_rpc;

    double PDCdata_server_write_out;
    double PDCdata_server_read_from;
    double PDCcache_write;
    double PDCcache_read;
    double PDCdata_server_write_posix;
    double PDCdata_server_read_posix;

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

pdc_timestamp *obtain_lock_write_timestamps;
pdc_timestamp *obtain_lock_read_timestamps;
pdc_timestamp *release_lock_write_timestamps;
pdc_timestamp *release_lock_read_timestamps;
pdc_timestamp *release_lock_bulk_transfer_write_timestamps;
pdc_timestamp *release_lock_bulk_transfer_inner_write_timestamps;
pdc_timestamp *release_lock_bulk_transfer_read_timestamps;
pdc_timestamp *release_lock_bulk_transfer_inner_read_timestamps;

pdc_timestamp *transfer_request_start_write_timestamps;
pdc_timestamp *transfer_request_start_read_timestamps;
pdc_timestamp *transfer_request_wait_write_timestamps;
pdc_timestamp *transfer_request_wait_read_timestamps;
pdc_timestamp *transfer_request_wait_write_bulk_timestamps;
pdc_timestamp *transfer_request_inner_write_bulk_timestamps;
pdc_timestamp *transfer_request_wait_read_bulk_timestamps;
pdc_timestamp *transfer_request_inner_read_bulk_timestamps;

pdc_timestamp *client_buf_obj_map_timestamps;
pdc_timestamp *client_buf_obj_unmap_timestamps;
pdc_timestamp *client_obtain_lock_write_timestamps;
pdc_timestamp *client_obtain_lock_read_timestamps;
pdc_timestamp *client_release_lock_write_timestamps;
pdc_timestamp *client_release_lock_read_timestamps;

pdc_timestamp *client_transfer_request_start_write_timestamps;
pdc_timestamp *client_transfer_request_start_read_timestamps;
pdc_timestamp *client_transfer_request_wait_write_timestamps;
pdc_timestamp *client_transfer_request_wait_read_timestamps;
double         base_time;

int PDC_timing_init();
int PDC_timing_finalize();
int PDC_timing_report(const char *prefix);
int PDC_server_timing_init();
int pdc_timestamp_register(pdc_timestamp *timestamp, double start, double end);
int PDC_server_timing_report();

#endif

#endif
