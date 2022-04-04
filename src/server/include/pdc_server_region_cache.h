#ifndef PDC_REGION_CACHE_H
#define PDC_REGION_CACHE_H

#include <sys/time.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "pdc_region.h"
#include "pdc_client_server_common.h"

#define PDC_SERVER_CACHE

#ifdef PDC_SERVER_CACHE

#define PDC_REGION_CONTAINED       0
#define PDC_REGION_CONTAINED_BY    1
#define PDC_REGION_PARTIAL_OVERLAP 2
#define PDC_REGION_NO_OVERLAP      3
#define PDC_MERGE_FAILED           4
#define PDC_MERGE_SUCCESS          5

pthread_mutex_t pdc_obj_cache_list_mutex;

int   PDC_region_server_cache_init();
int   PDC_region_server_cache_finalize();
int   PDC_region_cache_flush_all();
int   PDC_region_cache_flush(uint64_t obj_id);
int   PDC_region_fetch(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                       struct pdc_region_info *region_info, void *buf, size_t unit);
int   PDC_region_cache_register(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims, const char *buf,
                                size_t buf_size, const uint64_t *offset, const uint64_t *size, int ndim,
                                size_t unit);
void *PDC_region_cache_clock_cycle(void *ptr);

perr_t PDC_transfer_request_data_read_from(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                           struct pdc_region_info *region_info, void *buf, size_t unit);
perr_t PDC_transfer_request_data_write_out(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                           struct pdc_region_info *region_info, void *buf, size_t unit);

#endif

#endif
