#ifndef PDC_REGION_CACHE_H
#define PDC_REGION_CACHE_H
#include "pdc_client_server_common.h"
#include "pdc_query.h"
#include <sys/time.h>
#include <pthread.h>

#define PDC_SERVER_CACHE

#ifdef PDC_SERVER_CACHE

typedef struct pdc_region_cache {
    struct pdc_region_info * region_cache_info;
    struct pdc_region_cache *next;
} pdc_region_cache;

typedef struct pdc_obj_cache {
    struct pdc_obj_cache *next;
    uint64_t              obj_id;
    int                   ndim;
    uint64_t *            dims;
    pdc_region_cache *    region_cache;
    pdc_region_cache *    region_cache_end;
    struct timeval        timestamp;
} pdc_obj_cache;

#define PDC_REGION_CONTAINED       0
#define PDC_REGION_CONTAINED_BY    1
#define PDC_REGION_PARTIAL_OVERLAP 2
#define PDC_REGION_NO_OVERLAP      3
#define PDC_MERGE_FAILED           4
#define PDC_MERGE_SUCCESS          5

pdc_obj_cache *obj_cache_list, *obj_cache_list_end;

hg_thread_mutex_t pdc_obj_cache_list_mutex;
pthread_t         pdc_recycle_thread;
pthread_mutex_t   pdc_cache_mutex;
int               pdc_recycle_close_flag;

int   PDC_region_cache_flush_all();
int   PDC_region_fetch(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                       struct pdc_region_info *region_info, void *buf, size_t unit);
int   PDC_region_cache_register(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims, const char *buf,
                                size_t buf_size, const uint64_t *offset, const uint64_t *size, int ndim,
                                size_t unit);
void *PDC_region_cache_clock_cycle(void *ptr);

perr_t PDC_Server_transfer_request_io(uint64_t obj_id, int obj_ndim, uint64_t *obj_dims,
                                      struct pdc_region_info *region_info, void *buf, size_t unit,
                                      int is_write);
perr_t PDC_transfer_request_data_read_from(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                           struct pdc_region_info *region_info, void *buf, size_t unit);
perr_t PDC_transfer_request_data_write_out(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                           struct pdc_region_info *region_info, void *buf, size_t unit);

#endif

#endif
