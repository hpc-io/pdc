/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#ifndef PDC_CLIENT_CACHE_H
#define PDC_CLIENT_CACHE_H

#include "pdc_public.h"
#include "pdc_obj.h"
#include "pdc_client_cache_dl.h"
#include "pdc_client_cache_prefetch.h"

extern pdcid_t pdc_id;
extern size_t  total_buf_size;
extern int     total_item_num;

typedef struct {
    int      world_rank;
    int      cache_enabled;
    int      cached_item_num;
    uint64_t total_cached_bytes;
    uint64_t cache_hits;
    uint64_t cache_misses;
} pdc_cache_stats_t;

/****************************************************/
/* Public Functions for Client-side Region Caching */
/****************************************************/
perr_t PDCcache_get_stats(pdc_cache_stats_t *out_stats);
void   PDCcache_reset_stats(void);

/****************************************************/
/* Private Functions for Client-side Region Caching */
/****************************************************/

perr_t PDC_client_cache_init(pdcid_t pdcid);

int PDC_client_cache_search(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                            void *buf);

perr_t PDC_client_cache_insert(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                               void *buf);

perr_t PDC_client_cache_update(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                               void *buf);

perr_t PDC_client_cache_evict();

void PDC_client_cache_timelog(double start_time, const char *message);

perr_t PDC_client_cache_finalize();

#endif /* PDC_CLIENT_CACHE_H */
