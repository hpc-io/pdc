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

#ifndef PDC_CLIENT_CACHE_DL_H
#define PDC_CLIENT_CACHE_DL_H

#include <mpi.h>
#include "pdc_public.h"
#include "pdc_obj.h"

#ifndef NUM_CHUNKS
#define NUM_CHUNKS 4
#endif
#ifndef MAX_ITEM_SIZE
#define MAX_ITEM_SIZE 67108864
#endif
#ifndef MAX_SLOTS_PER_NODE
#define MAX_SLOTS_PER_NODE 340
#endif

#define INTRA_TRANSFER_UNIT_SIZE (sizeof(pdcid_t) + sizeof(int) * 2 + sizeof(uint64_t) * 8)
#define INTER_TRANSFER_UNIT_SIZE (sizeof(pdcid_t) + sizeof(int) + sizeof(uint64_t) * 8 + MAX_ITEM_SIZE)

#define SLOT_INVALID -1

/**************************/
/* Library Private Struct */
/**************************/
typedef struct {
    int  free_stack_head;
    int  free_list[MAX_SLOTS_PER_NODE];
    char padding[64 - sizeof(int)];
} SharedMemoryHeader;

typedef struct pdc_object_data {
    // PDC Object information
    pdcid_t  obj_id;
    uint64_t unit;

    // Remote region information
    int reg_ndim;

    uint64_t reg_offset[3];
    uint64_t reg_size[3];
    uint64_t reg_buf_size;

    // Region data
    int slot_idx;

    // Index info
    int target_rank;
    int data_exchange_type; // 0 is intra, 1 is inter

    struct pdc_object_data *prev;
    struct pdc_object_data *next;
} pdc_object_data;

// Specifies client specific information
typedef struct pdc_client_info {
    // Global and local rank info
    int world_rank;
    int world_size;

    int node_rank;
    int node_size;
    int node_manager_rank;

    int cached_item_num;

    pdc_object_data *local_cache_list_head;
    pdc_object_data *local_cache_list_tail;

    MPI_Win             node_shared_data_win;
    SharedMemoryHeader *header;
    void *              node_shared_base;
    char *              node_shared_data_base;

    int *rank_to_node_id_map;
    int *world_to_node_rank_map;
    int *target_ranks;

    int client_cache_init;
} pdc_client_info;

/**************************************************************************/
/* Private Functions for Linked List Structure Client-side Region Caching */
/**************************************************************************/

perr_t PDC_client_cache_dl_init();

int PDC_client_cache_dl_local_search(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset,
                                     uint64_t *size, void *buf, uint64_t read_size);

perr_t PDC_client_cache_dl_insert(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                                  void *buf, uint64_t read_size);

perr_t PDC_client_cache_dl_update(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                                  void *buf);

perr_t PDC_client_cache_dl_evict();

perr_t PDC_client_cache_dl_prepare_data_exchange(pdcid_t *global_prefetch_list, uint64_t *offset,
                                                 uint64_t *size, int obj_prefetch_list_len);

perr_t PDC_client_cache_dl_data_exchange(pdcid_t *global_prefetch_list, int obj_prefetch_list_len);

perr_t PDC_client_cache_dl_finalize();

#endif /* PDC_CLIENT_CACHE_DL_H */
