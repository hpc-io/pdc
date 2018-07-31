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


#ifndef PDC_SERVER_METADATA_H
#define PDC_SERVER_METADATA_H

#include <time.h>

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"
#include "mercury_atomic.h"

#include "hash-table.h"

#include "pdc_server_common.h"
#include "pdc_client_server_common.h"

#define CREATE_BLOOM_THRESHOLD  64

extern int    pdc_server_rank_g;
extern int    pdc_server_size_g;
extern char   pdc_server_tmp_dir_g[ADDR_MAX];
extern uint32_t n_metadata_g;
extern HashTable *metadata_hash_table_g;
extern HashTable *container_hash_table_g;
extern hg_class_t   *hg_class_g;
extern hg_context_t *hg_context_g;
extern int is_debug_g;

extern hg_id_t get_metadata_by_id_register_id_g;
extern hg_id_t send_client_storage_meta_rpc_register_id_g;
extern pdc_client_info_t        *pdc_client_info_g       ;
extern pdc_remote_server_info_t *pdc_remote_server_info_g;
extern double total_mem_usage_g;
extern int is_hash_table_init_g;
extern int    is_restart_g;

typedef struct pdc_hash_table_entry_head {
    int n_obj;
    void *bloom;
    pdc_metadata_t *metadata;
} pdc_hash_table_entry_head;

typedef struct pdc_cont_hash_table_entry_t {
    uint64_t  cont_id;
    char      cont_name[ADDR_MAX];
    int       n_obj;
    int       n_deleted;
    int       n_allocated;
    uint64_t *obj_ids;
    char      tags[TAG_LEN_MAX];
} pdc_cont_hash_table_entry_t;


pbool_t region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2);
int PDC_Server_is_contiguous_region_overlap(region_list_t *a, region_list_t *b);
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out);
perr_t PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts, pdc_metadata_t** out);
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs);
pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id);
perr_t PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out);
perr_t PDC_Server_create_container(gen_cont_id_in_t *in, gen_cont_id_out_t *out);
perr_t PDC_Server_container_del_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id);
perr_t PDC_Server_container_add_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id);
perr_t PDC_Server_print_all_metadata();
perr_t PDC_Server_print_all_containers();
perr_t PDC_Server_metadata_duplicate_check();
perr_t PDC_Server_init_hash_table();
perr_t PDC_Server_hash_table_list_init(pdc_hash_table_entry_head *entry, uint32_t *hash_key);
perr_t PDC_Server_hash_table_list_insert(pdc_hash_table_entry_head *head, pdc_metadata_t *new);
pdc_metadata_t *find_metadata_by_id(uint64_t obj_id);
perr_t PDC_Server_get_metadata_by_id_with_cb(uint64_t obj_id, perr_t (*cb)(), void *args);

hg_return_t PDC_Server_query_read_names_clinet_cb(const struct hg_cb_info *callback_info);
#endif /* PDC_SERVER_METADATA_H */
