#include <time.h>

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"

#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"
#include "mercury_list.h"

#ifndef PDC_SERVER_H
#define PDC_SERVER_H

#define CREATE_BLOOM_THRESHOLD 64

static pdc_cnt_t pdc_num_reg;
extern hg_class_t *hg_class_g;

perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out);
/* perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out); */
perr_t PDC_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out);
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out);
perr_t PDC_Server_checkpoint(char *filename);
perr_t PDC_Server_restart(char *filename);
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs);
pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id);
//perr_t PDC_Server_region_map(gen_reg_map_notification_in_t *in, gen_reg_map_notification_out_t *out, struct hg_info *info);

typedef struct pdc_metadata_name_mark_t {
    char obj_name[ADDR_MAX];
    struct pdc_metadata_name_mark_t *next;
    struct pdc_metadata_name_mark_t *prev;
} pdc_metadata_name_mark_t;

typedef struct pdc_hash_table_entry_head {
    int n_obj;
    void *bloom;
    pdc_metadata_t *metadata;
} pdc_hash_table_entry_head;

#endif /* PDC_SERVER_H */
