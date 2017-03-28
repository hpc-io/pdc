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

static unsigned pdc_num_reg;

perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out);
perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out);
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out);
perr_t PDC_Server_checkpoint(char *filename);
perr_t PDC_Server_restart(char *filename);

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
