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

perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out);
perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out);

typedef struct pdc_metadata_name_mark_t {
    char obj_name[ADDR_MAX];
    struct pdc_metadata_name_mark_t *next;
    struct pdc_metadata_name_mark_t *prev;
} pdc_metadata_name_mark_t;

#endif /* PDC_SERVER_H */
