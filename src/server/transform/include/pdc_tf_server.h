#ifndef PDC_TF_H
#define PDC_TF_H

#include "pdc_tf_common.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_vector.h"

extern PDC_VECTOR *tf_obj_id_to_dg_vector_g;

/**
 * This is similar to the client side pdc_tf_obj_t
 * The mean difference being that in the former
 * the structure is a field on _pdc_obj_info which has the
 * object id. We need to keep track of it on the server side.
 * Hence the extra obj_id field here.
 */
typedef struct pdc_tf_obj_id_to_dg_t {
    pdcid_t             obj_id;
    struct pdc_tf_obj_t pdc_tf_obj;
    pdc_dg_t *          dg;
} pdc_tf_obj_id_to_dg_t;

/**
 * These functions should only be used on the
 * data server. There are similar functions between
 * this and the client API, however, the method in
 * which directed graphs and their relation to regions
 * is stored does not use the pdcid_t on the server side.
 */

/**
 * Load the JSON filepath into a directed graph and
 * creates a mapping for the conceptual to actual region representation
 * NOTE: This does not set the actual reigon size because that should be
 *       set after the transformations have been run when writing to the data server
 *       and before transformations have been run when reading from the data server
 */
perr_t PDCtf_store_json_mapping(pdcid_t obj_id, char *json_filepath, char *cur_state, char *client_state,
                                char *store_state, uint64_t *offset, uint64_t *size, uint8_t ndim,
                                pdc_var_type_t pdc_var_type);

/* ── scheduling mode enum ────────────────────────────────────────────────────
 * Pass to PDCtf_exec_graph() to select scheduling strategy.
 *
 * PDC_TF_SCHED_DYNAMIC : polynomial-based GPU selection (default)
 *   - Predicts total_ms for each GPU using the fitted degree-3 polynomial
 *   - Selects the GPU with lowest predicted total_ms
 *   - Updates lag features after each transformation
 *
 * PDC_TF_SCHED_STATIC  : always use GPU 0 (baseline for comparison)
 *   - Mirrors the behavior of USE_GPU=1 with a fixed device
 *   - Used to measure scheduler benefit vs no scheduling
 * ────────────────────────────────────────────────────────────────────────── */
typedef enum {
    PDC_TF_SCHED_DYNAMIC = 0,
    PDC_TF_SCHED_STATIC  = 1,
} pdc_tf_sched_mode_t;

perr_t PDCtf_exec_graph(pdc_dg_t *dg, uint64_t flat_conceptual_offset, char *cur_state, char *desired_state,
                        pdc_tf_region_t input_region, pdc_tf_region_t *output_region, void **input,
                        int is_write, pdc_tf_sched_mode_t sched_mode);
#endif
