#ifndef PDC_TF_H
#define PDC_TF_H

/**
 * List of FIXME:'s
 *  1. Keep track of region history
 *  2. Parse func name correctly when adding function
 */
#include "pdc_public.h"
#include "pdc_dg.h"
#include "pdc_tf_common.h"

#ifndef TF_GRAPHS_DIR
#error "TF_GRAPHS_DIR was undefined"
#endif

/**
 * - PDC_TF_OVERWRITE: the transformation modifies the existing object.
 * - PDC_TF_CREATE: the transformation creates a new object for the output.
 */
typedef enum pdc_tf_output_mode_t {
    PDC_TF_OVERWRITE,
    PDC_TF_CREATE,
} pdc_tf_output_mode_t;

pdc_dg_t *PDCtf_get_dg(pdcid_t dg_id);

/**
 * specifies how the output of a transformation function should be handled
 * the default is PDC_TF_OVERWRITE if this function is not called
 */
perr_t PDCtf_set_output_mode(pdcid_t dg_id, pdc_tf_output_mode_t mode, pdcid_t *obj_ids, char **result_names,
                             int num_ids);

/**
 * free resources used by directed graph
 * this includes resources managed by functions/data states
 */
perr_t PDCtf_close_dg(pdcid_t dg_id);

// region transfer to/from the specified obj_id, global_reg_id follow DG
perr_t PDCtf_attach_to_region(pdcid_t dg_id, pdcid_t obj_id, pdcid_t remote_reg, char *client_state,
                              char *store_state);

// all region transfers for obj_id follow DG
perr_t PDCtf_attach_to_obj(pdcid_t dg_id, pdcid_t obj_id, char *client_state, char *store_state);

// all region transfers for specified obj_ids follow DG
perr_t PDCtf_attach_to_objs(pdcid_t dg_id, pdcid_t *obj_ids, int num_ids, char *client_state,
                            char *store_state);

// register data states, transformations, and directed graphs as types
perr_t PDCtf_init();

// print graph in readable format
void PDCtf_print_dg(pdcid_t dg_id, bool write_to_file);

// print execution path
void PDCtf_print_exec_path(pdcid_t dg_id, char *cur_state, char *desired_state);

// load JSON file describing directed graph
pdcid_t PDCtf_dg_json_create(char *json_filepath);

/**
 * free a pdc_tf_obj_t and everything it owns: the per-region state strings and region
 * mapping entries in region_mappings_vector, the vector itself, and the all_regions_state
 * strings set by PDCtf_attach_to_obj(s). Safe to call with tf_obj == NULL.
 */
void PDCtf_obj_free(struct pdc_tf_obj_t *tf_obj);

#endif /* PDC_TF_H */
