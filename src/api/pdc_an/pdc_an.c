#include <string.h>

#include "pdc_an.h"
#include "pdc_timing.h"
#include "pdc_interface.h"
#include "pdc_prop.h"
#include "pdc_obj_pkg.h"
#include "pdc_dg.h"
#include "pdc_malloc.h"
#include "pdc_region.h"
#include "pdc_an_common.h"
#include "pdc_client_connect.h"
#include "pdc_tf_common.h"
#include "pdc_tf.h"

pdc_dg_t *
PDCan_get_dg(pdcid_t dg_id)
{
    FUNC_ENTER(NULL);

    pdc_dg_t *ret_value = NULL;

    struct _pdc_id_info *id_info = PDC_find_id(dg_id);
    if (id_info == NULL)
        PGOTO_ERROR(NULL, "Failed to find dg_id");

    ret_value = (pdc_dg_t *)id_info->obj_ptr;
    if (ret_value == NULL)
        PGOTO_ERROR(NULL, "Failed to find dg");

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCan_dg_json_create(char *json_filepath)
{
    FUNC_ENTER(NULL);

    pdcid_t ret_value = 0;

    pdc_dg_t *dg = PDCan_dg_json_create_common(json_filepath);
    if (dg == NULL)
        PGOTO_ERROR(0, "Failed to parse analysis graph JSON \"%s\"\n", json_filepath);

    ret_value = PDC_id_register(PDC_AN_DG, dg);

done:
    FUNC_LEAVE(ret_value);
}

static perr_t
PDCan_dg_free(void *dg)
{
    FUNC_ENTER(NULL);
    PDCdg_destroy((pdc_dg_t *)dg);
    FUNC_LEAVE(SUCCEED);
}

perr_t
PDCan_close_dg(pdcid_t dg_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    if (PDC_dec_ref(dg_id) < 0)
        PGOTO_ERROR(FAIL, "Analysis graph: problem freeing ID");

done:
    FUNC_LEAVE(ret_value);
}

/**
 * Checks whether tf_obj already has a PDC TF graph attached to exactly
 * region_info (matching PDCtf_region_has_attached_graph's per-region
 * matching, plus the whole-object attach_to_all_regions case that
 * function doesn't check). If so, resolves the state's client-local dg_id
 * to the underlying graph's json_filepath and returns true with
 * *json_filepath_out/*client_state_out/*store_state_out set (borrowed
 * pointers, not owned by the caller).
 */
static bool
find_attached_tf_info(struct pdc_tf_obj_t *tf_obj, struct pdc_region_info *region_info,
                      char **json_filepath_out, char **client_state_out, char **store_state_out)
{
    if (tf_obj == NULL)
        return false;

    pdc_tf_region_state_t *state = NULL;

    if (tf_obj->attach_to_all_regions) {
        state = &tf_obj->all_regions_state;
    }
    else if (tf_obj->region_mappings_vector != NULL) {
        PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(tf_obj->region_mappings_vector);
        while (pdc_vector_iterator_has_next(iter)) {
            pdc_tf_region_mapping_t *m = (pdc_tf_region_mapping_t *)pdc_vector_iterator_next(iter);
            if (m == NULL || m->conceptual_region.ndim != region_info->ndim)
                continue;

            bool match = true;
            for (size_t i = 0; i < region_info->ndim; i++) {
                if (m->conceptual_offset[i] != region_info->offset[i] ||
                    m->conceptual_region.size[i] != region_info->size[i]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                state = &m->region_state;
                break;
            }
        }
        pdc_vector_iterator_destroy(iter);
    }

    if (state == NULL)
        return false;

    pdc_dg_t *tf_dg = PDCtf_get_dg(state->dg_id);
    if (tf_dg == NULL)
        return false;

    *json_filepath_out = (char *)tf_dg->data;
    *client_state_out  = state->client_state;
    *store_state_out   = state->store_state;
    return true;
}

perr_t
PDCan_attach_to_region(pdcid_t dg_id, char *state_name, pdcid_t obj_id, pdcid_t region)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    pdc_dg_t *dg = PDCan_get_dg(dg_id);
    if (dg == NULL)
        PGOTO_ERROR(FAIL, "Failed to find analysis graph");

    struct _pdc_id_info *obj_id_info = PDC_find_id(obj_id);
    if (obj_id_info == NULL)
        PGOTO_ERROR(FAIL, "Failed to find object using pdcid");
    struct _pdc_obj_info *pdc_obj_info = obj_id_info->obj_ptr;

    struct _pdc_id_info *region_id_info = PDC_find_id(region);
    if (region_id_info == NULL)
        PGOTO_ERROR(FAIL, "Cannot locate region ID");
    struct pdc_region_info *region_info = region_id_info->obj_ptr;

    if (PDCan_dg_get_state(dg, state_name) == NULL)
        PGOTO_ERROR(FAIL, "State \"%s\" not found in analysis graph", state_name);

    /* Client-local only -- no RPC. Exactly like PDCtf_attach_to_region:
     * the server never sees this call at all. The actual registration
     * happens lazily, piggybacked on whichever read or write RPC first
     * touches a matching region (see the an_pkg block in
     * PDC_Client_transfer_request) -- that RPC is already correctly
     * routed to whichever server owns this object's data (via
     * obj->metadata->data_server_id), so the piggybacked registration
     * always lands on the right server without this call needing to know
     * or compute that itself. */
    if (pdc_obj_info->pdc_an_obj == NULL) {
        pdc_obj_info->pdc_an_obj                         = PDC_calloc(1, sizeof(pdc_an_obj_t));
        pdc_obj_info->pdc_an_obj->region_mappings_vector = pdc_vector_create(8, 2.0);
    }

    pdc_an_region_mapping_t *mapping = PDC_calloc(1, sizeof(pdc_an_region_mapping_t));
    mapping->region_state.dg_id      = dg_id;
    mapping->region_state.state_name = strdup(state_name);
    mapping->ndim                    = (uint8_t)region_info->ndim;
    memcpy(mapping->offset, region_info->offset, region_info->ndim * sizeof(uint64_t));
    memcpy(mapping->size, region_info->size, region_info->ndim * sizeof(uint64_t));
    mapping->obj_id       = (uint64_t)pdc_obj_info->obj_info_pub->meta_id;
    mapping->obj_ndim     = (int32_t)pdc_obj_info->obj_pt->obj_prop_pub->ndim;
    mapping->pdc_var_type = pdc_obj_info->obj_pt->obj_prop_pub->type;
    memcpy(mapping->obj_dims, pdc_obj_info->obj_pt->obj_prop_pub->dims,
           (size_t)mapping->obj_ndim * sizeof(uint64_t));

    /* If this object/region also has a PDC TF graph attached client-side
     * (PDCtf_attach_to_region/_to_obj), record it here too so the
     * piggyback can register it alongside the analysis binding -- an
     * analysis input/output is written and read by PDCan_exec_graph
     * running server-side, never by a client write RPC, so PDC TF's own
     * piggyback (which rides on client write RPCs) would otherwise never
     * see it. find_attached_tf_info returns borrowed pointers; copy them
     * since this mapping outlives this call. */
    char *tf_json_filepath = NULL;
    char *tf_client_state  = NULL;
    char *tf_store_state   = NULL;
    if (find_attached_tf_info(pdc_obj_info->pdc_tf_obj, region_info, &tf_json_filepath, &tf_client_state,
                              &tf_store_state)) {
        mapping->region_state.tf_json_filepath = strdup(tf_json_filepath);
        mapping->region_state.tf_client_state  = strdup(tf_client_state);
        mapping->region_state.tf_store_state   = strdup(tf_store_state);
    }

    pdc_vector_add(pdc_obj_info->pdc_an_obj->region_mappings_vector, mapping);
    PDCan_add_client_dg_mapping(dg_id, mapping);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCan_init(void)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (PDC_register_type(PDC_AN_DG, (PDC_free_t)PDCan_dg_free) < 0)
        PGOTO_ERROR(FAIL, "Failed to register PDC_AN_DG type");
    if (PDCan_init_builtin_funcs() != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDCan_init_builtin_funcs");

done:
    FUNC_LEAVE(ret_value);
}
