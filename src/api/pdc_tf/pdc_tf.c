#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>

#include "pdc_tf.h"
#include "pdc_timing.h"
#include "pdc_interface.h"
#include "pdc_prop.h"
#include "pdc_obj_pkg.h"
#include "pdc_dg.h"
#include "pdc_malloc.h"
#include "pdc_region.h"
#include "pdc_tf_common.h"
#include "pdc_client_server_common.h"

pdc_dg_t *
PDCtf_get_dg(pdcid_t dg_id)
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
PDCtf_dg_json_create(char *json_filepath)
{
    FUNC_ENTER(NULL);
    pdcid_t ret_value = 0;

    pdc_dg_t *dg = PDCtf_dg_json_create_common(json_filepath);
    if (dg == NULL) {
        abort();
        PGOTO_ERROR(FAIL, "Error with PDCtf_open_dg_json_common");
    }

    ret_value = PDC_id_register(PDC_TF_DG, dg);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCtf_close_dg(pdcid_t dg_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    if (PDC_dec_ref(dg_id) < 0)
        PGOTO_ERROR(FAIL, "Directed graph: problem of freeing ID");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCtf_set_output_mode(pdcid_t dg_id, pdc_tf_output_mode_t mode, pdcid_t *obj_ids, char **result_names,
                      int num_ids)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}

/**
 * Gets pdc_tf_obj_t from obj_id
 * If pdc_tf_obj_t is NULL then one is allocated
 */
static perr_t
locate_and_set_pdc_tf_obj_t(pdcid_t obj_id, struct _pdc_obj_info **pdc_obj_info,
                            struct pdc_tf_obj_t **pdc_tf_obj)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    // First locate object
    const struct _pdc_id_info *obj_id_info = PDC_find_id(obj_id);
    if (obj_id_info == NULL)
        PGOTO_ERROR(FAIL, "Failed to find object using pdcid");
    *pdc_obj_info = obj_id_info->obj_ptr;

    // Validate partition strategy is supported with transformations
    /*if ((*pdc_obj_info)->obj_pt->obj_prop_pub->region_partition != PDC_OBJ_STATIC) {
        LOG_WARNING("Partition strategy not supported for transformations\n");
        PGOTO_ERROR(FAIL, "The following partitions strategies are supported: PDC_OBJ_STATIC");
    }*/

    // Validate user has set the datatype on the object
    if (PDC_get_var_type_size((*pdc_obj_info)->obj_pt->obj_prop_pub->type) == 0) {
        LOG_ERROR("Invalid data type for object transformation\n");
        PGOTO_ERROR(FAIL, "Data type must be set on object before attaching transformations");
    }

    // Pull out pdc obj transform information and allocate first if NULL
    if ((*pdc_obj_info)->pdc_tf_obj == NULL)
        (*pdc_obj_info)->pdc_tf_obj = PDC_calloc(1, sizeof(struct pdc_tf_obj_t));
    *pdc_tf_obj = (*pdc_obj_info)->pdc_tf_obj;

done:
    FUNC_LEAVE(ret_value);
}

// Region transfer to/from the specified obj_id, remote_reg_id follow DG
perr_t
PDCtf_attach_to_region(pdcid_t dg_id, pdcid_t obj_id, pdcid_t remote_reg, char *client_state,
                       char *store_state)
{
    FUNC_ENTER(NULL);

    LOG_DEBUG("PDCtf_attach_to_region was called\n");

    perr_t ret_value = SUCCEED;

    struct pdc_tf_obj_t * pdc_tf_obj   = NULL;
    struct _pdc_obj_info *pdc_obj_info = NULL;
    if (locate_and_set_pdc_tf_obj_t(obj_id, &pdc_obj_info, &pdc_tf_obj) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with locate_and_set_pdc_tf_obj_t");

    // Get region information
    struct _pdc_id_info *region_id_info = PDC_find_id(remote_reg);
    if (region_id_info == NULL)
        PGOTO_ERROR(FAIL, "Cannot locate remote region ID");
    struct pdc_region_info *region_info = region_id_info->obj_ptr;

    // Reject attaching a graph to a region that already has one. PDCtf_attach_to_region has no
    // "replace" semantics: silently adding a second mapping for the same region would leak the old
    // region_mapping and leave two conflicting conceptual/actual regions for the same bytes.
    pdc_tf_region_mapping_t *existing_mapping = NULL;
    if (PDCtf_region_has_attached_graph(pdc_tf_obj, region_info->ndim,
                                        PDC_get_var_type_size(pdc_obj_info->obj_pt->obj_prop_pub->type),
                                        region_info->offset, region_info->size, &existing_mapping))
        PGOTO_ERROR(FAIL, "A graph is already attached to this region");

    // Create region mapping vector if needed
    if (pdc_tf_obj->region_mappings_vector == NULL)
        pdc_tf_obj->region_mappings_vector = pdc_vector_create(8, 2.0);

    // Add region mapping
    pdc_tf_region_mapping_t *region_mapping = PDC_calloc(1, sizeof(pdc_tf_region_mapping_t));
    pdc_vector_add(pdc_tf_obj->region_mappings_vector, region_mapping);

    pdc_tf_region_t *conceptual_region = &region_mapping->conceptual_region;
    uint64_t *       conceptual_offset = region_mapping->conceptual_offset;

    // Copy region information into conceptual region
    PDCtf_set_tf_region_t(conceptual_region, region_info->ndim, pdc_obj_info->obj_pt->obj_prop_pub->type,
                          region_info->size);
    memcpy(conceptual_offset, region_info->offset, region_info->ndim * sizeof(uint64_t));

    region_mapping->region_state.client_state = strdup(client_state);
    region_mapping->region_state.cur_state    = strdup(client_state);
    region_mapping->region_state.store_state  = strdup(store_state);
    region_mapping->region_state.dg_id        = dg_id;
done:
    FUNC_LEAVE(ret_value);
}

// all region transfers for obj_id follow DG
perr_t
PDCtf_attach_to_obj(pdcid_t dg_id, pdcid_t obj_id, char *client_state, char *store_state)
{
    FUNC_ENTER(NULL);

    LOG_DEBUG("PDCtf_attach_to_obj was called\n");

    perr_t ret_value = SUCCEED;

    struct pdc_tf_obj_t * pdc_tf_obj   = NULL;
    struct _pdc_obj_info *pdc_obj_info = NULL;
    if (locate_and_set_pdc_tf_obj_t(obj_id, &pdc_obj_info, &pdc_tf_obj) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with locate_and_set_pdc_tf_obj_t");

    if (pdc_tf_obj->attach_to_all_regions)
        LOG_WARNING("Graph was already attached to obj\n");
    pdc_tf_obj->attach_to_all_regions = true;

    pdc_tf_obj->all_regions_state.client_state = strdup(client_state);
    pdc_tf_obj->all_regions_state.cur_state    = strdup(client_state);
    pdc_tf_obj->all_regions_state.store_state  = strdup(store_state);
    pdc_tf_obj->all_regions_state.dg_id        = dg_id;

done:
    FUNC_LEAVE(ret_value);
}

// all region transfers for specified obj_ids follow DG
perr_t
PDCtf_attach_to_objs(pdcid_t dg_id, pdcid_t *obj_ids, int num_ids, char *client_state, char *store_state)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    for (int i = 0; i < num_ids; i++) {
        if (PDCtf_attach_to_obj(dg_id, obj_ids[i], client_state, store_state) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDCtf_attach_to_obj");
    }

done:
    FUNC_LEAVE(ret_value);
}

static perr_t
PDCtf_dg_free(void *dg)
{
    FUNC_ENTER(NULL);

    LOG_INFO("PDCtf_dg_free called\n");

    perr_t ret_value = SUCCEED;
    PDCdg_destroy((pdc_dg_t *)dg);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCtf_init()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (PDC_register_type(PDC_TF_DG, (PDC_free_t)PDCtf_dg_free) < 0)
        PGOTO_ERROR(FAIL, "Failed to register PDC_TF_DG type");
    if (PDCtf_init_builtin_funcs() != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDCtf_init_builtin_funcs");

done:
    FUNC_LEAVE(ret_value);
}

void
PDCtf_print_dg(pdcid_t dg_id, bool write_to_file)
{
    pdc_dg_t *dg = PDCtf_get_dg(dg_id);
    assert(dg != NULL);
    PDCtf_print_dg_common(dg, write_to_file);
}

void
PDCtf_print_exec_path(pdcid_t dg_id, char *cur_state, char *desired_state)
{
    PDCtf_print_exec_path_common(PDCtf_get_dg(dg_id), cur_state, desired_state);
}

void
PDCtf_obj_free(struct pdc_tf_obj_t *tf_obj)
{
    if (tf_obj == NULL)
        return;

    if (tf_obj->region_mappings_vector != NULL) {
        size_t n = pdc_vector_size(tf_obj->region_mappings_vector);
        for (size_t i = 0; i < n; i++) {
            // Only free the strings here: pdc_vector_destroy() below already frees each item in
            // the vector (the pdc_tf_region_mapping_t structs themselves), so freeing them here
            // too would be a double free.
            pdc_tf_region_mapping_t *region_mapping = pdc_vector_get(tf_obj->region_mappings_vector, i);
            region_mapping->region_state.client_state =
                (char *)PDC_free(region_mapping->region_state.client_state);
            region_mapping->region_state.cur_state = (char *)PDC_free(region_mapping->region_state.cur_state);
            region_mapping->region_state.store_state =
                (char *)PDC_free(region_mapping->region_state.store_state);
        }
        pdc_vector_destroy(tf_obj->region_mappings_vector);
        tf_obj->region_mappings_vector = NULL;
    }

    tf_obj->all_regions_state.client_state = (char *)PDC_free(tf_obj->all_regions_state.client_state);
    tf_obj->all_regions_state.cur_state    = (char *)PDC_free(tf_obj->all_regions_state.cur_state);
    tf_obj->all_regions_state.store_state  = (char *)PDC_free(tf_obj->all_regions_state.store_state);

    PDC_free(tf_obj);
}