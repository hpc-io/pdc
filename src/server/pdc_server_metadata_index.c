#include "pdc_server_metadata_index.h"
#include "idioms_local_index.h"
#include "idioms_persistence.h"

art_tree *art_key_prefix_tree_g   = NULL;
art_tree *art_key_suffix_tree_g   = NULL;
size_t    num_kv_pairs_loaded_mdb = 0;
size_t    num_attrs_loaded_mdb    = 0;

uint32_t midx_server_id_g  = 0;
uint32_t midx_num_server_g = 0;

IDIOMS_t *idioms_g = NULL;

/****************************/
/* Initialize DART */
/****************************/
void
PDC_Server_metadata_index_init(uint32_t num_server, uint32_t server_id)
{
    midx_num_server_g = num_server;
    midx_server_id_g  = server_id;
    idioms_g          = IDIOMS_init(server_id, num_server);
}

/****************************/
/* DART Get Server Info */
/****************************/

perr_t
PDC_Server_dart_get_server_info(dart_get_server_info_in_t *in, dart_get_server_info_out_t *out)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);
    uint32_t serverId = in->serverId;

    out->indexed_word_count = idioms_g->index_record_count_g;
    out->request_count      = idioms_g->search_request_count_g;

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_dart_perform_one_server(dart_perform_one_server_in_t *in, dart_perform_one_server_out_t *out,
                                   uint64_t *n_obj_ids_ptr, uint64_t **buf_ptrs)
{
    perr_t                 result     = SUCCEED;
    dart_op_type_t         op_type    = in->op_type;
    dart_hash_algo_t       hash_algo  = in->hash_algo;
    char *                 attr_key   = (char *)in->attr_key;
    void *                 attr_val   = in->attr_val;
    uint32_t               attr_vsize = in->attr_vsize;
    pdc_c_var_type_t       attr_dtype = in->attr_vtype;
    dart_object_ref_type_t ref_type   = in->obj_ref_type;

    IDIOMS_md_idx_record_t *idx_record = (IDIOMS_md_idx_record_t *)calloc(1, sizeof(IDIOMS_md_idx_record_t));
    idx_record->key                    = attr_key;
    idx_record->value                  = attr_val;
    idx_record->virtual_node_id        = in->vnode_id;
    idx_record->type                   = in->attr_vtype;
    idx_record->value_len              = in->attr_vsize;
    idx_record->src_client_id          = in->src_client_id;

    uint64_t obj_locator = in->obj_primary_ref;
    if (ref_type == REF_PRIMARY_ID) {
        obj_locator = in->obj_primary_ref;
    }
    else if (ref_type == REF_SECONDARY_ID) {
        obj_locator = in->obj_secondary_ref;
    }
    else if (ref_type == REF_SERVER_ID) {
        obj_locator = in->obj_server_ref;
    }

    idx_record->obj_ids     = (uint64_t *)calloc(1, sizeof(uint64_t));
    idx_record->obj_ids[0]  = obj_locator;
    idx_record->num_obj_ids = 1;

    out->has_bulk = 0;
    if (op_type == OP_INSERT) {
        idx_record->is_key_suffix = in->inserting_suffix;
        idioms_local_index_create(idioms_g, idx_record);
    }
    else if (op_type == OP_DELETE) {
        idx_record->is_key_suffix = in->inserting_suffix;
        idioms_local_index_delete(idioms_g, idx_record);
    }
    else {
        // printf("attr_key=%s, attr_val=%s, attr_vsize=%d, attr_dtype=%d\n", attr_key, attr_val, attr_vsize,
        //        attr_dtype);

        idx_record->num_obj_ids = 0;
        idioms_local_index_search(idioms_g, idx_record);
        *n_obj_ids_ptr = idx_record->num_obj_ids;
        *buf_ptrs      = idx_record->obj_ids;

        if (attr_key[0] == '*' && attr_key[strlen(attr_key) - 1] == '*') {
            printf("server_id = %d, attr_key=%s, attr_val=%s, attr_vsize=%d, attr_dtype=%d\n",
                   midx_server_id_g, attr_key, attr_val, attr_vsize, attr_dtype);
            printf("result = ");
            for (int i = 0; i < *n_obj_ids_ptr; i++) {
                printf("%" PRIu64 " ", idx_record->obj_ids[i]);
            }
            printf("\n");
        }

        out->n_items = (*n_obj_ids_ptr);
        if ((*n_obj_ids_ptr) > 0) {
            out->has_bulk = 1;
        }
    }
    return result;
}

// ********************* Index Dump  *********************

perr_t
metadata_index_dump(char *checkpiont_dir, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;
    ret_value        = idioms_metadata_index_dump(idioms_g, checkpiont_dir, serverID);
    return ret_value;
}

// ********************* Index Recover  *********************

perr_t
metadata_index_recover(char *checkpiont_dir, int num_server, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;
    ret_value        = idioms_metadata_index_recover(idioms_g, checkpiont_dir, num_server, serverID);
    return ret_value;
}