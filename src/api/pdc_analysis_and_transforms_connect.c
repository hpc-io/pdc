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

#include "pdc_obj_pkg.h"
#include "pdc_client_connect.h"
#include "pdc_analysis_pkg.h"
#include "pdc_transforms_common.h"

static hg_context_t *send_context_g = NULL;
static int           work_todo_g    = 0;

/* Forward References:: */
// Analysis and Transformations

hg_id_t analysis_ftn_register_id_g;
hg_id_t transform_ftn_register_id_g;
hg_id_t server_transform_ftn_register_id_g;
hg_id_t object_data_iterator_register_id_g;

static hg_return_t client_register_iterator_rpc_cb(const struct hg_cb_info *info);
static hg_return_t client_register_analysis_rpc_cb(const struct hg_cb_info *info);
static hg_return_t client_register_transform_rpc_cb(const struct hg_cb_info *info);

perr_t PDC_free_obj_info(struct _pdc_obj_info *obj);

/* Client APIs */

perr_t
PDC_Client_send_iter_recv_id(pdcid_t iter_id, pdcid_t *meta_id)
{
    uint64_t                   ret_value = SUCCEED;
    struct _pdc_iterator_info *thisIter  = NULL;
    struct _pdc_my_rpc_state * my_rpc_state_p;
    obj_data_iterator_in_t     in;
    hg_return_t                hg_ret;
    int                        server_id = 0;
    int                        n_retry   = 0;
    struct _pdc_obj_info *     object_info;

    FUNC_ENTER(NULL);

    my_rpc_state_p = (struct _pdc_my_rpc_state *)calloc(1, sizeof(struct _pdc_my_rpc_state));
    if (my_rpc_state_p == NULL)
        PGOTO_ERROR(FAIL, "PDC_Client_send_iter_recv_id(): Could not allocate my_rpc_state");

    if ((PDC_Block_iterator_cache != NULL) && (iter_id > 0)) {
        thisIter = &PDC_Block_iterator_cache[iter_id];
        /* Find the server association to the specific object */
        in.client_iter_id = iter_id;
        object_info       = PDC_obj_get_info(thisIter->objectId);

        /* Co-locate iterator info with the actual object */
        if (object_info != NULL) {
            server_id    = object_info->obj_info_pub->server_id;
            in.object_id = object_info->obj_info_pub->meta_id;
        }
        else
            in.object_id = thisIter->objectId; /* Is this ok? */
        in.reg_id           = thisIter->reg_id;
        in.sliceCount       = thisIter->sliceCount;
        in.sliceNext        = thisIter->sliceNext;
        in.sliceResetCount  = thisIter->sliceResetCount;
        in.elementsPerSlice = thisIter->elementsPerSlice;
        in.slicePerBlock    = thisIter->slicePerBlock;
        in.elementsPerPlane = thisIter->elementsPerPlane;
        in.elementsPerBlock = thisIter->elementsPerBlock;
        in.skipCount        = thisIter->skipCount;
        in.element_size     = thisIter->element_size;
        in.srcBlockCount    = thisIter->srcBlockCount;
        in.contigBlockSize  = thisIter->contigBlockSize;
        in.totalElements    = thisIter->totalElements;
        in.ndim             = thisIter->ndim;
        in.dims_0           = thisIter->dims[0];
        in.dims_1           = thisIter->dims[1];
        in.dims_2           = thisIter->dims[2];
        in.dims_3           = thisIter->dims[3];
        in.storageinfo      = (int)((thisIter->storage_order << 8) | thisIter->pdc_datatype);
    }

    while (pdc_server_info_g[server_id].addr_valid != 1) {
        if (n_retry > 0)
            break;
        if (PDC_Client_lookup_server(server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_lookup_server", pdc_client_mpi_rank_g);

        n_retry++;
    }

    in.server_id = server_id;
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, object_data_iterator_register_id_g,
              &my_rpc_state_p->handle);
    hg_ret = HG_Forward(my_rpc_state_p->handle, client_register_iterator_rpc_cb, my_rpc_state_p, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_client_send_iter_recv_id(): Could not start HG_Forward()");

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (my_rpc_state_p->value == 0) {
        *meta_id = 0;
        PGOTO_DONE(FAIL);
    }

    *meta_id = my_rpc_state_p->value;

done:
    fflush(stdout);
    HG_Destroy(my_rpc_state_p->handle);
    free(my_rpc_state_p);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_register_iterator_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t               ret_value      = HG_SUCCESS;
    struct _pdc_my_rpc_state *my_rpc_state_p = info->arg;
    obj_data_iterator_out_t   output;

    FUNC_ENTER(NULL);

    if (info->ret == HG_SUCCESS) {
        ret_value = HG_Get_output(info->info.forward.handle, &output);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(FAIL,
                        "PDC_CLIENT: register_iterator_rpc_cb(): Unable to read the server return values");

        my_rpc_state_p->value = output.server_iter_id;
    }

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(info->info.forward.handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_register_obj_analysis(struct _pdc_region_analysis_ftn_info *thisFtn, const char *func,
                                 const char *loadpath, pdcid_t in_local, pdcid_t out_local, pdcid_t in_meta,
                                 pdcid_t out_meta)
{
    perr_t                     ret_value = SUCCEED;
    uint32_t                   server_id = 0;
    hg_return_t                hg_ret;
    analysis_ftn_in_t          in;
    struct _pdc_my_rpc_state * my_rpc_state_p;
    struct _pdc_iterator_info *thisIter;
    int                        n_retry      = 0;
    int                        input_server = -1, output_server = -1;
    struct _pdc_obj_info *     obj_prop;

    FUNC_ENTER(NULL);

    my_rpc_state_p = (struct _pdc_my_rpc_state *)calloc(1, sizeof(struct _pdc_my_rpc_state));
    if (my_rpc_state_p == NULL)
        PGOTO_ERROR(FAIL, "PDC_Client_register_obj_analysis(): Could not allocate my_rpc_state");

    if (pdc_server_selection_g != PDC_SERVER_DEFAULT) {
        thisIter      = &PDC_Block_iterator_cache[in_local];
        obj_prop      = PDC_obj_get_info(thisIter->objectId);
        input_server  = obj_prop->obj_info_pub->server_id;
        output_server = input_server;
    }
    else {
        if (in_local > 0) {
            thisIter     = &PDC_Block_iterator_cache[in_local];
            input_server = PDC_get_server_by_obj_id(thisIter->objectId, pdc_server_num_g);
        }
        if (out_local > 0) {
            thisIter      = &PDC_Block_iterator_cache[out_local];
            output_server = PDC_get_server_by_obj_id(thisIter->objectId, pdc_server_num_g);
        }
    }

    if (input_server < 0) {
        if (output_server < 0) {
            server_id = pdc_client_mpi_rank_g % pdc_server_num_g;
        }
        else
            server_id = output_server;
    }
    else {
        server_id = input_server;
    }

    while (pdc_server_info_g[server_id].addr_valid != 1) {
        if (n_retry > 0)
            break;
        if (PDC_Client_lookup_server(server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_lookup_server", pdc_client_mpi_rank_g);

        n_retry++;
    }

    memset(&in, 0, sizeof(in));
    in.ftn_name = func;
    in.loadpath = loadpath;
    in.iter_in  = in_meta;
    in.iter_out = out_meta;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous
    // client_test_connect_lookup_cb
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, analysis_ftn_register_id_g,
              &my_rpc_state_p->handle);
    hg_ret = HG_Forward(my_rpc_state_p->handle, client_register_analysis_rpc_cb, my_rpc_state_p, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_register_obj_analysis(): Could not start HG_Forward()");

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (my_rpc_state_p->value < 0) {
        PGOTO_DONE(FAIL);
    }
    // Here, we should update the local analysis registry with the returned valued from my_rpc_state_p;
    thisFtn->meta_index = my_rpc_state_p->value;

done:
    fflush(stdout);
    HG_Destroy(my_rpc_state_p->handle);
    free(my_rpc_state_p);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_register_analysis_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t               ret_value      = HG_SUCCESS;
    struct _pdc_my_rpc_state *my_rpc_state_p = info->arg;
    analysis_ftn_out_t        output;

    FUNC_ENTER(NULL);

    if (info->ret == HG_SUCCESS) {
        ret_value = HG_Get_output(info->info.forward.handle, &output);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value,
                        "PDC_CLIENT: register_analysis_rpc_cb(): Unable to read the server return values");

        my_rpc_state_p->value = output.remote_ftn_id;
    }

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(info->info.forward.handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_register_region_transform(const char *func, const char *loadpath,
                                     pdcid_t src_region_id ATTRIBUTE(unused), pdcid_t dest_region_id,
                                     pdcid_t obj_id, int start_state, int next_state, int op_type, int when,
                                     int client_index)
{
    perr_t                    ret_value = SUCCEED;
    uint32_t                  server_id = 0;
    hg_return_t               hg_ret;
    transform_ftn_in_t        in;
    struct _pdc_obj_info *    object_info = NULL;
    struct _pdc_my_rpc_state *my_rpc_state_p;

    FUNC_ENTER(NULL);

    my_rpc_state_p = (struct _pdc_my_rpc_state *)calloc(1, sizeof(struct _pdc_my_rpc_state));
    if (my_rpc_state_p == NULL)
        PGOTO_ERROR(FAIL, "Could not allocate my_rpc_state");

    /* Find the server associated with the input object */
    server_id   = PDC_get_server_by_obj_id(obj_id, pdc_server_num_g);
    object_info = PDC_obj_get_info(obj_id);
    memset(&in, 0, sizeof(in));
    in.ftn_name = func;
    in.loadpath = loadpath;
    if (object_info != NULL)
        in.object_id = object_info->obj_info_pub->meta_id;
    else
        in.object_id = obj_id;
    in.region_id = dest_region_id;

    in.operation_type = when;
    in.start_state    = start_state;
    in.next_state     = next_state;
    in.op_type        = op_type & 0xFF;
    in.when           = when & 0xFF;
    in.client_index   = client_index;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous
    // client_test_connect_lookup_cb
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, transform_ftn_register_id_g,
              &my_rpc_state_p->handle);
    hg_ret = HG_Forward(my_rpc_state_p->handle, client_register_transform_rpc_cb, my_rpc_state_p, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (my_rpc_state_p->value < 0) {
        PGOTO_DONE(FAIL);
    }
    // Here, we should update the local registry with the returned valued from my_rpc_state_p;

done:
    fflush(stdout);
    if (object_info)
        PDC_free_obj_info(object_info);
    HG_Destroy(my_rpc_state_p->handle);
    free(my_rpc_state_p);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_register_transform_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t         ret_value = HG_SUCCESS;
    transform_ftn_out_t output;

    FUNC_ENTER(NULL);

    if (info->ret == HG_SUCCESS) {
        ret_value = HG_Get_output(info->info.forward.handle, &output);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value, "PDC_CLIENT: Unable to read the server return values");

        PDC_update_transform_server_meta_index(output.client_index, output.ret);
    }

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(info->info.forward.handle, &output);

    FUNC_LEAVE(ret_value);
}
