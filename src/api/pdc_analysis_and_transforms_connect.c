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

#include "pdc_analysis_and_transforms.h"

/* Forward References:: */
// Analysis and Transformations

hg_id_t                analysis_ftn_register_id_g;
hg_id_t                transform_ftn_register_id_g;
hg_id_t                server_transform_ftn_register_id_g;
hg_id_t                object_data_iterator_register_id_g;
static hg_return_t     client_register_iterator_rpc_cb(const struct hg_cb_info *info);
static hg_return_t     client_register_analysis_rpc_cb(const struct hg_cb_info *info);
static hg_return_t     client_register_transform_rpc_cb(const struct hg_cb_info *info);
static hg_return_t     client_forward_transform_rpc_cb(const struct hg_cb_info *info);

/* Client APIs */
// Registers an iterator
perr_t pdc_client_send_iter_recv_id(pdcid_t iter_id, pdcid_t *meta_id)
{
    uint64_t ret_value = SUCCEED;
    struct PDC_iterator_info *thisIter = NULL;
    struct my_rpc_state *my_rpc_state_p;

    obj_data_iterator_in_t in;
    hg_return_t hg_ret;
    int server_id = 0;
    int n_retry = 0;

    FUNC_ENTER(NULL);

    my_rpc_state_p = (struct my_rpc_state *)calloc(1,sizeof(struct my_rpc_state));
    if (my_rpc_state_p == NULL) {
        fprintf(stderr, "pdc_client_send_iter_recv_id(): Could not allocate my_rpc_state\n");
        ret_value = FAIL;
	goto done;
    }

    if ((PDC_Block_iterator_cache != NULL) && (iter_id > 0)) {
        struct PDC_obj_info *object_info;
        thisIter = &PDC_Block_iterator_cache[iter_id];
	/* Find the server association to the specific object */
	server_id = PDC_get_server_by_obj_id(thisIter->objectId, pdc_server_num_g);
	object_info = PDC_obj_get_info(thisIter->objectId);
	in.client_iter_id = iter_id;
	if (object_info != NULL) {
            in.object_id = object_info->meta_id;
	}
	else in.object_id = thisIter->objectId;
	in.reg_id = thisIter->reg_id;
	in.sliceCount = thisIter->sliceCount;
	in.sliceNext = thisIter->sliceNext;
	in.sliceResetCount = thisIter->sliceResetCount;
	in.elementsPerSlice = thisIter->elementsPerSlice;
	in.slicePerBlock = thisIter->slicePerBlock;
	in.elementsPerPlane = thisIter->elementsPerPlane;
	in.elementsPerBlock = thisIter->elementsPerBlock;
	in.skipCount = thisIter->skipCount;
	in.element_size = thisIter->element_size;
	in.srcBlockCount = thisIter->srcBlockCount;
	in.contigBlockSize = thisIter->contigBlockSize;
	in.totalElements = thisIter->totalElements;
        in.ndim   = thisIter->ndim;
        in.dims_0 = thisIter->dims[0];
        in.dims_1 = thisIter->dims[1];
        in.dims_2 = thisIter->dims[2];
        in.dims_3 = thisIter->dims[3];
        in.storageinfo = (int)((thisIter->storage_order << 8) | thisIter->pdc_datatype);
    }

    while (pdc_server_info_g[server_id].addr_valid != 1) {
        if (n_retry > 0) 
            break;
        if( PDC_Client_lookup_server(server_id) != SUCCEED) {
            printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n",
		   pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }
        n_retry++;
    }

    in.server_id = server_id;
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, object_data_iterator_register_id_g, &my_rpc_state_p->handle );
    hg_ret = HG_Forward(my_rpc_state_p->handle, client_register_iterator_rpc_cb , my_rpc_state_p , &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "pdc_client_send_iter_recv_id(): Could not start HG_Forward()\n");
	fflush(stderr);
        ret_value = FAIL;
	goto done;
    }

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);
    
    if (my_rpc_state_p->value == 0) {
       ret_value = FAIL;
        *meta_id = 0;
        goto done;
     }
    *meta_id = my_rpc_state_p->value;
    ret_value = SUCCEED;

 done:
    HG_Destroy(my_rpc_state_p->handle);
    free(my_rpc_state_p);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_register_iterator_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t ret_value = HG_SUCCESS;
    struct my_rpc_state *my_rpc_state_p = info->arg;
    obj_data_iterator_out_t output;
    
    FUNC_ENTER(NULL);

    if (info->ret == HG_SUCCESS) {
      ret_value = HG_Get_output(info->info.forward.handle, &output);
      if (ret_value != HG_SUCCESS) {
	  printf("PDC_CLIENT: register_iterator_rpc_cb(): Unable to read the server return values\n");
	  goto done;
      }
      my_rpc_state_p->value = output.server_iter_id;
      printf("Server returned iterator index = 0x%" PRIu64 "\n", output.server_iter_id);
    }

done:
    work_todo_g--;
    HG_Free_output(info->info.forward.handle, &output);
    FUNC_LEAVE(ret_value);
}

/* Send a name to server and receive an obj id */
perr_t pdc_client_register_obj_analysis(struct region_analysis_ftn_info *thisFtn, const char *func, const char *loadpath, pdcid_t iter_in, pdcid_t iter_out)
{
    perr_t ret_value = SUCCEED;
    uint32_t server_id = 0;
    hg_return_t hg_ret;
    analysis_ftn_in_t in;
    struct my_rpc_state *my_rpc_state_p;

    FUNC_ENTER(NULL);
    my_rpc_state_p = (struct my_rpc_state *)calloc(1,sizeof(struct my_rpc_state));
    if (my_rpc_state_p == NULL) {
        fprintf(stderr, "pdc_client_register_obj_analysis(): Could not allocate my_rpc_state\n");
        ret_value = FAIL;
	goto done;
    }

    int n_retry = 0;
    /* FIXME!
     * The server lookup should be associated with the input object 
     * (at a minimum) and/or with the out object.  Question: is
     * it possible or even likely that the input and output
     * objects can be co-located???
     *
     * One thing to remember is that when an object/region gets
     * mapped, there is a BULK descriptor created and exchanged
     * at that time.  In the case mentioned above, when the source
     * is of the mapping operation is directed to the same server
     * (the server selection is the result of a hashing function)
     * as the target, we choose the target server for analysis
     * and in doing so, we can communicate the pre-existing bulk
     * descriptor as part of the analysis callback.  We do something
     * similar for transforms, e.g. the source "size" may change from
     * the original region due to compression or possibly datatype
     * conversion; and the client creates a NEW bulk descriptor to
     * send to the server at the time of the lock release (and
     * after the local transform has taken place).
     *
     * Special case:  If NO input or output iterators
     * are defined (NULL iterators), then this client should
     * probably choose a server that spreads the client load
     * amonst the collection of available servers. One such
     * approach is to select the server number based on our
     * client MPI rank modulo the number of servers.
     */
    while (pdc_server_info_g[server_id].addr_valid != 1) {
        if (n_retry > 0)
            break;
        if( PDC_Client_lookup_server(server_id) != SUCCEED) {
            printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }
        n_retry++;
    }

    memset(&in,0,sizeof(in));
    in.ftn_name = func;
    in.loadpath = loadpath;
    in.iter_in  = iter_in;
    in.iter_out = iter_out;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, analysis_ftn_register_id_g, &my_rpc_state_p->handle);
    hg_ret = HG_Forward(my_rpc_state_p->handle, client_register_analysis_rpc_cb, my_rpc_state_p, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "pdc_client_register_obj_analysis(): Could not start HG_Forward()");
    }

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (my_rpc_state_p->value < 0) {
        ret_value = FAIL;
        goto done;
    }
    // Here, we should update the local analysis registry with the returned valued from my_rpc_state_p;
    thisFtn->meta_index = my_rpc_state_p->value;
    ret_value = SUCCEED;

done:
    HG_Destroy(my_rpc_state_p->handle);
    free(my_rpc_state_p);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_register_analysis_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t ret_value = HG_SUCCESS;
    struct my_rpc_state *my_rpc_state_p = info->arg;
    analysis_ftn_out_t output;
    
    FUNC_ENTER(NULL);

    if (info->ret == HG_SUCCESS) {
      ret_value = HG_Get_output(info->info.forward.handle, &output);
      if (ret_value != HG_SUCCESS) {
	  printf("PDC_CLIENT: register_analysis_rpc_cb(): Unable to read the server return values\n");
	  goto done;
      }
      my_rpc_state_p->value = output.remote_ftn_id;
      printf("Server returned analysis index = 0x%" PRIu64 "\n", output.remote_ftn_id);
    }

done:
    work_todo_g--;
    HG_Free_output(info->info.forward.handle, &output);
    FUNC_LEAVE(ret_value);
}

/* Send a name to server and receive an obj id */
perr_t pdc_client_register_obj_transform(const char *func, const char *loadpath, pdcid_t obj_id, int start_state, int next_state, int op_type, int when)
{
    perr_t ret_value = SUCCEED;
    uint32_t server_id = 0;
    hg_return_t hg_ret;
    transform_ftn_in_t in;
    struct PDC_obj_info *object_info;
    struct my_rpc_state *my_rpc_state_p;

    FUNC_ENTER(NULL);
    my_rpc_state_p = (struct my_rpc_state *)calloc(1,sizeof(struct my_rpc_state));
    if (my_rpc_state_p == NULL) {
        fprintf(stderr, "pdc_client_register_obj_analysis(): Could not allocate my_rpc_state\n");
        ret_value = FAIL;
	goto done;
    }
    /* Find the server associated with the input object */
    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_num_g);
    object_info = PDC_obj_get_info(obj_id);

    memset(&in,0,sizeof(in));
    in.ftn_name = func;
    in.loadpath = loadpath;
    if (object_info != NULL) {
      in.object_id = object_info->meta_id;
    } else in.object_id = obj_id;
    in.operation_type = when;
    in.start_state = start_state;
    in.next_state = next_state;
    in.op_type = op_type & 0xFF;
    in.when = when & 0xFF;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, transform_ftn_register_id_g, &my_rpc_state_p->handle);
    hg_ret = HG_Forward(my_rpc_state_p->handle, client_register_transform_rpc_cb, my_rpc_state_p, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "pdc_client_register_obj_transform(): Could not start HG_Forward()");
    }

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (my_rpc_state_p->value < 0) {
        ret_value = FAIL;
        goto done;
    }
    // Here, we should update the local registry with the returned valued from my_rpc_state_p;
    ret_value = SUCCEED;

done:
    HG_Destroy(my_rpc_state_p->handle);
    free(my_rpc_state_p);
    FUNC_LEAVE(ret_value);
}

/* Send a name to server and receive an obj id */
perr_t pdc_client_register_region_transform(const char *func, const char *loadpath,
					    pdcid_t src_region_id, pdcid_t dest_region_id, pdcid_t obj_id,
					    int start_state, int next_state, int op_type, int when, int client_index)
{
    perr_t ret_value = SUCCEED;
    uint32_t server_id = 0;
    hg_return_t hg_ret;
    transform_ftn_in_t in;
    struct PDC_obj_info *object_info;
    struct my_rpc_state *my_rpc_state_p;

    FUNC_ENTER(NULL);

    my_rpc_state_p = (struct my_rpc_state *)calloc(1,sizeof(struct my_rpc_state));
    if (my_rpc_state_p == NULL) {
        fprintf(stderr, "pdc_client_register_obj_analysis(): Could not allocate my_rpc_state\n");
        ret_value = FAIL;
	goto done;
    }
    /* Find the server associated with the input object */
    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_num_g);
    object_info = PDC_obj_get_info(obj_id);
    memset(&in,0,sizeof(in));
    in.ftn_name = func;
    in.loadpath = loadpath;
    if (object_info != NULL) {
      in.object_id = object_info->meta_id;
    } else in.object_id = obj_id;
    in.operation_type = when;
    in.start_state = start_state;
    in.next_state = next_state;
    in.op_type = op_type & 0xFF;
    in.when = when & 0xFF;
    in.client_index = client_index;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, transform_ftn_register_id_g, &my_rpc_state_p->handle);
    hg_ret = HG_Forward(my_rpc_state_p->handle, client_register_transform_rpc_cb, my_rpc_state_p, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "pdc_client_register_region_transform(): Could not start HG_Forward()");
    }

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (my_rpc_state_p->value < 0) {
        ret_value = FAIL;
        goto done;
    }
    // Here, we should update the local registry with the returned valued from my_rpc_state_p;
    ret_value = SUCCEED;

done:
    HG_Destroy(my_rpc_state_p->handle);
    free(my_rpc_state_p);
    FUNC_LEAVE(ret_value);
}


// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_register_transform_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t ret_value = HG_SUCCESS;
    struct my_rpc_state *my_rpc_state_p = info->arg;
    transform_ftn_out_t output;
    // analysis_ftn_out_t output;

    FUNC_ENTER(NULL);

    printf("Entered: %s\n", __FUNCTION__);
    if (info->ret == HG_SUCCESS) {
      ret_value = HG_Get_output(info->info.forward.handle, &output);
      if (ret_value != HG_SUCCESS) {
	  printf("PDC_CLIENT: register_analysis_rpc_cb(): Unable to read the server return values\n");
	  goto done;
      }

      printf("Server returned transform index = 0x%x, client_index = 0x%x\n", output.ret, output.client_index);
      pdc_update_transform_server_meta_index(output.client_index, output.ret);
    }

done:
    work_todo_g--;
    HG_Free_output(info->info.forward.handle, &output);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_forward_transform_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t ret_value = HG_SUCCESS;
    struct my_rpc_state *my_rpc_state_p = info->arg;
    transform_ftn_out_t output;
    // analysis_ftn_out_t output;

    FUNC_ENTER(NULL);

    printf("Entered: %s\n", __FUNCTION__);
    if (info->ret == HG_SUCCESS) {
      ret_value = HG_Get_output(info->info.forward.handle, &output);
      if (ret_value != HG_SUCCESS) {
	  printf("PDC_CLIENT: register_analysis_rpc_cb(): Unable to read the server return values\n");
	  goto done;
      }

      printf("Server returned transform index = 0x%x, client_index = 0x%x\n", output.ret, output.client_index);
      pdc_update_transform_server_meta_index(output.client_index, output.ret);
    }

done:
    work_todo_g--;
    HG_Free_output(info->info.forward.handle, &output);
    FUNC_LEAVE(ret_value);
}


