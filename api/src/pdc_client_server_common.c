#include "server/utlist.h"

#include "mercury.h"
#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"

#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"
#include "server/pdc_server.h"
#include "server/utlist.h"

// Thread
hg_thread_pool_t *hg_test_thread_pool_g;

hg_atomic_int32_t close_server_g;

uint64_t pdc_id_seq_g = PDC_SERVER_ID_INTERVEL;
// actual value for each server is set by PDC_Server_init()
//

#ifdef ENABLE_MULTITHREAD

extern struct hg_thread_work *
hg_core_get_thread_work(hg_handle_t handle);

// Macros for multi-thread callback, grabbed from Mercury/Testing/mercury_rpc_cb.c
#define HG_TEST_RPC_CB(func_name, handle) \
    static hg_return_t \
    func_name ## _thread_cb(hg_handle_t handle)

/* Assuming func_name_cb is defined, calling HG_TEST_THREAD_CB(func_name)
 * will define func_name_thread and func_name_thread_cb that can be used
 * to execute RPC callback from a thread
 */
#define HG_TEST_THREAD_CB(func_name) \
        static HG_THREAD_RETURN_TYPE \
        func_name ## _thread \
        (void *arg) \
        { \
            hg_handle_t handle = (hg_handle_t) arg; \
            hg_thread_ret_t thread_ret = (hg_thread_ret_t) 0; \
            \
            func_name ## _thread_cb(handle); \
            \
            return thread_ret; \
        } \
        hg_return_t \
        func_name ## _cb(hg_handle_t handle) \
        { \
            struct hg_thread_work *work = hg_core_get_thread_work(handle); \
            hg_return_t ret = HG_SUCCESS; \
            \
            work->func = func_name ## _thread; \
            work->args = handle; \
            hg_thread_pool_post(hg_test_thread_pool_g, work); \
            \
            return ret; \
        }
#else
#define HG_TEST_RPC_CB(func_name, handle) \
        hg_return_t \
        func_name ## _cb(hg_handle_t handle)
#define HG_TEST_THREAD_CB(func_name)

#endif // End of ENABLE_MULTITHREAD

uint32_t PDC_get_server_by_obj_id(uint64_t obj_id, int n_server)
{
    FUNC_ENTER(NULL);

    // TODO: need a smart way to deal with server number change
    uint32_t ret_value;
    ret_value  = (uint32_t)(obj_id / PDC_SERVER_ID_INTERVEL) - 1;
    ret_value %= n_server;

done:
    FUNC_LEAVE(ret_value);
}

static uint32_t pdc_hash_djb2(const char *pc)
{
        uint32_t hash = 5381, c;
        while (c = *pc++)
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        if (hash < 0)
            hash *= -1;

        return hash;
}

static uint32_t pdc_hash_sdbm(const char *pc)
{
        uint32_t hash = 0, c;
        while (c = (*pc++))
                hash = c + (hash << 6) + (hash << 16) - hash;
        if (hash < 0)
            hash *= -1;
        return hash;
}

uint32_t PDC_get_hash_by_name(const char *name)
{
    return pdc_hash_djb2(name); 
}

inline int PDC_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b)
{
    int ret = 0;
    // Timestep
    if (a->time_step >= 0 && b->time_step >= 0) {
        ret = (a->time_step - b->time_step);
        /* if (ret != 0) */ 
        /*     printf("==PDC_SERVER: timestep not equal\n"); */
    }
    if (ret != 0 ) return ret;

    // Object name
    if (a->obj_name[0] != '\0' && b->obj_name[0] != '\0') {
        ret = strcmp(a->obj_name, b->obj_name); 
        /* if (ret != 0) */ 
        /*     printf("==PDC_SERVER: obj_name not equal\n"); */
    }
    if (ret != 0 ) return ret;

    // UID
    if (a->user_id > 0 && b->user_id > 0) {
        ret = (a->user_id - b->user_id);
        /* if (ret != 0) */ 
        /*     printf("==PDC_SERVER: uid not equal\n"); */
    }
    if (ret != 0 ) return ret;

    // Application name 
    if (a->app_name[0] != '\0' && b->app_name[0] != '\0') {
        ret = strcmp(a->app_name, b->app_name);
        /* if (ret != 0) */ 
        /*     printf("==PDC_SERVER: app_name not equal\n"); */
    }

    return ret;
}

void PDC_print_metadata(pdc_metadata_t *a)
{
    if (a == NULL) {
        printf("==Empty metadata structure\n");
    }
    printf("================================\n");
    printf("  obj_id    = %llu\n", a->obj_id);
    printf("  uid       = %d\n",   a->user_id);
    printf("  app_name  = %s\n",   a->app_name);
    printf("  obj_name  = %s\n",   a->obj_name);
    printf("  time_step = %d\n",   a->time_step);
    printf("  ndim      = %d\n",   a->ndim);
    printf("  tags      = %s\n",   a->tags);
    printf("================================\n\n");
    fflush(stdout);
}

#ifndef IS_PDC_SERVER
// Dummy function for client to compile, real function is used only by server and code is in pdc_server.c
perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out) {return SUCCEED;}
/* perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out) {return SUCCEED;} */
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out) {return SUCCEED;}
perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out) {return SUCCEED;}
perr_t delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out) {return SUCCEED;}
perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out) {return SUCCEED;}
perr_t PDC_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out) {return SUCCEED;}
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs) {return SUCCEED;}
hg_class_t *hg_class_g;
#endif

/*
 * The routine that sets up the routines that actually do the work.
 * This 'handle' parameter is the only value passed to this callback, but
 * Mercury routines allow us to query information about the context in which
 * we are called.
 *
 * This callback/handler triggered upon receipt of rpc request
 */
/* static hg_return_t */
/* gen_obj_id_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(gen_obj_id, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    gen_obj_id_in_t in;
    gen_obj_id_out_t out;

    HG_Get_input(handle, &in);
    int ret;

    // Insert to hash table
    ret = insert_metadata_to_hash_table(&in, &out);

    HG_Respond(handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER: gen_obj_id_cb(): returned %llu\n", out.ret); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* client_test_connect_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(client_test_connect, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    client_test_connect_in_t  in;
    client_test_connect_out_t out;

    HG_Get_input(handle, &in);
    out.ret = in.client_id + 100000;

    HG_Respond(handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER: client_test_connect(): Returned %llu\n", out.ret); */
    fflush(stdout);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// send_obj_name_marker_cb(hg_handle_t handle)
/* HG_TEST_RPC_CB(send_obj_name_marker, handle) */
/* { */
/*     FUNC_ENTER(NULL); */

/*     hg_return_t ret_value; */

/*     /1* Get input parameters sent on origin through on HG_Forward() *1/ */
/*     // Decode input */
/*     send_obj_name_marker_in_t  in; */
/*     send_obj_name_marker_out_t out; */

/*     HG_Get_input(handle, &in); */
    
/*     // Insert to object marker hash table */
/*     insert_obj_name_marker(&in, &out); */

/*     /1* out.ret = 1; *1/ */
/*     HG_Respond(handle, NULL, NULL, &out); */
/*     /1* printf("==PDC_SERVER: send_obj_name_marker() Returned %llu\n", out.ret); *1/ */
/*     /1* fflush(stdout); *1/ */

/*     HG_Free_input(handle, &in); */
/*     HG_Destroy(handle); */

/*     ret_value = HG_SUCCESS; */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */


/* static hg_return_t */
/* metadata_query_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(metadata_query, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    metadata_query_in_t  in;
    metadata_query_out_t out;

    pdc_metadata_t *query_result = NULL;

    // TODO check DHT for query result
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Received query with name: %s, hash value: %u\n", in.obj_name, in.hash_value); */
    /* fflush(stdout); */
    PDC_Server_search_with_name_hash(in.obj_name, in.hash_value, &query_result);

    if (query_result != NULL) {
        out.ret.user_id        = query_result->user_id;
        out.ret.obj_id         = query_result->obj_id;
        out.ret.time_step      = query_result->time_step;
        out.ret.obj_name       = query_result->obj_name;
        out.ret.app_name       = query_result->app_name;
        out.ret.tags           = query_result->tags;
        // TODO add location info
        out.ret.data_location  = "/test/location"; 
    }
    else {
        out.ret.user_id        = -1;
        out.ret.obj_id         = 0;
        out.ret.time_step      = -1;
        out.ret.obj_name       = "invalid";
        out.ret.app_name       = "invalid";
        out.ret.tags           = "invalid";
        // TODO add location info
        out.ret.data_location  = "/test/location"; 
    }
    HG_Respond(handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER: metadata_query_cb(): Returned obj_name=%s, obj_id=%llu\n", out.ret.obj_name, out.ret.obj_id); */
    /* fflush(stdout); */

//    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_delete_by_id_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_delete_by_id, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    metadata_delete_by_id_in_t  in;
    metadata_delete_by_id_out_t out;

    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got delete_by_id request: hash=%d, obj_id=%llu\n", in.hash_value, in.obj_id); */


    delete_metadata_by_id(&in, &out);
    
    /* out.ret = 1; */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}


/* static hg_return_t */
// metadata_delete_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_delete, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    metadata_delete_in_t  in;
    metadata_delete_out_t out;

    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got delete request: hash=%d, obj_id=%llu\n", in.hash_value, in.obj_id); */


    delete_metadata_from_hash_table(&in, &out);
    
    /* out.ret = 1; */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_update_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_update, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    metadata_update_in_t  in;
    metadata_update_out_t out;

    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got update request: hash=%d, obj_id=%llu\n", in.hash_value, in.obj_id); */


    PDC_Server_update_metadata(&in, &out);
    
    /* out.ret = 1; */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// close_server_cb(hg_handle_t handle)
HG_TEST_RPC_CB(close_server, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    close_server_in_t  in;
    close_server_out_t out;

    HG_Get_input(handle, &in);
    /* printf("\n==PDC_SERVER: Close server request received\n"); */
    /* fflush(stdout); */

    // Set close server marker
    while (hg_atomic_get32(&close_server_g) == 0 ) {
        hg_atomic_set32(&close_server_g, 1);
    }

    out.ret = 1;
    HG_Respond(handle, NULL, NULL, &out);


    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

//enter this function, transfer is done
static hg_return_t
region_lock_bulk_transfer_cb (const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret = HG_SUCCESS;
    region_lock_out_t out;
    
    struct lock_bulk_args *bulk_args = (struct lock_bulk_args *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
   
    if (hg_cb_info->ret == HG_CANCELED) {
        printf("HG_Bulk_transfer() was successfully canceled\n");
        out.ret = 0;
    } else if (hg_cb_info->ret != HG_SUCCESS) {
        printf("Error in region_lock_bulk_transfer_cb()");
        ret = HG_PROTOCOL_ERROR;
        out.ret = 0;
    }
    // TODO
    // Perform lock function
    PDC_Server_region_lock(&(bulk_args->in), &out);
 
    HG_Respond(bulk_args->handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER: region_lock_bulk_transfer_cb(): returned %llu\n", out.ret); */

    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Bulk_free(local_bulk_handle);
done:
    HG_Destroy(bulk_args->handle);
    free(bulk_args);
    return ret;
}

HG_TEST_RPC_CB(region_lock, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    hg_return_t hg_ret;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    region_lock_in_t in;
    region_lock_out_t out;

    HG_Get_input(handle, &in);

    if(in.access_type==READ || in.lock_op==PDC_LOCK_OP_OBTAIN || in.mapping==0) {
//        printf("READ or PDC_LOCK_OP_OBTAIN  or no mapping callback or no mapping\n\n\n");
        // TODO
        // Perform lock function
        perr_t ret = PDC_Server_region_lock(&in, &out);

        HG_Respond(handle, NULL, NULL, &out);
        /* printf("==PDC_SERVER: gen_obj_id_cb(): returned %llu\n", out.ret); */

        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }
    // do data tranfer if it is write lock release. Respond to client in callback after data transfer is done
    else {
//        printf("release with mapping\n\n\n");
        hg_op_id_t hg_bulk_op_id;
        struct hg_info *hg_info = NULL;
        /* Get info from handle */
        hg_info = HG_Get_info(handle);

        struct lock_bulk_args *bulk_args = NULL;
        bulk_args = (struct lock_bulk_args *) malloc(sizeof(struct lock_bulk_args));
        /* Keep handle to pass to callback */
        bulk_args->handle = handle;
        bulk_args->in = in;
        
        pdc_metadata_t *target_obj = pdc_find_metadata_by_id(in.obj_id);
        if (target_obj == NULL) {
            printf("==PDC_SERVER: PDC_Server_region_lock - requested object (id=%llu) does not exist\n", in.obj_id);
            out.ret = 0;
            goto done;
        }
        
        int found = 0;
        if(target_obj->region_map_head != NULL) {
            region_map_t *elt;
            DL_FOREACH(target_obj->region_lock_head, elt) {
                // find the mapping region list
                if(elt->local_obj_id == in.obj_id && elt->local_reg_id==in.local_reg_id) {
                    found = 1;
                    hg_bulk_t origin_bulk_handle = HG_BULK_NULL;
                    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
                    
                    origin_bulk_handle = elt->bulk_handle;
                    hg_size_t   data_size = HG_Bulk_get_size(origin_bulk_handle);
                    
                    // start data copy from client to server
                    // allocate contiguous space in the server or RDMA later
                    void *buf_ptrs = NULL;
                    if(elt->local_data_type == PDC_DOUBLE)
                        buf_ptrs = (void *)malloc(data_size * sizeof(double));
                    else if(elt->local_data_type == PDC_FLOAT)
                        buf_ptrs = (void *)malloc(data_size * sizeof(float));
                    else if(elt->local_data_type == PDC_INT)
                        buf_ptrs = (void *)malloc(data_size * sizeof(int));
                    else
                        PGOTO_ERROR(FAIL, "local data type is not supported yet");
                    if(buf_ptrs == NULL)
                        PGOTO_ERROR(FAIL, "cannot allocate space for data copy to server");
                    
                    /* Create a new block handle to read the data */
                    hg_ret = HG_Bulk_create(hg_info->hg_class, 1, &buf_ptrs, &data_size, HG_BULK_READWRITE, &local_bulk_handle);
                    if (hg_ret != HG_SUCCESS) {
                        PGOTO_ERROR(FAIL, "Could not create bulk data handle\n");
                    }
                    /* Pull bulk data */
                    hg_ret = HG_Bulk_transfer(hg_info->context, region_lock_bulk_transfer_cb, bulk_args, HG_BULK_PULL, elt->local_addr, origin_bulk_handle, 0, local_bulk_handle, 0, data_size, &hg_bulk_op_id);
                    if (hg_ret != HG_SUCCESS) {
                        PGOTO_ERROR(FAIL, "Could not read bulk data\n");  //printf or PGOTO_ERROR???????????????
                    }
                    // call back to client means data transfer is done and release the unlock
                    // TODO: send notification to remote regions to start data movement
                    // bulk_todo_g = 1;
                    // PDC_Client_check_bulk(send_class_g, send_context_g); //??????????????????????need that?
                }
            }
        }
        if(found == 0) {
            printf("==PDC SERVER ERROR: Could not find local region %lld in server\n", in.local_reg_id);
            fflush(stdout);
        }
    }
done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// gen_obj_unmap_notification_cb(hg_handle_t handle)
HG_TEST_RPC_CB(gen_obj_unmap_notification, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    // Decode input
    gen_obj_unmap_notification_in_t in;
    gen_obj_unmap_notification_out_t out;
    HG_Get_input(handle, &in);

    unsigned i = 0;
    int found = 0;
    for(i=0; i<pdc_num_reg; i++) {
        // found the origin region, which was mapped before
        /*
        if(PDC_mapping_id[i]->local_obj_id == in.local_obj_id)  {
            PDC_mapping_t *map_ptr = PDC_mapping_id[i];
            while(!PDC_LIST_IS_EMPTY(&map_ptr->ids)) {
                PDC_mapping_info_t *info_ptr = (&map_ptr->ids)->head;
                PDC_LIST_REMOVE(info_ptr, entry);
            }
        }
         */
    }
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;
done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// gen_reg_unmap_notification_cb(hg_handle_t handle)
HG_TEST_RPC_CB(gen_reg_unmap_notification, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    // Decode input
    gen_reg_unmap_notification_in_t in;
    gen_reg_unmap_notification_out_t out;
}

/* static hg_return_t */
// gen_reg_map_notification_cb(hg_handle_t handle)
HG_TEST_RPC_CB(gen_reg_map_notification, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;;

    // Decode input
    gen_reg_map_notification_in_t in;
    gen_reg_map_notification_out_t out;

    HG_Get_input(handle, &in);
    struct hg_info *info = HG_Get_info(handle);
//    PDC_Server_region_map(&in, &out, info);

    pdc_metadata_t *target_obj = pdc_find_metadata_by_id(in.local_obj_id);
    if (target_obj == NULL) {
        printf("==PDC_SERVER: PDC_Server_region_map - requested object (id=%llu) does not exist\n", in.local_obj_id);
        out.ret = 0;
        goto done;
    }
    
    int found = 0;
    if(target_obj->region_map_head != NULL) {
        region_map_t *elt;
        DL_FOREACH(target_obj->region_lock_head, elt) {
            if(in.local_obj_id==elt->local_obj_id && in.local_reg_id==elt->local_reg_id) {
                found = 1;
                region_map_t *map_ptr = target_obj->region_lock_head;
                PDC_mapping_info_t *tmp_ptr;
                PDC_LIST_GET_FIRST(tmp_ptr, &map_ptr->ids);
                while(tmp_ptr!=NULL && (tmp_ptr->remote_reg_id!=in.remote_reg_id || tmp_ptr->remote_obj_id!=in.remote_obj_id)) {
                    printf("tgt region in list is %lld\n", tmp_ptr->remote_reg_id);
                    printf("tgt region is %lld\n", in.remote_reg_id);
                    PDC_LIST_TO_NEXT(tmp_ptr, entry);
                }
                if(tmp_ptr!=NULL) {
                    printf("==PDC SERVER ERROR: mapping from obj %lld (region %lld) to obj %lld (reg %lld) already exists\n", in.local_obj_id, in.local_reg_id, in.remote_obj_id, in.remote_reg_id);
                    out.ret = 0;
                    goto done;
                }
                else {
                    PDC_mapping_info_t *m_info_ptr = (PDC_mapping_info_t *)malloc(sizeof(PDC_mapping_info_t));
                    m_info_ptr->remote_obj_id = in.remote_obj_id;
                    m_info_ptr->remote_reg_id = in.remote_reg_id;
                    m_info_ptr->remote_ndim = in.ndim;
                    PDC_LIST_INSERT_HEAD(&map_ptr->ids, m_info_ptr, entry);
                    atomic_fetch_add(&(map_ptr->mapping_count), 1);
                    out.ret = 0;
                }
            }
        }
    }
    if(found == 0) {
        region_map_t *map_ptr = (region_map_t *)malloc(sizeof(region_map_t));
        PDC_LIST_INIT(&map_ptr->ids);
        map_ptr->mapping_count = ATOMIC_VAR_INIT(1);
        map_ptr->local_obj_id = in.local_obj_id;
        map_ptr->local_reg_id = in.local_reg_id;
        map_ptr->local_ndim = in.ndim;
        HG_Bulk_ref_incr(in.bulk_handle);
        map_ptr->local_data_type = in.local_type;
        struct hg_info *info = HG_Get_info(handle);
        HG_Addr_dup(info->hg_class, info->addr, &(map_ptr->local_addr));
        map_ptr->bulk_handle = in.bulk_handle;
        
        PDC_mapping_info_t *m_info_ptr = (PDC_mapping_info_t *)malloc(sizeof(PDC_mapping_info_t));
        m_info_ptr->remote_obj_id = in.remote_obj_id;
        m_info_ptr->remote_reg_id = in.remote_reg_id;
        m_info_ptr->remote_ndim = in.ndim;
        PDC_LIST_INSERT_HEAD(&map_ptr->ids, m_info_ptr, entry);
        
        LL_APPEND(target_obj->region_map_head, map_ptr);
        out.ret = 0;
    }

done:
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}


// Bulk
/* static hg_return_t */
/* query_partial_cb(hg_handle_t handle) */
// Server execute
HG_TEST_RPC_CB(query_partial, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;
    hg_return_t hg_ret;


    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    metadata_query_transfer_in_t in;
    metadata_query_transfer_out_t out;

    HG_Get_input(handle, &in);

    out.ret = -1;

    // bulk
    // Create a bulk descriptor
    hg_bulk_t bulk_handle = HG_BULK_NULL;

    int i;
    void  **buf_ptrs;
    size_t *buf_sizes;

    uint32_t *n_meta_ptr, n_buf;
    n_meta_ptr = (uint32_t*)malloc(sizeof(uint32_t));

    PDC_Server_get_partial_query_result(&in, n_meta_ptr, &buf_ptrs);

    /* printf("query_partial_cb: n_meta=%u\n", *n_meta_ptr); */

    // No result found
    if (*n_meta_ptr == 0) {
        out.bulk_handle = HG_BULK_NULL;
        out.ret = 0;
        printf("No objects returned for the query\n");
        ret_value = HG_Respond(handle, NULL, NULL, &out);
        goto done;
    }

    n_buf = *n_meta_ptr;

    buf_sizes = (size_t*)malloc( (n_buf+1) * sizeof(size_t));
    for (i = 0; i < *n_meta_ptr; i++) {
        buf_sizes[i] = sizeof(pdc_metadata_t);
    }
    // TODO: free buf_sizes

    // Note: it seems Mercury bulk transfer has issues if the total transfer size is less
    //       than 3862 bytes in Eager Bulk mode, so need to add some padding data 
    /* pdc_metadata_t *padding; */
    /* if (*n_meta_ptr < 11) { */
    /*     size_t padding_size; */
    /*     /1* padding_size = (10 - *n_meta_ptr) * sizeof(pdc_metadata_t); *1/ */
    /*     padding_size = 5000 * sizeof(pdc_metadata_t); */
    /*     padding = malloc(padding_size); */
    /*     memcpy(padding, buf_ptrs[0], sizeof(pdc_metadata_t)); */
    /*     buf_ptrs[*n_meta_ptr] = padding; */
    /*     buf_sizes[*n_meta_ptr] = padding_size; */
    /*     n_buf++; */
    /* } */

    // Fix when Mercury output in HG_Respond gets too large and cannot be transfered
    // hg_set_output(): Output size exceeds NA expected message size
    pdc_metadata_t *large_serial_meta_buf;
    if (*n_meta_ptr > 80) {
        large_serial_meta_buf = (pdc_metadata_t*)malloc( sizeof(pdc_metadata_t) * (*n_meta_ptr) );
        for (i = 0; i < *n_meta_ptr; i++) {
            memcpy(&large_serial_meta_buf[i], buf_ptrs[i], sizeof(pdc_metadata_t) );
        }
        buf_ptrs[0]  = large_serial_meta_buf;
        buf_sizes[0] = sizeof(pdc_metadata_t) * (*n_meta_ptr);
        n_buf = 1;
    }

    // Create bulk handle
    hg_ret = HG_Bulk_create(hg_class_g, n_buf, buf_ptrs, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        return EXIT_FAILURE;
    }

    // Fill bulk handle and return number of metadata that satisfy the query 
    out.bulk_handle = bulk_handle;
    out.ret = *n_meta_ptr;

    // Send bulk handle to client
    /* printf("query_partial_cb(): Sending bulk handle to client\n"); */
    /* fflush(stdout); */
    /* HG_Respond(handle, PDC_server_bulk_respond_cb, NULL, &out); */
    ret_value = HG_Respond(handle, NULL, NULL, &out);


done:
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}





HG_TEST_THREAD_CB(gen_obj_id)
/* HG_TEST_THREAD_CB(send_obj_name_marker) */
HG_TEST_THREAD_CB(client_test_connect)
HG_TEST_THREAD_CB(metadata_query)
HG_TEST_THREAD_CB(metadata_delete)
HG_TEST_THREAD_CB(metadata_delete_by_id)
HG_TEST_THREAD_CB(metadata_update)
HG_TEST_THREAD_CB(close_server)
HG_TEST_THREAD_CB(gen_reg_map_notification)
HG_TEST_THREAD_CB(gen_reg_unmap_notification)
HG_TEST_THREAD_CB(gen_obj_unmap_notification)
HG_TEST_THREAD_CB(region_lock)
HG_TEST_THREAD_CB(query_partial)

hg_id_t
gen_obj_id_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "gen_obj_id", gen_obj_id_in_t, gen_obj_id_out_t, gen_obj_id_cb);

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
client_test_connect_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "client_test_connect", client_test_connect_in_t, client_test_connect_out_t, client_test_connect_cb);

done:
    FUNC_LEAVE(ret_value);
}

/* hg_id_t */
/* send_obj_name_marker_register(hg_class_t *hg_class) */
/* { */
/*     FUNC_ENTER(NULL); */

/*     hg_id_t ret_value; */
/*     ret_value = MERCURY_REGISTER(hg_class, "send_obj_name_marker", send_obj_name_marker_in_t, send_obj_name_marker_out_t, send_obj_name_marker_cb); */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

hg_id_t
metadata_query_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "metadata_query", metadata_query_in_t, metadata_query_out_t, metadata_query_cb);

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
metadata_update_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "metadata_update", metadata_update_in_t, metadata_update_out_t, metadata_update_cb);

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
metadata_delete_by_id_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "metadata_delete_by_id", metadata_delete_by_id_in_t, metadata_delete_by_id_out_t, metadata_delete_by_id_cb);

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
metadata_delete_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "metadata_delete", metadata_delete_in_t, metadata_delete_out_t, metadata_delete_cb);

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
close_server_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "close_server", close_server_in_t, close_server_out_t, close_server_cb);

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
gen_reg_map_notification_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "gen_reg_map_notification", gen_reg_map_notification_in_t, gen_reg_map_notification_out_t, gen_reg_map_notification_cb);


done:
    FUNC_LEAVE(ret_value);
}


hg_id_t
gen_reg_unmap_notification_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "gen_reg_unmap_notification", gen_reg_unmap_notification_in_t, gen_reg_unmap_notification_out_t, gen_reg_unmap_notification_cb);


done:
    FUNC_LEAVE(ret_value);
}


hg_id_t
gen_obj_unmap_notification_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "gen_obj_unmap_notification", gen_obj_unmap_notification_in_t, gen_obj_unmap_notification_out_t, gen_obj_unmap_notification_cb);


done:
    FUNC_LEAVE(ret_value);
}


hg_id_t
region_lock_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "region_lock", region_lock_in_t, region_lock_out_t, region_lock_cb);

done:
    FUNC_LEAVE(ret_value);
}


hg_id_t
query_partial_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "query_partial", metadata_query_transfer_in_t, metadata_query_transfer_out_t, query_partial_cb);

done:
    FUNC_LEAVE(ret_value);
}
