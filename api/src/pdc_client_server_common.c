#include "server/utlist.h"

#include "mercury.h"
#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"

#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "server/pdc_server.h"

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
    if (a->user_id >= 0 && b->user_id >= 0) {
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

    HG_Free_input(handle, &in);
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

HG_TEST_RPC_CB(region_lock, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    region_lock_in_t in;
    region_lock_out_t out;

    HG_Get_input(handle, &in);
    int ret;

    // TODO
    // Perform lock function
    ret = PDC_Server_region_lock(&in, &out);

    HG_Respond(handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER: gen_obj_id_cb(): returned %llu\n", out.ret); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// gen_reg_map_notification_cb(hg_handle_t handle)
HG_TEST_RPC_CB(gen_reg_map_notification, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    // Decode input
    gen_reg_map_notification_in_t in;
    gen_reg_map_notification_out_t out;

    HG_Get_input(handle, &in);
//  struct hg_info *info = HG_Get_info(handle);
//  hg_addr_t new_addr;
//  HG_Addr_dup(info->hg_class, info->addr, &new_addr);
    
    PDC_mapping_t *map_ptr = NULL;
    PDC_mapping_info_t *m_info_ptr = NULL;
    int m_success = 0;

    if(PDC_mapping_id == NULL) {
//      TODO: free space later
//      printf("==PDC SERVER: pdc region mapping list is currently null. \n");
        if(NULL == (PDC_mapping_id = (PDC_mapping_t **)malloc(sizeof(PDC_mapping_t *)))) {
            m_success = 1;
            printf("==PDC_SERVER: Failed to allocate region mapping list\n");
        }
        if(NULL == (map_ptr = (PDC_mapping_t *)malloc(sizeof(PDC_mapping_t)))) {
            m_success = 1;
            printf("==PDC_SERVER: Failed to allocate map_ptr\n");
        }
        if(NULL == (m_info_ptr = (PDC_mapping_info_t *)malloc(sizeof(PDC_mapping_info_t)))) {
            m_success = 1;
            printf("==PDC_SERVER: Failed to allocate m_info_ptr \n");
        }
        PDC_mapping_id[0] = map_ptr;
        PDC_LIST_INIT(&map_ptr->ids);
        pdc_num_reg = ATOMIC_VAR_INIT(1);
        map_ptr->mapping_count = ATOMIC_VAR_INIT(1);
        map_ptr->local_obj_id = in.local_obj_id;
        map_ptr->local_reg_id = in.local_reg_id;
        map_ptr->local_ndim = in.ndim;
        m_info_ptr->remote_obj_id = in.remote_obj_id;
        m_info_ptr->remote_reg_id = in.remote_reg_id;
        m_info_ptr->remote_ndim = in.ndim;
        m_info_ptr->bulk_handle = in.bulk_handle;
        PDC_LIST_INSERT_HEAD(&map_ptr->ids, m_info_ptr, entry);
//      printf("PDC SERVER: # of mapping region is %u\n", pdc_num_reg);
    }
    else {
//      printf("==PDC SERVER: pdc region mapping list is NOT null. \n");
        int found = 0;
//      printf("==PDC SERVER: # of mapping region is %u\n", pdc_num_reg);
        unsigned i = 0;
        for(i=0; i<pdc_num_reg; i++) {
            // found the origin region, which was mapped before
            if(PDC_mapping_id[i]->local_obj_id == in.local_obj_id && PDC_mapping_id[i]->local_reg_id==in.local_reg_id) {
                found = 1;
//              printf("==PDC SERVER: region %lld mapped before\n", in.from_region_id);
                PDC_mapping_info_t *tmp_ptr = NULL;
//              search the list of origin region
                PDC_mapping_t *tmp_mapping = PDC_mapping_id[i];
//              printf("==PDC SERVER: search list\n");
//              printf("origin = %lld\n", tmp_mapping->reg_id);
//              PDC_LIST_SEARCH(tmp_ptr, &tmp_mapping->ids, entry, tgt_reg_id, in.to_region_id); // not working
//              tmp_ptr = (&tmp_mapping->ids)->head;
                PDC_LIST_GET_FIRST(tmp_ptr, &tmp_mapping->ids);
                while(tmp_ptr!=NULL && tmp_ptr->remote_reg_id!=in.remote_reg_id) {
//                  printf("tgt region in list is %lld\n", tmp_ptr->remote_reg_id);
//                  printf("tgt region is %lld\n", in.remote_reg_id);
                    PDC_LIST_TO_NEXT(tmp_ptr, entry);
                }
/*
                if(tmp_ptr == NULL)
                    printf("reach the end of list\n");
                else
                    printf("tgt region is %lld\n", tmp_ptr->tgt_reg_id);
*/
                // target region not found, new mapping will be inserted to the list
                if(tmp_ptr == NULL) {
                    if(NULL == (tmp_ptr = (PDC_mapping_info_t *)malloc(sizeof(PDC_mapping_info_t)))) {
                        m_success = 1;
                        printf("==PDC_SERVER: Failed to allocate tmp_ptr \n");
                    }
                    atomic_fetch_add(&(PDC_mapping_id[i]->mapping_count), 1);
//                  printf("==PDC SERVER: create new mapping, mapping_count = % u\n", PDC_mapping_id[i]->mapping_count);
                    tmp_ptr->remote_obj_id = in.remote_obj_id;
                    tmp_ptr->remote_reg_id = in.remote_reg_id;
                    tmp_ptr->remote_ndim = in.ndim;
                    tmp_ptr->bulk_handle = in.bulk_handle;
                    PDC_LIST_INSERT_HEAD(&PDC_mapping_id[i]->ids, tmp_ptr, entry);
                }
                else {// same mapping stored in server already
                    m_success = 1;
                    printf("==PDC SERVER ERROR: mapping from %lld to %lld already exists\n", in.local_reg_id, in.remote_reg_id);
                }
            }
        } // end of for loop
        if(found == 0) {
//          printf("==PDC SERVER: new mapping\n");
            if(NULL == (map_ptr = (PDC_mapping_t *)malloc(sizeof(PDC_mapping_t)))) {
                m_success = 1;
                printf("==PDC_SERVER: Failed to allocate map_ptr\n");
            }
            if(NULL == (m_info_ptr = (PDC_mapping_info_t *)malloc(sizeof(PDC_mapping_info_t)))) {
                m_success = 1;
                printf("==PDC_SERVER: Failed to allocate m_info_ptr \n");
            }
            atomic_fetch_add(&pdc_num_reg, 1);
            printf("PDC SERVER: # of mapping region is %u\n", pdc_num_reg);
            PDC_mapping_id = (PDC_mapping_t **)realloc(PDC_mapping_id, pdc_num_reg*sizeof(PDC_mapping_t *));
            PDC_mapping_id[pdc_num_reg-1] = map_ptr;
            PDC_LIST_INIT(&map_ptr->ids);
            map_ptr->mapping_count = ATOMIC_VAR_INIT(1);
            map_ptr->local_obj_id = in.local_obj_id;
            map_ptr->local_reg_id = in.local_reg_id;
            map_ptr->local_ndim = in.ndim;
            m_info_ptr->remote_obj_id = in.remote_obj_id;
            m_info_ptr->remote_reg_id = in.remote_reg_id;
            m_info_ptr->remote_ndim = in.ndim;
            m_info_ptr->bulk_handle = in.bulk_handle;
            PDC_LIST_INSERT_HEAD(&map_ptr->ids, m_info_ptr, entry);
        }
    }
    
    // mapping success
    if(m_success == 0) {
        out.ret = 1;
        HG_Respond(handle, NULL, NULL, &out);
    }
    else {
        out.ret = 0;
        HG_Respond(handle, NULL, NULL, &out);
    }

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;
done:
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
