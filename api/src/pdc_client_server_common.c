#include "mercury.h"
#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"

#include "pdc_interface.h"
#include "pdc_client_server_common.h"

// Thread
hg_thread_pool_t *hg_test_thread_pool_g;
hg_thread_mutex_t pdc_metadata_hash_table_mutex_g;

hg_atomic_int32_t close_server_g;

uint64_t pdc_id_seq_g = 1000000;
// actual value for each server is set by PDC_Server_init()

uint64_t PDCS_gen_obj_id()
{
    FUNC_ENTER(NULL);
    uint64_t ret_value;
    ret_value = pdc_id_seq_g++;
done:
    FUNC_LEAVE(ret_value);
}

#ifdef ENABLE_MULTITHREAD

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

inline int PDC_Server_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b)
{
    // UID
    if (a->user_id >= 0 && b->user_id >= 0 && a->user_id != b->user_id) 
        return -1;

    // Timestep
    if (a->time_step >= 0 && b->time_step >= 0 && a->time_step != b->time_step) 
        return -1;

    // Application name 
    if (a->app_name[0] != '\0' && b->app_name[0] != '\0' && strcmp(a->app_name, b->app_name) != 0) 
    /* if (a->app_name != NULL && b->app_name != NULL && strcmp(a->app_name, b->app_name) != 0) */ 
        return -1;

    // Object name
    if (a->obj_name[0] != '\0' && b->obj_name[0] != '\0' && strcmp(a->obj_name, b->obj_name) != 0) 
    /* if (a->obj_name != NULL && b->obj_name != NULL && strcmp(a->obj_name, b->obj_name) != 0) */ 
        return -1;

    return 0;
}

#ifdef IS_PDC_SERVER

/* HG_TEST_RPC_CB(insert_metadata_to_hash_table, handle) */
hg_return_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{

    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* printf("Got RPC request with name: %s\nHash=%d\n", in->obj_name, in->hash_value); */
    /* printf("Full name check: %s\n", &in->obj_name[507]); */

    pdc_metadata_t *metadata = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    strcpy(metadata->obj_name, in->obj_name);

    int32_t *hash_key = (int32_t*)malloc(sizeof(int32_t));
    *hash_key = in->hash_value;

    pdc_metadata_t *lookup_value;

    // Obtain lock for hash table
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", *hash_key); */
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, hash_key);
        // compare
        if (lookup_value != NULL) {
            if (PDC_Server_metadata_cmp(lookup_value, metadata) == 0) {
                printf("Same metadata exisit in current Metadata store!\n");
                ret_value = -1;
                out->ret = -1;
                goto done;
            }
        }

    }
    else {
        printf("metadata_hash_table_g not initilized!\n");
        ret_value = -1;
    }

    // Fill metadata structure
    // Generate object id (uint64_t)
    metadata->obj_id = PDCS_gen_obj_id();


    ret_value = hg_hash_table_insert(metadata_hash_table_g, hash_key, metadata);

    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);

    // Fill $out structure for returning the generated obj_id to client
    out->ret = metadata->obj_id;
    /* printf("Generated obj_id=%llu\n", out->ret); */


    fflush(stdout);

done:
    FUNC_LEAVE(ret_value);
}
#else

hg_return_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out) {return 0;}
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

    // TODO: use a seperated callback function for closing server
    if (strcmp("==PDC Close Server", in.obj_name) == 0) {
        hg_atomic_set32(&close_server_g, 1);
        out.ret = 1;
    }
    else {
        // Insert to hash table
        ret = insert_metadata_to_hash_table(&in, &out);
    }

    // Generate an object id as return value
    /* out.ret = PDCS_gen_obj_id(); */

    HG_Respond(handle, NULL, NULL, &out);
    /* printf("Returned %llu\n", out.ret); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

/* HG_TEST_RPC_CB(close_server, handle) */
/* { */
/*     FUNC_ENTER(NULL); */

/*     hg_return_t ret_value; */

/*     /1* Get input parameters sent on origin through on HG_Forward() *1/ */
/*     // Decode input */
/*     gen_obj_id_in_t in; */
/*     gen_obj_id_out_t out; */

/*     HG_Get_input(handle, &in); */
    
/*     close_server_g = 1; */

/*     out.ret = 1; */
/*     HG_Respond(handle, NULL, NULL, &out); */
/*     /1* printf("Returned %llu\n", out.ret); *1/ */

/*     HG_Free_input(handle, &in); */
/*     HG_Destroy(handle); */

/*     ret_value = HG_SUCCESS; */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */



HG_TEST_THREAD_CB(gen_obj_id)
/* HG_TEST_THREAD_CB(close_server) */
/* HG_TEST_THREAD_CB(insert_metadata_to_hash_table) */

hg_id_t
gen_obj_id_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "gen_obj_id", gen_obj_id_in_t, gen_obj_id_out_t, gen_obj_id_cb);
    /* ret_value = MERCURY_REGISTER(hg_class, "close_server", gen_obj_id_in_t, gen_obj_id_out_t, close_server_cb); */

done:
    FUNC_LEAVE(ret_value);
}
