#include "mercury.h"
#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"

#include "pdc_interface.h"
#include "pdc_client_server_common.h"

// Thread
hg_thread_pool_t *hg_test_thread_pool_g;

uint64_t pdc_id_seq_g = 1000;
// actual value for each server is set by PDC_Server_init()

uint64_t PDCS_gen_obj_id()
{
    FUNC_ENTER(NULL);
    uint64_t ret_value;
    ret_value = pdc_id_seq_g++;
done:
    FUNC_LEAVE(ret_value);
}

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

#ifdef IS_PDC_SERVER

/* HG_TEST_RPC_CB(insert_metadata_to_hash_table, handle) */
hg_return_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{

    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* printf("Got RPC request with name: %s\nHash=%d\n", in->obj_name, in->hash_value); */
    /* printf("Full name check: %s\n", &in->obj_name[507]); */

    hash_value_metadata_t *metadata = (hash_value_metadata_t*)malloc(sizeof(hash_value_metadata_t));

    // Generate object id (uint64_t)
    metadata->obj_id = PDCS_gen_obj_id();

    // Fill $out structure for returning the generated obj_id to client
    out->ret = metadata->obj_id;
    /* printf("Generated obj_id=%llu\n", out->ret); */

    int32_t *hash_key = (int32_t*)malloc(sizeof(int32_t));
    *hash_key = in->hash_value;

    if (metadata_hash_table_g != NULL) {
        ret_value = hg_hash_table_insert(metadata_hash_table_g, hash_key, metadata);
    }
    else {
        printf("metadata_hash_table_g not initilized!\n");
        ret_value = HG_INVALID_PARAM;
    }
    fflush(stdout);

done:
    FUNC_LEAVE(ret_value);
}
#else

hg_return_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out) {}
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
    
    // Insert to hash table
    insert_metadata_to_hash_table(&in, &out);

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


HG_TEST_THREAD_CB(gen_obj_id)
/* HG_TEST_THREAD_CB(insert_metadata_to_hash_table) */

hg_id_t
gen_obj_id_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "gen_obj_id", gen_obj_id_in_t, gen_obj_id_out_t, gen_obj_id_cb);

done:
    FUNC_LEAVE(ret_value);
}
