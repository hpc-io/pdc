#include "mercury.h"
#include "pdc_interface.h"
#include "pdc_client_server_common.h"

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

/*
 * The routine that sets up the routines that actually do the work.
 * This 'handle' parameter is the only value passed to this callback, but
 * Mercury routines allow us to query information about the context in which
 * we are called.
 *
 * This callback/handler triggered upon receipt of rpc request
 */
static hg_return_t
gen_obj_id_cb(hg_handle_t handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* Get input parameters sent on origin through on HG_Forward() */
    // Decode input
    gen_obj_id_in_t in;
    HG_Get_input(handle, &in);
    printf("Got RPC request with name: %s\n", in.obj_name);

    // Generate an object id as return value
    gen_obj_id_out_t out;
    out.ret = PDCS_gen_obj_id();

    // TODO: add callback function to insert the object metadata to DB
    HG_Respond(handle, NULL, NULL, &out);
    printf("Returned %llu\n", out.ret);

    HG_Destroy(handle);

    ret_value = HG_SUCCESS;

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
gen_obj_id_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "gen_obj_id", gen_obj_id_in_t, gen_obj_id_out_t, gen_obj_id_cb);

done:
    FUNC_LEAVE(ret_value);
}
