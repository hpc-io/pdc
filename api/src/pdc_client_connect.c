#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* #define ENABLE_MPI 1 */
#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

#include "mercury.h"
#include "mercury_macros.h"

#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

int  pdc_client_mpi_rank_g = 0;
int  pdc_client_mpi_size_g = 1;

int  pdc_server_num_g;
pdc_server_info_t *pdc_server_info = NULL;

static hg_id_t gen_obj_id_client_id_g;
static int work_todo_g = 0;

int PDC_Client_read_server_addr_from_file()
{
    FUNC_ENTER(NULL);

    int ret_value;
    int i = 0;
    char  *p;
    FILE *na_config = NULL;
    na_config = fopen(pdc_server_cfg_name, "r");
    if (!na_config) {
        fprintf(stderr, "Could not open config file from default location: %s\n", pdc_server_cfg_name);
        exit(0);
    }
    char n_server_string[PATH_MAX];
    // Get the first line as $pdc_server_num_g
    fgets(n_server_string, PATH_MAX, na_config);
    pdc_server_num_g = atoi(n_server_string);

    // Allocate $pdc_server_info
    pdc_server_info = (pdc_server_info_t*)malloc(sizeof(pdc_server_info_t) * pdc_server_num_g);

    while (fgets(pdc_server_info[i].addr_string, ADDR_MAX, na_config)) {
        p = strrchr(pdc_server_info[i].addr_string, '\n');
        if (p != NULL) *p = '\0';
        /* printf("%s", pdc_server_info[i]->addr_string); */
        i++;
    }
    fclose(na_config);

    if (i != pdc_server_num_g) {
        printf("Invalid number of servers in server.cfg\n");
        exit(0);
    }

    ret_value = i;

done:
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* printf("Entered client_rpc_cb()"); */
    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    gen_obj_id_out_t output;
    ret_value = HG_Get_output(handle, &output);
    /* printf("Return value=%llu\n", output.ret); */
    client_lookup_args->obj_id = output.ret;

    work_todo_g--;

done:
    FUNC_LEAVE(ret_value);
}

// Callback function for HG_Addr_lookup()
// Start RPC connection
static hg_return_t
client_lookup_cb(const struct hg_cb_info *callback_info)
{

    FUNC_ENTER(NULL);

    /* printf("Entered client_lookup_cb()"); */
    hg_return_t ret_value;

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args *) callback_info->arg;
    client_lookup_args->hg_target_addr = callback_info->info.lookup.addr;

    /* Create HG handle bound to target */
    hg_handle_t handle;
    HG_Create(client_lookup_args->hg_context, client_lookup_args->hg_target_addr, gen_obj_id_client_id_g, &handle);

    /* Fill input structure */
    gen_obj_id_in_t in;
    in.obj_name= client_lookup_args->obj_name;

    /* printf("Sending input to target\n"); */
    ret_value = HG_Forward(handle, client_rpc_cb, client_lookup_args, &in);
    if (ret_value != HG_SUCCESS) {
        fprintf(stderr, "client_lookup_cb(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

done:
    FUNC_LEAVE(ret_value);
}

// Init Mercury class and context
// Register gen_obj_id rpc
perr_t PDC_Client_mercury_init(hg_class_t **hg_class, hg_context_t **hg_context, int port)
{

    FUNC_ENTER(NULL);

    perr_t ret_value;

    char na_info_string[PATH_MAX];
    sprintf(na_info_string, "cci+tcp://%d", port);
    if (!na_info_string) {
        fprintf(stderr, HG_PORT_NAME " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"cci+tcp://22222\"\n");
        exit(0);
    }
    /* printf("NA: %s\n", na_info_string); */

    /* Initialize Mercury with the desired network abstraction class */
    printf("Using %s\n", na_info_string);
    *hg_class = HG_Init(na_info_string, NA_FALSE);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        goto done;
    }

    /* Create HG context */
    *hg_context = HG_Context_create(*hg_class);

    // Register RPC
    gen_obj_id_client_id_g = gen_obj_id_register(*hg_class);

    ret_value = 0;

done:
    FUNC_LEAVE(ret_value);
}

// Check if all work has been processed
// Using global variable $work_todo_g
perr_t PDC_Client_check_response(hg_context_t **hg_context)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    hg_return_t hg_ret;
    do {
        unsigned int actual_count = 0;
        do {
            hg_ret = HG_Trigger(*hg_context, 0 /* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* printf("actual_count=%d\n",actual_count); */
        /* Do not try to make progress anymore if we're done */
        if (work_todo_g <= 0)  break;

        hg_ret = HG_Progress(*hg_context, HG_MAX_IDLE_TIME);
    } while (hg_ret == HG_SUCCESS);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
uint64_t PDC_Client_send_name_recv_id(int server_id, int port, const char *obj_name)
{

    FUNC_ENTER(NULL);

    uint64_t ret_value;

    hg_class_t   *hg_class = NULL;
    hg_context_t *hg_context = NULL;
    hg_return_t  hg_ret = 0;

    // Init Mercury network connection
    PDC_Client_mercury_init(&hg_class, &hg_context, port);
    if (hg_class == NULL || hg_context == NULL) {
        printf("Error with Mercury Init, exiting...\n");
        exit(0);
    }

    // Fill lookup args
    struct client_lookup_args lookup_args;
    lookup_args.hg_class   = hg_class;
    lookup_args.hg_context = hg_context;
    lookup_args.obj_name   = obj_name;
    lookup_args.obj_id     = -1;

    // Initiate server lookup and send obj name in client_lookup_cb()
    work_todo_g = 1;
    char *target_addr_string = pdc_server_info[server_id].addr_string;
    hg_ret = HG_Addr_lookup(hg_context, client_lookup_cb, &lookup_args, target_addr_string, HG_OP_ID_IGNORE);

    // Wait for response from server
    PDC_Client_check_response(&hg_context);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == -1) {
        printf("Error obtaining obj id from PDC server!\n");
    }
    
    /* Finalize Mercury*/
    HG_Addr_free(hg_class, lookup_args.hg_target_addr);
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);

    ret_value = lookup_args.obj_id;

done:
    FUNC_LEAVE(ret_value);
}

