#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* #define ENABLE_MPI 1 */
#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

#include "mercury.h"
#include "mercury_request.h"
#include "mercury_macros.h"

#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

int  pdc_client_mpi_rank_g = 0;
int  pdc_client_mpi_size_g = 1;

int  pdc_server_num_g;
pdc_server_info_t *pdc_server_info_g = NULL;

static int             mercury_has_init_g = 0;
static hg_class_t     *send_class_g = NULL;
static hg_context_t   *send_context_g = NULL;
static int             work_todo_g = 0;
static hg_id_t         gen_obj_register_id_g;


static int pdc_hash_djb2(const char *pc) {
        int hash = 5381, c;
        while (c = *pc++)
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        if (hash < 0) 
            hash *= -1;
        
        return hash;
}

static int pdc_hash_sdbm(const char *pc) {
        int hash = 0, c;
        while (c = (*pc++)) 
                hash = c + (hash << 6) + (hash << 16) - hash;
        if (hash < 0) 
            hash *= -1;
        return hash;
}


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

    // Allocate $pdc_server_info_g
    pdc_server_info_g = (pdc_server_info_t*)malloc(sizeof(pdc_server_info_t) * pdc_server_num_g);
    /* for (i = 0; i < pdc_server_num_g; i++) { */
        /* pdc_server_info_g[i].addr_valid = 0; */
        /* pdc_server_info_g[i].rpc_handle_valid = 0; */
    /* } */

    while (fgets(pdc_server_info_g[i].addr_string, ADDR_MAX, na_config)) {
        p = strrchr(pdc_server_info_g[i].addr_string, '\n');
        if (p != NULL) *p = '\0';
        /* printf("%s", pdc_server_info_g[i]->addr_string); */
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

    hg_return_t ret_value = HG_SUCCESS;

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args *) callback_info->arg;
    int server_id = client_lookup_args->server_id;
    pdc_server_info_g[server_id].addr = callback_info->info.lookup.addr;
    pdc_server_info_g[server_id].addr_valid = 1;

    // Create HG handle if needed
    int handle_valid = pdc_server_info_g[server_id].rpc_handle_valid;
    if (handle_valid != 1) {
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_register_id_g, &pdc_server_info_g[server_id].rpc_handle);
        pdc_server_info_g[server_id].rpc_handle_valid = 1;
    }

    // Fill input structure
    gen_obj_id_in_t in;
    in.obj_name= client_lookup_args->obj_name;
    in.hash_value = pdc_hash_djb2(client_lookup_args->obj_name);
    /* printf("Hash(%s) = %d\n", client_lookup_args->obj_name, in.hash_value); */

    /* printf("Sending input to target\n"); */
    ret_value = HG_Forward(pdc_server_info_g[server_id].rpc_handle, client_rpc_cb, client_lookup_args, &in);
    if (ret_value != HG_SUCCESS) {
        fprintf(stderr, "client_lookup_cb(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
send_req_to_server(struct client_lookup_args *client_lookup_args, int server_id)
{

    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    // Create HG handle if needed
    int handle_valid = pdc_server_info_g[server_id].rpc_handle_valid;
    if (handle_valid != 1) {
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_register_id_g, &pdc_server_info_g[server_id].rpc_handle);
        pdc_server_info_g[server_id].rpc_handle_valid = 1;
    }

    /* Fill input structure */
    gen_obj_id_in_t in;
    in.obj_name= client_lookup_args->obj_name;

    /* printf("Sending input to target\n"); */
    ret_value = HG_Forward(pdc_server_info_g[server_id].rpc_handle, client_rpc_cb, client_lookup_args, &in);
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
    sprintf(na_info_string, "bmi+tcp://%d", port);
    /* sprintf(na_info_string, "cci+tcp://%d", port); */
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: using %.7s\n\n", na_info_string);
        fflush(stdout);
    }
    
    if (!na_info_string) {
        fprintf(stderr, HG_PORT_NAME " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"cci+tcp://22222\"\n");
        exit(0);
    }

    /* Initialize Mercury with the desired network abstraction class */
    /* printf("Using %s\n", na_info_string); */
    *hg_class = HG_Init(na_info_string, NA_FALSE);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        goto done;
    }

    /* Create HG context */
    *hg_context = HG_Context_create(*hg_class);

    // Register RPC
    gen_obj_register_id_g = gen_obj_id_register(*hg_class);

    ret_value = 0;

done:
    FUNC_LEAVE(ret_value);
}

// Check if all work has been processed
// Using global variable $mercury_work_todo_g
perr_t PDC_Client_check_response(hg_context_t **hg_context)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    hg_return_t hg_ret;
    do {
        unsigned int actual_count = 0;
        do {
            hg_ret = HG_Trigger(*hg_context, 0/* timeout */, 1 /* max count */, &actual_count);
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

perr_t PDC_Client_init()
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    pdc_server_info_g = NULL;

    // get server address and fill in $pdc_server_info_g
    PDC_Client_read_server_addr_from_file();

#ifdef ENABLE_MPI
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_client_mpi_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_client_mpi_size_g);
#endif
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: Found %d PDC Metadata servers, running with %d PDC clients\n", pdc_server_num_g ,pdc_client_mpi_size_g);
    }
    /* printf("pdc_rank:%d\n", pdc_client_mpi_rank_g); */
    fflush(stdout);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_finalize()
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    /* Finalize Mercury*/
    int i;
    for (i = 0; i < pdc_server_num_g; i++) {
        if (pdc_server_info_g[i].addr_valid) {
            HG_Addr_free(send_class_g, pdc_server_info_g[i].addr);
        }
    }
    HG_Context_destroy(send_context_g);
    HG_Finalize(send_class_g);

    if (pdc_server_info_g != NULL)
        free(pdc_server_info_g);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
uint64_t PDC_Client_send_name_recv_id(const char *obj_name)
{

    FUNC_ENTER(NULL);

    uint64_t ret_value;
    hg_return_t  hg_ret = 0;
    int server_id = pdc_hash_djb2(obj_name) % pdc_server_num_g; 
    int port      = pdc_client_mpi_rank_g + 8000; 

    // Test
    /* server_id = pdc_client_mpi_rank_g / 20; */
    /* server_id = server_id % 2; */

    /* if (pdc_client_mpi_rank_g < 20) */ 
    /*     server_id = 0; */
    /* else if (pdc_client_mpi_rank_g < 40) */ 
    /*     server_id = 1; */
    /* else if (pdc_client_mpi_rank_g < 60) */ 
    /*     server_id = 2; */
    /* else if (pdc_client_mpi_rank_g < 80) */ 
    /*     server_id = 3; */
    
    /* printf("[%d]: target server %d\n", pdc_client_mpi_rank_g, server_id); */
    /* fflush(stdout); */

    if (mercury_has_init_g == 0) {
        // Init Mercury network connection
        PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
        if (send_class_g == NULL || send_context_g == NULL) {
            printf("Error with Mercury Init, exiting...\n");
            exit(0);
        }
        mercury_has_init_g = 1;
    }

    // Fill lookup args
    struct client_lookup_args lookup_args;
    lookup_args.obj_name   = obj_name;
    lookup_args.obj_id     = -1;
    lookup_args.server_id  = server_id;

    // Initiate server lookup and send obj name in client_lookup_cb()
    char *target_addr_string = pdc_server_info_g[server_id].addr_string;
    // Test
    if (pdc_server_info_g[server_id].rpc_handle_valid != 1) {
 
        hg_ret = HG_Addr_lookup(send_context_g, client_lookup_cb, &lookup_args, target_addr_string, HG_OP_ID_IGNORE);
    }
    else {
        // Fill input structure
        gen_obj_id_in_t in;
        in.obj_name   = obj_name;
        in.hash_value = pdc_hash_djb2(obj_name);
        /* printf("Hash(%s) = %d\n", obj_name, in.hash_value); */

        /* printf("Sending input to target\n"); */
        ret_value = HG_Forward(pdc_server_info_g[server_id].rpc_handle, client_rpc_cb, &lookup_args, &in);
        if (ret_value != HG_SUCCESS) {
            fprintf(stderr, "client_lookup_cb(): Could not start HG_Forward()\n");
            return EXIT_FAILURE;
        }
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == -1) {
        printf("Error obtaining obj id from PDC server!\n");
    }

    /* printf("Received obj_id=%llu\n", lookup_args.obj_id); */
    /* fflush(stdout); */

       ret_value = lookup_args.obj_id;

done:
    FUNC_LEAVE(ret_value);
}

