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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_time.h"

#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

#define MAX_SERVER_NUM 1024

static hg_id_t gen_obj_id_client_id_g;
static int     work_todo_g = 0;

int
PDC_Client_read_server_addr_from_file(char target_addr_string[MAX_SERVER_NUM][PATH_MAX])
{
    int   i = 0;
    char *p;
    FILE *na_config = NULL;

    na_config = fopen(pdc_server_cfg_name, "r");
    if (!na_config) {
        fprintf(stderr, "Could not open config file from default location: %s\n", pdc_server_cfg_name);
        exit(0);
    }
    char tmp[PATH_MAX];
    fgets(tmp, PATH_MAX, na_config);
    while (fgets(target_addr_string[i], PATH_MAX, na_config)) {
        p = strrchr(target_addr_string[i], '\n');
        if (p != NULL)
            *p = '\0';
        /* printf("%s", target_addr_string[i]); */
        i++;
    }
    fclose(na_config);
    return i;
}

/* This routine gets executed after a call to HG_Trigger and
 * the RPC has completed */
static hg_return_t
client_rpc_cb(const struct hg_cb_info *callback_info)
{
    /* printf("Entering client_rpc_cb()"); */
    struct client_lookup_args *client_lookup_args = (struct client_lookup_args *)callback_info->arg;
    hg_handle_t                handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    gen_obj_id_out_t output;
    HG_Get_output(handle, &output);
    /* printf("Return value=%llu\n", output.ret); */
    client_lookup_args->obj_id = output.ret;

    work_todo_g--;
    return HG_SUCCESS;
}

// callback function for HG_Addr_lookup()
// start RPC connection
static hg_return_t
client_lookup_cb(const struct hg_cb_info *callback_info)
{
    /* printf("Entering client_lookup_cb()"); */
    hg_return_t hg_ret;

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args *)callback_info->arg;
    client_lookup_args->hg_target_addr            = callback_info->info.lookup.addr;

    /* Create HG handle bound to target */
    hg_handle_t handle;
    HG_Create(client_lookup_args->hg_context, client_lookup_args->hg_target_addr, gen_obj_id_client_id_g,
              &handle);

    /* Fill input structure */
    gen_obj_id_in_t in;
    in.obj_name = client_lookup_args->obj_name;

    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(handle, client_rpc_cb, client_lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "client_lookup_cb(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }
    return HG_SUCCESS;
}

int
PDC_Client_mercury_init(hg_class_t **hg_class, hg_context_t **hg_context, int port)
{
    char na_info_string[PATH_MAX];
    sprintf(na_info_string, "cci+tcp://%d", port);
    if (!na_info_string) {
        fprintf(stderr, HG_PORT_NAME
                " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"cci+tcp://22222\"\n");
        exit(0);
    }
    /* printf("NA: %s\n", na_info_string); */

    /* Initialize Mercury with the desired network abstraction class */
    printf("Using %s\n", na_info_string);
    *hg_class = HG_Init(na_info_string, NA_FALSE);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        return -1;
    }

    /* Create HG context */
    *hg_context = HG_Context_create(*hg_class);

    // Register RPC
    gen_obj_id_client_id_g = gen_obj_id_register(*hg_class);

    return 0;
}

void
PDC_Client_check_response(hg_context_t **hg_context)
{
    hg_return_t hg_ret;
    do {
        unsigned int actual_count = 0;
        do {
            hg_ret = HG_Trigger(*hg_context, 0 /* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* printf("actual_count=%d\n",actual_count); */
        /* Do not try to make progress anymore if we're done */
        if (work_todo_g <= 0)
            break;

        hg_ret = HG_Progress(*hg_context, HG_MAX_IDLE_TIME);
    } while (hg_ret == HG_SUCCESS);

    return;
}

hg_return_t
send_name_recv_id(struct client_lookup_args *client_lookup_args, char *target_addr_string)
{
    hg_return_t hg_ret = 0;
    // This function takes a user callback. HG_Progress() and HG_Trigger() need to be called in this case
    // and the resulting address can be retrieved when the user callback is executed.
    // Connection to the target may occur at this time, though that behavior is left upon the NA plugin
    // implementation.
    hg_ret = HG_Addr_lookup(client_lookup_args->hg_context, client_lookup_cb, client_lookup_args,
                            target_addr_string, HG_OP_ID_IGNORE);
    /* printf("HG_Addr_lookup() ret=%d\n", hg_ret); */

    return hg_ret;
}

int
main(int argc, char **argv)
{
    int rank, size;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#else
    rank = 0;
    size = 1;
#endif

    hg_class_t *  hg_class   = NULL;
    hg_context_t *hg_context = NULL;
    hg_return_t   hg_ret     = 0;

    // Get server address string
    char target_addr_string[MAX_SERVER_NUM][PATH_MAX];
    int  i, n_server;
    int  port = rank + 8000;
    n_server  = PDC_Client_read_server_addr_from_file(target_addr_string);

    hg_time_t start_time, end_time, elapsed_time;
    double    elapsed_time_double;

    // Init Mercury network connection
    PDC_Client_mercury_init(&hg_class, &hg_context, port);
    if (hg_class == NULL || hg_context == NULL) {
        printf("Error with Mercury Init, exiting...\n");
        exit(0);
    }

    // Obj name and ID
    char **test_obj_names = (char **)malloc(sizeof(char *) * n_server);
    for (i = 0; i < n_server; i++)
        test_obj_names[i] = (char *)malloc(sizeof(char) * 128);

    work_todo_g = 0;
    struct client_lookup_args *client_lookup_args =
        (struct client_lookup_args *)malloc(sizeof(struct client_lookup_args) * n_server);

    hg_time_get_current(&start_time);
    for (i = 0; i < n_server; i++) {
        sprintf(test_obj_names[i], "Test%d", i);
        client_lookup_args[i].hg_class   = hg_class;
        client_lookup_args[i].hg_context = hg_context;
        client_lookup_args[i].obj_name   = test_obj_names[i];
        client_lookup_args[i].obj_id     = -1;

        work_todo_g++;
        printf("Target addr:%s\n", target_addr_string[i]);
        send_name_recv_id(&client_lookup_args[i], target_addr_string[i]);
    }

    PDC_Client_check_response(&hg_context);

    hg_time_get_current(&end_time);
    elapsed_time        = hg_time_subtract(end_time, start_time);
    elapsed_time_double = hg_time_to_double(elapsed_time);
    printf("Total elapsed time for PDC server connection: %.6fs\n", elapsed_time_double);

    for (i = 0; i < n_server; i++) {
        printf("\"%s\" obj_id = %d\n", client_lookup_args[i].obj_name, client_lookup_args[i].obj_id);
        HG_Addr_free(hg_class, client_lookup_args[i].hg_target_addr);
        free(test_obj_names[i]);
    }
    free(client_lookup_args);
    free(test_obj_names);

    /* Finalize Mercury*/
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
