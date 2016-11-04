#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

#include "mercury.h"
#include "mercury_macros.h"

#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

// Multithread
#include "mercury_thread.h"
#include "mercury_thread_pool.h"
#include "mercury_thread_mutex.h"

hg_thread_pool_t *hg_test_thread_pool_g;

void PDC_Server_print_version()
{
    unsigned major, minor, patch;
    HG_Version_get(&major, &minor, &patch);
    printf("Server running mercury version %u.%u-%u\n", major, minor, patch);
    return;
}

perr_t PDC_Server_get_self_addr(hg_class_t* hg_class, char* self_addr_string)
{
    FUNC_ENTER(NULL);
    perr_t ret_value;

    hg_addr_t self_addr;
    hg_size_t self_addr_string_size = PATH_MAX;
 
    // Get self addr to tell client about 
    HG_Addr_self(hg_class, &self_addr);
    HG_Addr_to_string(hg_class, self_addr_string, &self_addr_string_size, self_addr);
    /* HG_Addr_free(hg_class, self_addr); */

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_write_addr_to_file(char** addr_strings, int n)
{
    FUNC_ENTER(NULL);
    perr_t ret_value;

    // write to file
    FILE *na_config = fopen(pdc_server_cfg_name, "w+");
    if (!na_config) {
        fprintf(stderr, "Could not open config file from: %s\n", pdc_server_cfg_name);
        exit(0);
    }
    int i;
    fprintf(na_config, "%d\n", n);
    for (i = 0; i < n; i++) {
        fprintf(na_config, "%s\n", addr_strings[i]);
    }
    fclose(na_config);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_init(int rank, int size, int port, hg_class_t **hg_class, hg_context_t **hg_context)
{
    FUNC_ENTER(NULL);
    perr_t ret_value;

    /* PDC_Server_print_version(); */

    // set server id start
    pdc_id_seq_g = pdc_id_seq_g * (rank+1);

    int i;
    char *all_addr_strings_1d;
    char **all_addr_strings;
    if (rank == 0) {
        all_addr_strings_1d = (char*)malloc(sizeof(char) * size * PATH_MAX);
        all_addr_strings = (char**)malloc(sizeof(char*) * size);
    }

    char self_addr_string[PATH_MAX];
    char na_info_string[PATH_MAX];
    sprintf(na_info_string, "cci+tcp://%d", port);
    /* printf("na_info_string: %s\n", na_info_string); */
    if (!na_info_string) {
        fprintf(stderr, HG_PORT_NAME " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"tcp://127.0.0.1:22222\"\n");
        exit(0);
    }

    // Init server
    *hg_class = HG_Init(na_info_string, NA_TRUE);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        return -1;
    }

    // Create HG context 
    *hg_context = HG_Context_create(*hg_class);
    if (*hg_context == NULL) {
        printf("Error with HG_Context_create()\n");
        return -1;
    }

    // Get server address
    PDC_Server_get_self_addr(*hg_class, self_addr_string);
    /* printf("Server address is: %s\n", self_addr_string); */

    // Gather addresses
#ifdef ENABLE_MPI
    MPI_Gather(self_addr_string, PATH_MAX, MPI_CHAR, all_addr_strings_1d, PATH_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        for (i = 0; i < size; i++) {
            all_addr_strings[i] = &all_addr_strings_1d[i*PATH_MAX];
            /* printf("%s\n", all_addr_strings[i]); */
        }
    }
#else 
    all_addr_strings[0] = self_addr_string;
#endif
    // Rank 0 write all addresses to one file
    if (rank == 0) {
        printf("========================\n");
        printf("Server address%s:\n", size==1?"":"es");
        for (i = 0; i < size; i++) 
            printf("%s\n", all_addr_strings[i]);
        printf("========================\n");
        PDC_Server_write_addr_to_file(all_addr_strings, size);

        // Free
        free(all_addr_strings_1d);
        free(all_addr_strings);
    }
    fflush(stdout);
    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

static HG_THREAD_RETURN_TYPE
hg_progress_thread(void *arg)
{
    /* pthread_t tid = pthread_self(); */
    pid_t tid;
    tid = syscall(SYS_gettid);

    hg_context_t *context = (hg_context_t*)arg;

    HG_THREAD_RETURN_TYPE tret = (HG_THREAD_RETURN_TYPE) 0;
    hg_return_t ret = HG_SUCCESS;

    do {
        /* if (hg_atomic_cas32(&hg_test_finalizing_count_g, 1, 1)) */
        /*     break; */

        ret = HG_Progress(context, 100);
        /* printf("thread [%d]\n", tid); */
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    printf("Exiting\n");
    hg_thread_exit(tret);

    return tret;
}


// TODO: multithread
perr_t PDC_Server_multithread_loop(hg_class_t *class, hg_context_t *context)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    const int n_thread = 2;
    /* hg_thread_mutex_init(&hg_test_local_bulk_handle_mutex_g); */
    hg_thread_pool_init(n_thread, &hg_test_thread_pool_g);
    printf("# Starting server with %d threads...\n", n_thread);

    hg_thread_t progress_thread;
    hg_thread_create(&progress_thread, hg_progress_thread, context);
    /* hg_thread_create(&progress_thread, hg_progress_thread, context); */

    hg_return_t ret = HG_SUCCESS;
    do {
        /* if (hg_atomic_cas32(&hg_test_finalizing_count_g, 1, 1)) */
        /*     break; */

        ret = HG_Trigger(context, HG_MAX_IDLE_TIME, 1, NULL);
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    hg_thread_join(progress_thread);

    // Destory pool
    hg_thread_pool_destroy(hg_test_thread_pool_g);
    /* hg_thread_mutex_destroy(&hg_test_local_bulk_handle_mutex_g); */

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}
perr_t PDC_Server_loop(hg_class_t *hg_class, hg_context_t *hg_context)
{
    FUNC_ENTER(NULL);
    perr_t ret_value;

    hg_return_t hg_ret;

    /* Poke progress engine and check for events */
    do {
        unsigned int actual_count = 0;
        do {
            printf("Before Trigger()\n");
            hg_ret = HG_Trigger(hg_context, 0 /* timeout */, 1 /* max count */, &actual_count);
            printf("Trigger()\n");
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        /* if (all_work_done) break; */
        printf("Before Progress()\n");
        hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME);
        printf("Progress()\n");

        fflush(stdout);
    } while (hg_ret == HG_SUCCESS);

    if (hg_ret == HG_SUCCESS) 
        ret_value = SUCCEED;
    else
        ret_value = FAIL;
    
done:
    FUNC_LEAVE(ret_value);
}

int main(int argc, char *argv[])
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

    hg_class_t *hg_class = NULL;
    hg_context_t *hg_context = NULL;

    int port;
    port = rank + 6600;
    /* printf("rank=%d, port=%d\n", rank ,port); */
    PDC_Server_init(rank, size, port, &hg_class, &hg_context);
    if (hg_class == NULL || hg_context == NULL) {
        printf("Error with Mercury init\n");
        goto done;
    }

    // Register RPC
    gen_obj_id_register(hg_class);

    /* PDC_Server_loop(hg_class, hg_context); */
    PDC_Server_multithread_loop(hg_class, hg_context);

    // Finalize 
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
