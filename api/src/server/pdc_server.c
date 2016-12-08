#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>

/* #define ENABLE_MPI 1 */

#define BLOOM_NEW    new_counting_bloom
#define BLOOM_TYPE_T counting_bloom_t
#define BLOOM_CHECK  counting_bloom_check
#define BLOOM_ADD    counting_bloom_add

#ifdef ENABLE_MPI
    #include "mpi.h"
#endif
#include "utlist.h"
#include "dablooms.h"

#include "mercury.h"
#include "mercury_macros.h"

// Mercury multithread
#include "mercury_thread.h"
#include "mercury_thread_pool.h"
#include "mercury_thread_mutex.h"

// Mercury hash table and list
#include "mercury_hash_table.h"
#include "mercury_list.h"

#include "pdc_interface.h"
/* #include "pdc_client_connect.h" */
#include "pdc_client_server_common.h"
#include "pdc_server.h"

hg_thread_mutex_t pdc_metadata_hash_table_mutex_g;

// Global thread pool
hg_thread_pool_t *hg_test_thread_pool_g = NULL;

// Global hash table for storing metadata 
hg_hash_table_t *metadata_hash_table_g = NULL;

int pdc_server_rank_g = 0;
int pdc_server_size_g = 1;

static int32_t
PDC_Server_metadata_int_equal(hg_hash_table_key_t vlocation1, hg_hash_table_key_t vlocation2)
{
    return *((int32_t *) vlocation1) == *((int32_t *) vlocation2);
}

static unsigned int
PDC_Server_metadata_int_hash(hg_hash_table_key_t vlocation)
{
    return *((unsigned int*) vlocation);
}

static void
PDC_Server_metadata_PDC_Server_metadata_int_hash_key_free(hg_hash_table_key_t key)
{
    free((int32_t *) key);
}

static void
PDC_Server_metadata_hash_value_free(hg_hash_table_value_t value)
{
    pdc_metadata_t *elt, *tmp, *head;

    head= (pdc_metadata_t *) value;
    DL_FOREACH_SAFE(head,elt,tmp) {
      DL_DELETE(head,elt);
      free(elt);
    }

    /* if (tmp->app_name != NULL) */ 
    /*     free(tmp->app_name); */

    /* if (tmp->obj_name != NULL) */ 
    /*     free(tmp->obj_name); */

    /* if (tmp->obj_data_location != NULL) */ 
    /*     free(tmp->obj_data_location); */

}

inline void PDC_Server_metadata_init(pdc_metadata_t* a)
{
    a->user_id              = -1;
    a->time_step            = -1;
    /* a->app_name             = NULL; */
    /* a->obj_name             = NULL; */
    a->app_name[0]         = 0;
    a->obj_name[0]         = 0;

    a->obj_id               = -1;
    /* a->obj_data_location    = NULL; */
    a->obj_data_location[0] = 0;
    a->create_time          = 0;
    a->last_modified_time   = 0;

    a->prev                 = NULL;
    a->next                 = NULL;
}
// ^ hash table

void PDC_Server_print_version()
{
    unsigned major, minor, patch;
    HG_Version_get(&major, &minor, &patch);
    printf("Server running mercury version %u.%u-%u\n", major, minor, patch);
    return;
}

static uint64_t PDC_Server_gen_obj_id()
{
    FUNC_ENTER(NULL);
    uint64_t ret_value;
    ret_value = pdc_id_seq_g++;
done:
    FUNC_LEAVE(ret_value);
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
    HG_Addr_free(hg_class, self_addr);

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

perr_t PDC_Server_init(int port, hg_class_t **hg_class, hg_context_t **hg_context)
{
    FUNC_ENTER(NULL);
    perr_t ret_value;

    /* PDC_Server_print_version(); */

    // set server id start
    pdc_id_seq_g = pdc_id_seq_g * (pdc_server_rank_g+1);

    int i;
    char *all_addr_strings_1d;
    char **all_addr_strings;
    if (pdc_server_rank_g == 0) {
        all_addr_strings_1d = (char* )malloc(sizeof(char ) * pdc_server_size_g * PATH_MAX);
        all_addr_strings    = (char**)malloc(sizeof(char*) * pdc_server_size_g );
    }

    char self_addr_string[PATH_MAX];
    char na_info_string[PATH_MAX];
    char hostname[1024];
    memset(hostname, 0, 1024);
    gethostname(hostname, 1023);
    sprintf(na_info_string, "bmi+tcp://%s:%d", hostname, port);
    /* sprintf(na_info_string, "cci+tcp://%s:%d", hostname, port); */
    if (pdc_server_rank_g == 0) 
        printf("\n==PDC_SERVER: using %.7s\n", na_info_string);
    
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
    /* fflush(stdout); */

    // Gather addresses
#ifdef ENABLE_MPI
    MPI_Gather(self_addr_string, PATH_MAX, MPI_CHAR, all_addr_strings_1d, PATH_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
    if (pdc_server_rank_g == 0) {
        for (i = 0; i < pdc_server_size_g; i++) {
            all_addr_strings[i] = &all_addr_strings_1d[i*PATH_MAX];
            /* printf("%s\n", all_addr_strings[i]); */
        }
    }
#else 
    all_addr_strings[0] = self_addr_string;
#endif
    // Rank 0 write all addresses to one file
    if (pdc_server_rank_g == 0) {
        printf("========================\n");
        printf("Server address%s:\n", pdc_server_size_g ==1?"":"es");
        for (i = 0; i < pdc_server_size_g; i++) 
            printf("%s\n", all_addr_strings[i]);
        printf("========================\n");
        PDC_Server_write_addr_to_file(all_addr_strings, pdc_server_size_g);

        // Free
        free(all_addr_strings_1d);
        free(all_addr_strings);
    }
    fflush(stdout);

#ifdef ENABLE_MULTITHREAD
    // Init threadpool
    char *nthread_env = getenv("PDC_SERVER_NTHREAD"); 
    int n_thread; 
    if (nthread_env != NULL) 
        n_thread = atoi(nthread_env);
    
    if (n_thread <= 1) 
        n_thread = 2;
    hg_thread_pool_init(n_thread, &hg_test_thread_pool_g);
    if (pdc_server_rank_g == 0) {
        printf("\n==PDC_SERVER: Starting server with %d threads...\n", n_thread);
        fflush(stdout);
    }
#else
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER:Not using multi-thread!\n");
        fflush(stdout);
    }
#endif

    // Hashtable
    // TODO: read previous data from storage
    metadata_hash_table_g = hg_hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (metadata_hash_table_g == NULL) {
        printf("metadata_hash_table_g init error! Exit...\n");
        exit(0);
    }
    /* else */
    /*     printf("Hash table created!\n"); */
    hg_hash_table_register_free_functions(metadata_hash_table_g, PDC_Server_metadata_PDC_Server_metadata_int_hash_key_free, PDC_Server_metadata_hash_value_free);


    // Initalize atomic variable to finalize server 
    hg_atomic_set32(&close_server_g, 0);


    // Create server tmp dir
    int status;
    pdc_server_tmp_dir = "./pdc_tmp";
    status = mkdir(pdc_server_tmp_dir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    // TODO: remove server tmp dir?


    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_init

perr_t PDC_Server_finalize()
{
    FUNC_ENTER(NULL);
    perr_t ret_value = SUCCEED;

    // Free hash table
    if(metadata_hash_table_g != NULL)
        hg_hash_table_free(metadata_hash_table_g);

done:
    FUNC_LEAVE(ret_value);
}

static HG_THREAD_RETURN_TYPE
hg_progress_thread(void *arg)
{
    /* pthread_t tid = pthread_self(); */
    pid_t tid;
    /* tid = syscall(SYS_gettid); */

    hg_context_t *context = (hg_context_t*)arg;

    HG_THREAD_RETURN_TYPE tret = (HG_THREAD_RETURN_TYPE) 0;
    hg_return_t ret = HG_SUCCESS;

    do {
        if (hg_atomic_get32(&close_server_g)) break;

        ret = HG_Progress(context, 100);
        /* printf("thread [%d]\n", tid); */
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    hg_thread_exit(tret);

    return tret;
}

// Multithread Mercury
perr_t PDC_Server_multithread_loop(hg_class_t *class, hg_context_t *context)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    hg_thread_t progress_thread;
    hg_thread_create(&progress_thread, hg_progress_thread, context);

    hg_return_t ret = HG_SUCCESS;
    do {
        if (hg_atomic_get32(&close_server_g)) break;

        ret = HG_Trigger(context, 0, 1, NULL);
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    hg_thread_join(progress_thread);

    // Destory pool
    hg_thread_pool_destroy(hg_test_thread_pool_g);
    /* hg_thread_mutex_destroy(&hg_test_local_bulk_handle_mutex_g); */

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

// No threading
perr_t PDC_Server_loop(hg_class_t *hg_class, hg_context_t *hg_context)
{
    FUNC_ENTER(NULL);
    perr_t ret_value;

    hg_return_t hg_ret;

    /* Poke progress engine and check for events */
    do {
        unsigned int actual_count = 0;
        do {
            hg_ret = HG_Trigger(hg_context, 0/* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (hg_atomic_get32(&close_server_g)) break;
        hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME);

        /* fflush(stdout); */
    } while (hg_ret == HG_SUCCESS);

    if (hg_ret == HG_SUCCESS) 
        ret_value = SUCCEED;
    else
        ret_value = FAIL;
    
done:
    FUNC_LEAVE(ret_value);
}

static inline void combine_obj_info_to_string(pdc_metadata_t *metadata, char *output)
{
    perr_t ret_value = SUCCEED;
    sprintf(output, "%d%s%s%d", metadata->user_id, metadata->app_name, metadata->obj_name, metadata->time_step);
}

static int find_identical_metadata(pdc_metadata_t *mlist, pdc_metadata_t *a)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    // Use bloom filter to quick check if current metadata is in the list

    BLOOM_TYPE_T *bloom = (BLOOM_TYPE_T*)mlist->bloom;
    char combined_string[PATH_MAX];
    combine_obj_info_to_string(a, combined_string);
    /* printf("Combined string: %s\n", combined_string); */

    int bloom_check;
    bloom_check = BLOOM_CHECK(bloom, combined_string, strlen(combined_string));
    if (bloom_check == 0) {
        /* printf("Bloom filter: definitely not!\n"); */
        ret_value = 0;
        goto done;
    }
    else {
        // bloom filter says maybe, so need to check entire list
        pdc_metadata_t *elt;
        DL_FOREACH(mlist, elt) {
            if (PDC_Server_metadata_cmp(elt, a) == 0) {
                printf("Identical metadata already exist in current Metadata store!\n");
                PDC_Server_print_metadata(a);
                ret_value = 1;
                goto done;
            }
        }
    }

    /* int count; */
    /* DL_COUNT(lookup_value, elt, count); */
    /* printf("%d item(s) in list\n", count); */

    ret_value = 0;

done:
    FUNC_LEAVE(ret_value);
} 

static perr_t PDC_Server_hash_table_list_init(pdc_metadata_t *metadata, int32_t *hash_key)
{
    FUNC_ENTER(NULL);

    perr_t      ret_value = 0;
    hg_return_t ret;

    /* printf("hash key=%d\n", *hash_key); */

    // Init head of linked list
    metadata->prev = metadata;                                                                   \
    metadata->next = NULL;   

    /* PDC_Server_print_metadata(metadata); */

    // Insert to hash table
    ret = hg_hash_table_insert(metadata_hash_table_g, hash_key, metadata);
    if (ret != 1) {
        fprintf(stderr, "PDC_Server_hash_table_list_init(): Error with hash table insert!\n");
        ret_value = -1;
        goto done;
    }

    int capacity = 500000;
    double error_rate = 0.05;

    char bloom_file[PATH_MAX];
    sprintf(bloom_file, "%s/bloom.%d", pdc_server_tmp_dir, pdc_server_rank_g); 

    metadata->bloom = (BLOOM_TYPE_T*)BLOOM_NEW(capacity, error_rate, bloom_file);
    if (!metadata->bloom) {
        fprintf(stderr, "ERROR: Could not create bloom filter\n");
        ret_value = -1;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

static perr_t bloom_add(pdc_metadata_t *metadata)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    char combined_string[PATH_MAX];

    combine_obj_info_to_string(metadata, combined_string);
    /* printf("Combined string: %s\n", combined_string); */

    BLOOM_TYPE_T *bloom = (BLOOM_TYPE_T*)metadata->bloom;
    ret_value = BLOOM_ADD(bloom, combined_string, strlen(combined_string));

done:
    FUNC_LEAVE(ret_value);
}

perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{

    FUNC_ENTER(NULL);

    perr_t ret_value;

    /* printf("Got RPC request with name: %s\tHash=%d\n", in->obj_name, in->hash_value); */
    /* printf("Full name check: %s\n", &in->obj_name[507]); */

    pdc_metadata_t *metadata = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    if (metadata == NULL) {
        printf("Cannnot allocate pdc_metadata_t!\n");
        goto done;
    }
    strcpy(metadata->obj_name, in->obj_name);
    strcpy(metadata->app_name, in->app_name);

    // TODO: Both server and client gets it and do security check
    metadata->user_id        = in->user_id;
    metadata->time_step      = in->time_step;

    /* obj_id; */
    /* obj_data_location        = NULL; */
    /* create_time              =; */
    /* last_modified_time       =; */
    strcpy(metadata->tags, in->tags);


    int32_t *hash_key = (int32_t*)malloc(sizeof(int32_t));
    if (hash_key == NULL) {
        printf("Cannnot allocate hash_key!\n");
        goto done;
    }
    *hash_key = in->hash_value;

    pdc_metadata_t *lookup_value;
    pdc_metadata_t *elt;

    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", *hash_key); */
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            // Set the bloom filter pointer to all metadata list items so we don't need 
            // to take care of it when head got deleted
            metadata->bloom = lookup_value->bloom; 
            
            /* printf("lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            if (find_identical_metadata(lookup_value, metadata) == 1) {
                ret_value = -1;
                out->ret  = -1;
                free(metadata);
                goto done;
            }
            else {
                // add to bloom filter
                bloom_add(metadata);
                // Currently $metadata is unique, insert to linked list
                DL_APPEND(lookup_value, metadata);
            }
        
        }
        else {
            // First entry for current hasy_key, init linked list, and insert to hash table
            /* printf("lookup_value is NULL!\n"); */
            PDC_Server_hash_table_list_init(metadata, hash_key);
            /* pdc_metadata_t *head = NULL; */
            /* DL_APPEND(head, metadata); */
            /* ret_value = hg_hash_table_insert(metadata_hash_table_g, hash_key, metadata); */
        }

    }
    else {
        printf("metadata_hash_table_g not initilized!\n");
        ret_value = -1;
        goto done;
    }

    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;

    // Generate object id (uint64_t)
    metadata->obj_id = PDC_Server_gen_obj_id();

    // Fill $out structure for returning the generated obj_id to client
    out->ret = metadata->obj_id;

    // Debug print metadata info
    /* PDC_Server_print_metadata(metadata); */

done:
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    FUNC_LEAVE(ret_value);
}

int main(int argc, char *argv[])
{
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_server_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_server_size_g);
#else
    pdc_server_rank_g = 0;
    pdc_server_size_g = 1;
#endif

    hg_class_t *hg_class = NULL;
    hg_context_t *hg_context = NULL;

    int port;
    port = pdc_server_rank_g + 6600;
    /* printf("rank=%d, port=%d\n", pdc_server_rank_g,port); */
    PDC_Server_init(port, &hg_class, &hg_context);
    if (hg_class == NULL || hg_context == NULL) {
        printf("Error with Mercury init\n");
        goto done;
    }

    // Register RPC
    gen_obj_id_register(hg_class);

#ifdef ENABLE_MULTITHREAD
    PDC_Server_multithread_loop(hg_class, hg_context);
#else
    PDC_Server_loop(hg_class, hg_context);
#endif

    // Finalize 
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: exiting...\n");
        /* printf("==PDC_SERVER: [%d] exiting...\n", pdc_server_rank_g); */
    }
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
