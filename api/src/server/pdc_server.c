#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
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
hg_thread_mutex_t pdc_metadata_name_mark_hash_table_mutex_g;

// Global thread pool
hg_thread_pool_t *hg_test_thread_pool_g = NULL;

// Global hash table for storing metadata 
hg_hash_table_t *metadata_hash_table_g = NULL;
hg_hash_table_t *metadata_name_mark_hash_table_g = NULL;

int is_hash_table_init_g = 0;
int is_restart_g = 0;

int pdc_server_rank_g = 0;
int pdc_server_size_g = 1;

// Debug var
int n_bloom_total_g;
int n_bloom_maybe_g;
double server_bloom_check_time_g = 0.0;
double server_bloom_insert_time_g = 0.0;
double server_insert_time_g      = 0.0;
hg_thread_mutex_t pdc_time_mutex_g;
hg_thread_mutex_t pdc_bloom_time_mutex_g;

static int 
PDC_Server_metadata_int_equal(hg_hash_table_key_t vlocation1, hg_hash_table_key_t vlocation2)
{
    return *((uint32_t *) vlocation1) == *((uint32_t *) vlocation2);
}

static unsigned int
PDC_Server_metadata_int_hash(hg_hash_table_key_t vlocation)
{
    return *((unsigned int*) vlocation);
}

static void
PDC_Server_metadata_int_hash_key_free(hg_hash_table_key_t key)
{
    free((uint32_t *) key);
}

static void
PDC_Server_metadata_name_mark_hash_value_free(hg_hash_table_value_t value)
{
    pdc_metadata_name_mark_t *elt, *tmp, *head;

    head = (pdc_metadata_name_mark_t *) value;

    // Free metadata list
    DL_FOREACH_SAFE(head,elt,tmp) {
      /* DL_DELETE(head,elt); */
      free(elt);
    }
}


static void
PDC_Server_metadata_hash_value_free(hg_hash_table_value_t value)
{
    pdc_metadata_t *elt, *tmp, *head;

    head = (pdc_metadata_t *) value;

    // Free bloom filter
    free_counting_bloom(head->bloom);

    // Free metadata list
    if (is_restart_g == 0) {
        DL_FOREACH_SAFE(head,elt,tmp) {
          /* DL_DELETE(head,elt); */
          free(elt);
        }
    }
    else {
        free(head);
    }
}

inline void PDC_Server_metadata_name_mark_init(pdc_metadata_t* a)
{
    a->prev                 = NULL;
    a->next                 = NULL;
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

static inline void combine_obj_info_to_string(pdc_metadata_t *metadata, char *output)
{
    sprintf(output, "%d%s%s%d", metadata->user_id, metadata->app_name, metadata->obj_name, metadata->time_step);
}

static int find_identical_namemark(pdc_metadata_name_mark_t *mlist, pdc_metadata_name_mark_t *a)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    pdc_metadata_name_mark_t *elt;
    DL_FOREACH(mlist, elt) {
        if (strcmp(elt->obj_name, a->obj_name) == 0) {
            /* printf("Identical namemark with name [%s] already exist in current Metadata store!\n", ); */
            ret_value = 1;
            goto done;
        }
    }

    ret_value = 0;

done:
    FUNC_LEAVE(ret_value);
} 


static int find_identical_metadata(pdc_metadata_t *mlist, pdc_metadata_t *a)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    // Use bloom filter to quick check if current metadata is in the list

    BLOOM_TYPE_T *bloom = (BLOOM_TYPE_T*)mlist->bloom;
    char combined_string[PATH_MAX];
    combine_obj_info_to_string(a, combined_string);
    /* printf("bloom_check: Combined string: %s\n", combined_string); */

    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    gettimeofday(&ht_total_start, 0);

    int bloom_check;
    bloom_check = BLOOM_CHECK(bloom, combined_string, strlen(combined_string));

    // Timing
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    hg_thread_mutex_lock(&pdc_bloom_time_mutex_g);
    server_bloom_check_time_g += ht_total_sec;
    hg_thread_mutex_unlock(&pdc_bloom_time_mutex_g);

    n_bloom_total_g++;
    if (bloom_check == 0) {
        /* printf("Bloom filter: definitely not!\n"); */
        ret_value = 0;
        goto done;
    }
    else {
        // bloom filter says maybe, so need to check entire list
        /* printf("Bloom filter: maybe!\n"); */
        n_bloom_maybe_g++;
        pdc_metadata_t *elt;
        DL_FOREACH(mlist, elt) {
            if (PDC_metadata_cmp(elt, a) == 0) {
                printf("Identical metadata already exist in current Metadata store!\n");
                PDC_print_metadata(a);
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
    char config_fname[PATH_MAX];
    sprintf(config_fname, "%s/%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);
    FILE *na_config = fopen(config_fname, "w+");
    if (!na_config) {
        fprintf(stderr, "Could not open config file from: %s\n", config_fname);
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

static perr_t PDC_Server_init_hash_table()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    metadata_hash_table_g = hg_hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (metadata_hash_table_g == NULL) {
        printf("==PDC_SERVER: metadata_hash_table_g init error! Exit...\n");
        exit(-1);
    }
    hg_hash_table_register_free_functions(metadata_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_metadata_hash_value_free);

    // Name marker hash table, reuse some functions from metadata_hash_table
    metadata_name_mark_hash_table_g = hg_hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (metadata_name_mark_hash_table_g == NULL) {
        printf("==PDC_SERVER: metadata_name_mark_hash_table_g init error! Exit...\n");
        exit(-1);
    }
    hg_hash_table_register_free_functions(metadata_name_mark_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_metadata_name_mark_hash_value_free);

    is_hash_table_init_g = 1;

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_add_to_bloom(pdc_metadata_t *metadata)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    char combined_string[PATH_MAX];

    combine_obj_info_to_string(metadata, combined_string);
    /* printf("==PDC_SERVER: PDC_Server_add_to_bloom(): Combined string: %s\n", combined_string); */

    BLOOM_TYPE_T *bloom = (BLOOM_TYPE_T*)metadata->bloom;
    if (bloom == NULL) {
        printf("==PDC_SERVER: PDC_Server_add_to_bloom(): bloom pointer is NULL\n");
        ret_value = FAIL;
        goto done;
    }
    ret_value = BLOOM_ADD(bloom, combined_string, strlen(combined_string));

done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_hash_table_list_insert(pdc_metadata_t *head, pdc_metadata_t *new)
{
    FUNC_ENTER(NULL);

    perr_t      ret_value = SUCCEED;

    // Set the bloom filter pointer to all metadata list items so we don't need 
    // to take care of it when head got deleted
    new->bloom = head->bloom; 

    // add to bloom filter
    /* printf("Adding to bloom\n"); */
    PDC_Server_add_to_bloom(new);

    /* printf("Adding to linked list\n"); */
    // Currently $metadata is unique, insert to linked list
    DL_APPEND(head, new);

    /* // Debug print */
    /* int count; */
    /* pdc_metadata_t *elt; */
    /* DL_COUNT(head, elt, count); */
    /* printf("Append one metadata, total=%d\n", count); */
 
done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_hash_table_list_init(pdc_metadata_t *metadata, uint32_t *hash_key)
{
    FUNC_ENTER(NULL);

    perr_t      ret_value = 0;
    hg_return_t ret;

    /* printf("hash key=%d\n", *hash_key); */

    // Init head of linked list
    metadata->prev = metadata;                                                                   \
    metadata->next = NULL;   

    /* PDC_print_metadata(metadata); */

    // Insert to hash table
    ret = hg_hash_table_insert(metadata_hash_table_g, hash_key, metadata);
    if (ret != 1) {
        fprintf(stderr, "PDC_Server_hash_table_list_init(): Error with hash table insert!\n");
        ret_value = -1;
        goto done;
    }

    // Init bloom filter
    /* int capacity = 100000; */
    int capacity = 500000;
    double error_rate = 0.05;
    n_bloom_maybe_g = 0;
    n_bloom_total_g = 0;

    metadata->bloom = (BLOOM_TYPE_T*)BLOOM_NEW(capacity, error_rate);
    if (!metadata->bloom) {
        fprintf(stderr, "ERROR: Could not create bloom filter\n");
        ret_value = -1;
        goto done;
    }
    PDC_Server_add_to_bloom(metadata);

done:
    FUNC_LEAVE(ret_value);
}

perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out) {
    
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;


    /* printf("==PDC_SERVER: Insert obj name marker [%s]\n", in->obj_name); */

    uint32_t *hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("Cannnot allocate hash_key!\n");
        goto done;
    }

    *hash_key = in->hash_value;

    pdc_metadata_name_mark_t *namemark= (pdc_metadata_name_mark_t*)malloc(sizeof(pdc_metadata_name_mark_t));
    if (namemark == NULL) {
        printf("==PDC_SERVER: ERROR - Cannnot allocate pdc_metadata_name_mark_t!\n");
        goto done;
    }
    strcpy(namemark->obj_name, in->obj_name);

    pdc_metadata_name_mark_t *lookup_value;
    pdc_metadata_name_mark_t *elt;

    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_name_mark_hash_table_mutex_g);

    if (metadata_name_mark_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", *hash_key); */
        lookup_value = hg_hash_table_lookup(metadata_name_mark_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            // Check if there exist namemark identical to current one
            if (find_identical_namemark(lookup_value, namemark) == 1) {
                // If find same one, do nothing
                /* printf("==PDC_SERVER: marker exist for [%s]\n", namemark->obj_name); */
                ret_value = 0;
                free(namemark);
            }
            else {
                // Currently namemark is unique, insert to linked list
                DL_PREPEND(lookup_value, namemark);
            }
        
            /* free(hash_key); */
        }
        else {
            /* printf("lookup_value is NULL!\n"); */
            // First entry for current hasy_key, init linked list
            namemark->prev = namemark;                                                                   \
            namemark->next = NULL;   

            // Insert to hash table
            hg_ret = hg_hash_table_insert(metadata_name_mark_hash_table_g, hash_key, namemark);
            if (hg_ret != 1) {
                fprintf(stderr, "==PDC_SERVER: ERROR - insert_obj_name_marker() error with hash table insert!\n");
                ret_value = -1;
                goto done;
            }
        }
    }
    else {
        printf("metadata_hash_table_g not initilized!\n");
        ret_value = -1;
        goto done;
    }

    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_name_mark_hash_table_mutex_g);

done:
    out->ret = 1;
    FUNC_LEAVE(ret_value);
}

perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{

    FUNC_ENTER(NULL);

    perr_t ret_value;

    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    gettimeofday(&ht_total_start, 0);

    /* printf("Got RPC request with name: %s\tHash=%d\n", in->obj_name, in->hash_value); */
    /* printf("Full name check: %s\n", &in->obj_name[507]); */

    pdc_metadata_t *metadata = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    if (metadata == NULL) {
        printf("Cannnot allocate pdc_metadata_t!\n");
        goto done;
    }

    // TODO: [Future work] Both server and client gets it and do security check
    metadata->user_id        = in->data.user_id;
    metadata->time_step      = in->data.time_step;
    strcpy(metadata->obj_name, in->data.obj_name);
    strcpy(metadata->app_name, in->data.app_name);
    strcpy(metadata->tags,     in->data.tags);

    /* strcpy(metadata->data_location, in->data.data_location); */
    /* create_time              =; */
    /* last_modified_time       =; */

    uint32_t *hash_key = (uint32_t*)malloc(sizeof(uint32_t));
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
                PDC_Server_hash_table_list_insert(lookup_value, metadata);
            }
        
        }
        else {
            // First entry for current hasy_key, init linked list, and insert to hash table
            /* printf("lookup_value is NULL!\n"); */
            PDC_Server_hash_table_list_init(metadata, hash_key);
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
    /* PDC_print_metadata(metadata); */

    // Timing
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    hg_thread_mutex_lock(&pdc_time_mutex_g);
    server_insert_time_g += ht_total_sec;
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
    

done:
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    FUNC_LEAVE(ret_value);
}


static perr_t PDC_Server_metadata_duplicate_check()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    hg_hash_table_iter_t hash_table_iter;
    int n_entry, count = 0, dl_count;
    int all_maybe, all_total, all_entry;

    n_entry = hg_hash_table_num_entries(metadata_hash_table_g);

    #ifdef ENABLE_MPI
        MPI_Reduce(&n_bloom_maybe_g, &all_maybe, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&n_bloom_total_g, &all_total, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&n_entry,         &all_entry, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    #else
        all_maybe = n_bloom_maybe_g;
        all_total = n_bloom_total_g;
        all_entry = n_entry;
    #endif

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: Bloom filter says maybe %d times out of %d\n", all_maybe, all_total);
        printf("==PDC_SERVER: Metadata duplicate check with %d hash entries ", all_entry);
    }

    fflush(stdout);

    int has_dup_obj = 0;
    int all_dup_obj = 0;
    pdc_metadata_t *head, *elt, *elt_next;

    hg_hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

    while (n_entry != 0 && hg_hash_table_iter_has_more(&hash_table_iter)) {
        head = hg_hash_table_iter_next(&hash_table_iter);
        /* DL_COUNT(head, elt, dl_count); */
        /* if (pdc_server_rank_g == 0) { */
        /*     printf("  Hash entry[%d], with %d items\n", count, dl_count); */
        /* } */
        DL_SORT(head, PDC_metadata_cmp); 
        // With sorted list, just compare each one with its next
        DL_FOREACH(head, elt) {
            elt_next = elt->next;
            if (elt_next != NULL) {
                if (PDC_metadata_cmp(elt, elt_next) == 0) {
                    PDC_print_metadata(elt);
                    has_dup_obj = 1;
                    ret_value = FAIL;
                    goto done;
                }
            }
        }
        count++;
    }

    fflush(stdout);

done:
    #ifdef ENABLE_MPI
        MPI_Reduce(&has_dup_obj, &all_dup_obj, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    #else
        all_dup_obj = has_dup_obj;
    #endif
    if (pdc_server_rank_g == 0) {
        if (all_dup_obj > 0) {
            printf("  ...Found duplicates!\n");
        }
        else {
            printf("  ...No duplicates found!\n");
        }
    }
 
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_init(int port, hg_class_t **hg_class, hg_context_t **hg_context)
{
    FUNC_ENTER(NULL);
    perr_t ret_value;

    /* PDC_Server_print_version(); */

    // set server id start
    pdc_id_seq_g = pdc_id_seq_g * (pdc_server_rank_g+1);

    // Create server tmp dir
    int status;
    status = mkdir(pdc_server_tmp_dir_g, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

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

    // TODO: support restart with different number of servers than previous run 
    char checkpoint_file[PATH_MAX];
    if (is_restart_g == 1) {
        sprintf(checkpoint_file, "%s/%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);

        // Timing
        struct timeval  ht_total_start;
        struct timeval  ht_total_end;
        long long ht_total_elapsed;
        double restart_time, all_restart_time;
        gettimeofday(&ht_total_start, 0);

        PDC_Server_restart(checkpoint_file);

        // Timing
        gettimeofday(&ht_total_end, 0);
        ht_total_elapsed = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
        restart_time = ht_total_elapsed / 1000000.0;

#ifdef ENABLE_MPI
        MPI_Reduce(&restart_time, &all_restart_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
#else
        all_restart_time = restart_time;
#endif
        if (pdc_server_rank_g == 0) 
            printf("==PDC_SERVER: total restart time = %.6f\n", all_restart_time);
    }
    else {
        // We are starting a brand new server
        if (is_hash_table_init_g != 1) {
            // Hash table init
            PDC_Server_init_hash_table();
        }
    }

    // Initalize atomic variable to finalize server 
    hg_atomic_set32(&close_server_g, 0);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_init

perr_t PDC_Server_finalize()
{
    FUNC_ENTER(NULL);
    perr_t ret_value = SUCCEED;

    // Debug: check duplicates
    PDC_Server_metadata_duplicate_check();
    fflush(stdout);

    // Free hash table
    if(metadata_hash_table_g != NULL)
        hg_hash_table_free(metadata_hash_table_g);

    if(metadata_name_mark_hash_table_g != NULL)
        hg_hash_table_free(metadata_name_mark_hash_table_g);

#ifdef ENABLE_MPI
    double all_bloom_check_time_max, all_bloom_check_time_min, all_insert_time_max, all_insert_time_min;
    MPI_Reduce(&server_bloom_check_time_g, &all_bloom_check_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_bloom_check_time_g, &all_bloom_check_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_insert_time_g,      &all_insert_time_max,      1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_insert_time_g,      &all_insert_time_min,      1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: total bloom check time = %.6f, %.6f\n", all_bloom_check_time_min, all_bloom_check_time_max);
        printf("==PDC_SERVER: total insert      time = %.6f, %.6f\n", all_insert_time_min, all_insert_time_max);
        fflush(stdout);
    }
#else
    printf("==PDC_SERVER: total bloom check time = %.6f\n", server_bloom_check_time_g);
    printf("==PDC_SERVER: total insert      time = %.6f\n", server_insert_time_g);
    fflush(stdout);
#endif
    // TODO: remove server tmp dir?


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

// Backup in-memory DHT/Bloom to persist storage
perr_t PDC_Server_checkpoint(char *filename)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    if (pdc_server_rank_g == 0) {
        printf("\n\n==PDC_SERVER: Start checkpoint process [%s]\n", filename);
    }

    FILE *file = fopen(filename, "w+");
    if (file==NULL) {fputs("==PDC_SERVER: PDC_Server_checkpoint() - Checkpoint file open error", stderr); return -1;}

    // DHT 
    pdc_metadata_t *head, *elt;
    int count, n_entry;
    n_entry = hg_hash_table_num_entries(metadata_hash_table_g);
    /* printf("%d entries\n", n_entry); */
    fwrite(&n_entry, sizeof(int), 1, file);

    hg_hash_table_iter_t hash_table_iter;
    hg_hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

    while (n_entry != 0 && hg_hash_table_iter_has_more(&hash_table_iter)) {
        head = hg_hash_table_iter_next(&hash_table_iter);
        DL_COUNT(head, elt, count);
        /* printf("count=%d\n", count); */
        /* fflush(stdout); */

        fwrite(&count, sizeof(int), 1, file);
        // TODO: find a way to get hash_key from hash table istead of calculating again.
        uint32_t hash_key = PDC_get_hash_by_name(head->obj_name);
        fwrite(&hash_key, sizeof(uint32_t), 1, file);
        // Iterate every metadata structure in current entry
        DL_FOREACH(head, elt) {
            /* printf("==PDC_SERVER: Writing one metadata...\n"); */
            /* PDC_print_metadata(elt); */
            fwrite(elt, sizeof(pdc_metadata_t), 1, file);
        }
    }

    fclose(file);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

// Restart in-memory DHT/Bloom filters from persist storage
perr_t PDC_Server_restart(char *filename)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = 1;
    int n_entry, count, i, j, nobj = 0, all_nobj = 0;

    FILE *file = fopen(filename, "r");
    if (file==NULL) {fputs("==PDC_SERVER: PDC_Server_restart() - Checkpoint file not available\n", stderr); return -1;}

    // init hash table
    PDC_Server_init_hash_table();
    fread(&n_entry, sizeof(int), 1, file);
    /* printf("%d entries\n", n_entry); */

    pdc_metadata_t *entry, *elt;
    uint32_t *hash_key;
    while (n_entry--) {
        fread(&count, sizeof(int), 1, file);
        /* printf("Count:%d\n", count); */

        hash_key = (uint32_t *)malloc(sizeof(uint32_t));
        fread(hash_key, sizeof(uint32_t), 1, file);
        /* printf("Hash key is %u\n", *hash_key); */

        // Reconstruct hash table
        entry = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t) * count);
        fread(entry, sizeof(pdc_metadata_t), count, file);
        nobj += count;

        // Debug print for loaded metadata from checkpoint file
        /* for (i = 0; i < count; i++) { */
        /*     elt = entry + i; */
        /*     PDC_print_metadata(elt); */
        /* } */

        // Init hash table entry (w/ bloom) with first obj
        PDC_Server_hash_table_list_init(entry, hash_key);

        // Insert the rest objs to the linked list
        for (i = 1; i < count; i++) {
            elt = entry + i;
            // Add to hash list and bloom filter
            PDC_Server_hash_table_list_insert(entry, elt);
        }
    }

    fclose(file);

#ifdef ENABLE_MPI
    MPI_Reduce(&nobj, &all_nobj, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_nobj = nobj;
#endif 
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: Server restarted from saved session, successfully loaded %d objects...\n", all_nobj);
    }
    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
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
    /* hg_thread_mutex_destroy(&close_server_g); */

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

perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    pdc_metadata_t *lookup_value;

    *out = NULL;

    // Set up a metadata struct to query
    pdc_metadata_t metadata;
    strcpy(metadata.obj_name, obj_name);

    // Obtain lock for hash table
    /* int unlocked = 0; */
    /* hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g); */

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", *hash_key); */
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            /* printf("==PDC_SERVER: PDC_Server_search_with_name_hash(): lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            /* if (find_identical_metadata(lookup_value, &metadata) == 1) { */
            if (strcmp(lookup_value->obj_name, obj_name) == 0) {
                // There is one identical to current query
                // TODO: multiple objects satisfy current query
                *out = lookup_value; 
                /* printf("==PDC_SERVER: PDC_Server_search_with_name_hash(): Found exact match for query!\n"); */
            }
            else {
                // Collision no such thing exist
                *out = NULL;
            }
        
        }
        else {
            // First entry for current hasy_key, init linked list, and insert to hash table
            *out = NULL;
            printf("==PDC_SERVER: Queried object with name [%s] not found!\n", obj_name);
        }

    }
    else {
        printf("metadata_hash_table_g not initilized!\n");
        ret_value = -1;
        goto done;
    }

    // ^ Release hash table lock
    /* hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g); */
    /* unlocked = 1; */

done:
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
    is_restart_g = 0;
    port = pdc_server_rank_g + 6600;
    /* printf("rank=%d, port=%d\n", pdc_server_rank_g,port); */

    // Timing
    struct timeval  start;
    struct timeval  end;
    long long elapsed;
    double server_init_time, all_server_init_time;
    gettimeofday(&start, 0);



    if (argc > 1) {
        if (strcmp(argv[1], "restart") == 0) 
            is_restart_g = 1;
    }
    PDC_Server_init(port, &hg_class, &hg_context);
    if (hg_class == NULL || hg_context == NULL) {
        printf("Error with Mercury init\n");
        goto done;
    }

    // Register RPC
    client_test_connect_register(hg_class);
    gen_obj_id_register(hg_class);
    close_server_register(hg_class);
    send_obj_name_marker_register(hg_class);
    metadata_query_register(hg_class);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Timing
    gettimeofday(&end, 0);
    elapsed = (end.tv_sec-start.tv_sec)*1000000LL + end.tv_usec-start.tv_usec;
    server_init_time = elapsed / 1000000.0;


    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: total startup time = %.6f\n", server_init_time);
        printf("==PDC_SERVER: Server ready!\n\n\n");
    }
    fflush(stdout);

#ifdef ENABLE_MULTITHREAD
    PDC_Server_multithread_loop(hg_class, hg_context);
#else
    PDC_Server_loop(hg_class, hg_context);
#endif

    // Finalize 
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);

    // TODO: instead of checkpoint at app finalize time, try checkpoint with a time countdown or # of objects
    char checkpoint_file[PATH_MAX];
    sprintf(checkpoint_file, "%s/%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);

    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double checkpoint_time, all_checkpoint_time;
    gettimeofday(&ht_total_start, 0);

    PDC_Server_checkpoint(checkpoint_file);

    // Timing
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    checkpoint_time = ht_total_elapsed / 1000000.0;

#ifdef ENABLE_MPI
    MPI_Reduce(&checkpoint_time, &all_checkpoint_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
#else
    all_checkpoint_time = checkpoint_time;
#endif
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: total checkpoint  time = %.6f\n", all_checkpoint_time);
    }


    PDC_Server_finalize();
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: exiting...\n");
        /* printf("==PDC_SERVER: [%d] exiting...\n", pdc_server_rank_g); */
    }



done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
