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
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctype.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <sys/shm.h>
#include <sys/mman.h>

#include "config.h"

#include "mercury.h"
#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"

#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"
#include "../server/utlist.h"
#include "../server/pdc_server.h"
#include "pdc_malloc.h"

#ifdef ENABLE_MULTITHREAD 
// Mercury multithread
//#include "mercury_thread_mutex.h"
hg_thread_mutex_t insert_metadata_mutex_g = HG_THREAD_MUTEX_INITIALIZER;

// Thread
hg_thread_pool_t *hg_test_thread_pool_g = NULL;
hg_thread_pool_t *hg_test_thread_pool_fs_g = NULL;
#endif

uint64_t pdc_id_seq_g = PDC_SERVER_ID_INTERVEL;
// actual value for each server is set by PDC_Server_init()
//

#if defined(IS_PDC_SERVER) && defined(ENABLE_MULTITHREAD)

// Macros for multi-thread callback, grabbed from Mercury/Testing/mercury_rpc_cb.c
#define HG_TEST_RPC_CB(func_name, handle) \
    static hg_return_t \
    func_name ## _thread_cb(hg_handle_t handle)

/* Assuming func_name_cb is defined, calling HG_TEST_THREAD_CB(func_name)
 * will define func_name_thread and func_name_thread_cb that can be used
 * to execute RPC callback from a thread
 */
#define HG_TEST_THREAD_CB(func_name) \
        static HG_INLINE HG_THREAD_RETURN_TYPE \
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
            struct hg_thread_work *work = HG_Get_data(handle); \
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

double PDC_get_elapsed_time_double(struct timeval *tstart, struct timeval *tend)
{
    return (double)(((tend->tv_sec-tstart->tv_sec)*1000000LL + tend->tv_usec-tstart->tv_usec) / 1000000.0);
}

perr_t PDC_get_self_addr(hg_class_t* hg_class, char* self_addr_string)
{
    perr_t ret_value;
    hg_addr_t self_addr;
    hg_size_t self_addr_string_size = ADDR_MAX;
    
    FUNC_ENTER(NULL);
 
    // Get self addr to tell client about 
    HG_Addr_self(hg_class, &self_addr);
    HG_Addr_to_string(hg_class, self_addr_string, &self_addr_string_size, self_addr);
    HG_Addr_free(hg_class, self_addr);

    ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}

uint32_t PDC_get_local_server_id(int my_rank, int n_client_per_server, int n_server)
{
    return  (my_rank / n_client_per_server) % n_server;
}


uint32_t PDC_get_server_by_obj_id(uint64_t obj_id, int n_server)
{
    // TODO: need a smart way to deal with server number change
    uint32_t ret_value;
    
    FUNC_ENTER(NULL);
    
    ret_value  = (uint32_t)(obj_id / PDC_SERVER_ID_INTERVEL) - 1;
    ret_value %= n_server;

    FUNC_LEAVE(ret_value);
}

static uint32_t pdc_hash_djb2(const char *pc)
{
    uint32_t hash = 5381, c;
    
    FUNC_ENTER(NULL);
    
    while (c = *pc++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    /* if (hash < 0) */
    /*     hash *= -1; */

    return hash;
}

int pdc_msleep(unsigned long milisec)
{
    struct timespec req={0};
    time_t sec=(int)(milisec/1000);
    milisec=milisec-(sec*1000);
    req.tv_sec=sec;
    req.tv_nsec=milisec*1000000L;
    while(nanosleep(&req,&req)==-1)
         continue;
    return 1;
}

/*
static uint32_t pdc_hash_sdbm(const char *pc)
{
    uint32_t hash = 0, c;
    
    FUNC_ENTER(NULL);
    
    while (c = (*pc++))
        hash = c + (hash << 6) + (hash << 16) - hash;
    if (hash < 0)
        hash *= -1;
    return hash;
}
 */

uint32_t PDC_get_hash_by_name(const char *name)
{
    return pdc_hash_djb2(name); 
}

uint32_t PDC_get_server_by_name(char *name, int n_server)
{
    // TODO: need a smart way to deal with server number change
    uint32_t ret_value;
    
    FUNC_ENTER(NULL);
    
    ret_value  = PDC_get_hash_by_name(name) % n_server;

    FUNC_LEAVE(ret_value);
}

int PDC_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b)
{
    int ret = 0;
    
    FUNC_ENTER(NULL);
    
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
    if (a->user_id > 0 && b->user_id > 0) {
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

void pdc_mkdir(const char *dir) 
{
    char tmp[ADDR_MAX];
    char *p = NULL;
    /* size_t len; */

    snprintf(tmp, sizeof(tmp),"%s",dir);
    /* len = strlen(tmp); */
    /* if(tmp[len - 1] == '/') */
    /*     tmp[len - 1] = 0; */
    for(p = tmp + 1; *p; p++)
        if(*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU | S_IRWXG);
            *p = '/';
        }
    /* mkdir(tmp, S_IRWXU | S_IRWXG); */
}

void PDC_print_metadata(pdc_metadata_t *a)
{
    size_t i;
    region_list_t *elt;
    FUNC_ENTER(NULL);
    
    if (a == NULL) {
        printf("==Empty metadata structure\n");
        return;
    }
    printf("================================\n");
    printf("  obj_id    = %" PRIu64 "\n", a->obj_id);
    printf("  uid       = %d\n",   a->user_id);
    printf("  app_name  = [%s]\n",   a->app_name);
    printf("  obj_name  = [%s]\n",   a->obj_name);
    printf("  obj_loc   = [%s]\n",   a->data_location);
    printf("  time_step = %d\n",     a->time_step);
    printf("  tags      = [%s]\n",   a->tags);
    printf("  ndim      = %lu\n",    a->ndim);
    printf("  dims = %" PRIu64 "",   a->dims[0]);
    for (i = 1; i < a->ndim; i++) 
        printf(", %" PRIu64 "",      a->dims[i]);
    // print regiono info
    DL_FOREACH(a->storage_region_list_head, elt)
        PDC_print_region_list(elt);
    printf("\n================================\n\n");
    fflush(stdout);
}

perr_t PDC_metadata_init(pdc_metadata_t *a)
{
    if (a == NULL) {
        printf("Unable to init NULL pdc_metadata_t\n");
        return FAIL;
    }
    a->user_id            = 0;
    a->time_step          = -1;
    a->obj_id             = 0;
    a->create_time        = 0;
    a->last_modified_time = 0;
    a->ndim = 0;

    memset(a->app_name,      0, sizeof(char)*ADDR_MAX);
    memset(a->obj_name,      0, sizeof(char)*ADDR_MAX);
    memset(a->tags,          0, sizeof(char)*TAG_LEN_MAX);
    memset(a->data_location, 0, sizeof(char)*ADDR_MAX);
    memset(a->dims,          0, sizeof(uint64_t)*DIM_MAX);

    a->storage_region_list_head = NULL;
    a->region_lock_head = NULL;
    a->region_map_head = NULL;
    a->region_buf_map_head  = NULL;
    a->prev  = NULL;
    a->next  = NULL;
    a->bloom = NULL;

    return SUCCEED;
}

perr_t PDC_init_region_list(region_list_t *a)
{
    perr_t ret_value = SUCCEED;

    memset(a, 0, sizeof(region_list_t));
    a->shm_fd        = -1;
    a->data_loc_type = NONE;
    hg_atomic_init32(&(a->buf_map_refcount), 0);
    a->reg_dirty_from_buf     = 0;
    a->lock_handle   = NULL;
    a->access_type   = NA;
    return ret_value;
}

// currently assumes both region are of same object, so only compare ndim, start, and count.
int PDC_is_same_region_list(region_list_t *a, region_list_t *b)
{
    int ret_value = 1;
    size_t i = 0;

    if (NULL == a || NULL == b) {
        printf("==Empty region_list_t structure\n");
        return -1;
    }

    if (a->ndim != b->ndim) 
        return -1;

    for (i = 0; i < a->ndim; i++) {

        if (a->start[i] != b->start[i]) 
            return -1;

        if (a->count[i] != b->count[i])
            return -1;
    }

/* done: */
    return ret_value;
}

int PDC_is_same_region_transfer(region_info_transfer_t *a, region_info_transfer_t *b)
{
    if (NULL == a || NULL == b) {
        printf("==Empty region_info_transfer_t structure\n");
        return -1;
    }

    if (a->ndim != b->ndim)
        return -1;

    if (a->ndim >= 1) 
        if(a->start_0 != b->start_0 || a->count_0 != b->count_0)
            return -1;

    if (a->ndim >= 2)
        if(a->start_1 != b->start_1 || a->count_1 != b->count_1)
            return -1;

    if (a->ndim >= 3)
        if(a->start_2 != b->start_2 || a->count_2 != b->count_2)
            return -1;

    if (a->ndim >= 4)
        if(a->start_3 != b->start_3 || a->count_3 != b->count_3)
            return -1;

    return 0;
}

void PDC_print_storage_region_list(region_list_t *a)
{
    FUNC_ENTER(NULL);
    
    if (a == NULL) {
        printf("==Empty region_list_t structure\n");
        return;
    }

    if (a->ndim > 4) {
        printf("==Error with ndim %lu\n", a->ndim);
        return;
    }
    size_t i;
    printf("================================\n");
    printf("  ndim      = %lu\n",   a->ndim);
    printf("  start    count\n");
    /* printf("start stride count\n"); */
    for (i = 0; i < a->ndim; i++) {
        printf("  %5" PRIu64 "    %5" PRIu64 "\n", a->start[i], a->count[i]);
        /* printf("%5d %6d %5d\n", a->start[i], a->stride[i], a->count[i]); */
    }
    printf("    path: %s\n", a->storage_location);
    printf(" buf_map: %d\n", a->buf_map_refcount);
    printf("   dirty: %d\n", a->reg_dirty_from_buf);
    printf("  offset: %" PRIu64 "\n", a->offset);
   
    printf("================================\n\n");
    fflush(stdout);
}

void PDC_print_region_list(region_list_t *a)
{
    FUNC_ENTER(NULL);
    
    if (a == NULL) {
        printf("==Empty region_list_t structure\n");
        return;
    }
    size_t i;
    printf("\n  == Region Info ==\n");
    printf("    ndim      = %lu\n",   a->ndim);
    if (a->ndim > 4) {
        printf("Error with dim %lu\n", a->ndim);
        return;
    }
    printf("    start    count\n");
    /* printf("start stride count\n"); */
    for (i = 0; i < a->ndim; i++) {
        printf("    %5" PRIu64"    %5" PRIu64"\n", a->start[i], a->count[i]);
        /* printf("%5d %6d %5d\n", a->start[i], a->stride[i], a->count[i]); */
    }
    printf("    Storage location: [%s]\n", a->storage_location);
    printf("    Storage offset  : %" PRIu64 " \n", a->offset);
    printf("    Client IDs: ");
    i = 0;
    while (1) {
        printf("%u, ", a->client_ids[i]);
        i++;
        if (a->client_ids[i] == 0 ) 
            break;
    }
    printf("\n  =================\n");
    fflush(stdout);
}

uint64_t pdc_get_region_size(region_list_t *a)
{
    unsigned i;
    uint64_t size = 1;
    for (i = 0; i < a->ndim; i++) 
        size *= a->count[i];
    
    return size;
}

perr_t pdc_region_list_t_deep_cp(region_list_t *from, region_list_t *to)
{
    if (NULL == from || NULL == to) {
        printf("pdc_region_list_t_deep_cp(): NULL input!\n");
        return FAIL;
    }

    if (from->ndim > 4 || from->ndim <= 0) {
        printf("pdc_region_list_t_deep_cp(): ndim %zu ERROR!\n", from->ndim);
        return FAIL;
    }

    memcpy(to, from, sizeof(region_list_t));

    memcpy(to->start, from->start, sizeof(uint64_t)*from->ndim);
    memcpy(to->count, from->count, sizeof(uint64_t)*from->ndim);
    memcpy(to->client_ids, from->client_ids, sizeof(uint32_t)*PDC_SERVER_MAX_PROC_PER_NODE);
    memcpy(to->shm_addr, from->shm_addr, sizeof(char) * ADDR_MAX);
    strcpy(to->storage_location, from->storage_location);
    strcpy(to->cache_location, from->cache_location);

    return SUCCEED;
}

int PDC_region_list_seq_id_cmp(region_list_t *a, region_list_t *b)
{
    return (a->seq_id > b->seq_id) ? 1 : -1;
}

perr_t pdc_region_transfer_t_to_list_t(region_info_transfer_t *transfer, region_list_t *region)
{
    if (NULL==region || NULL==transfer ) {
        printf("    pdc_region_transfer_t_to_list_t(): NULL input!\n");
        return FAIL;
    }

    region->ndim            = transfer->ndim;
    region->start[0]        = transfer->start_0;
    region->count[0]        = transfer->count_0;

    if (region->ndim > 1) {
        region->start[1]        = transfer->start_1;
        region->count[1]        = transfer->count_1;
    }

    if (region->ndim > 2) {
        region->start[2]        = transfer->start_2;
        region->count[2]        = transfer->count_2;
    }

    if (region->ndim > 3) {
        region->start[3]        = transfer->start_3;
        region->count[3]        = transfer->count_3;
    }

    /* region->stride[0]       = transfer->stride_0; */
    /* region->stride[1]       = transfer->stride_1; */
    /* region->stride[2]       = transfer->stride_2; */
    /* region->stride[3]       = transfer->stride_3; */

    return SUCCEED;
}

perr_t pdc_region_info_to_list_t(struct PDC_region_info *region, region_list_t *list)
{
    size_t i;

    if (NULL==region || NULL==list ) {
        printf("    pdc_region_info_to_list_t(): NULL input!\n");
        return FAIL;
    }

    size_t ndim = region->ndim;
    if (ndim <= 0 || ndim >=5) {
        printf("pdc_region_info_to_list_t() unsupported dim: %lu\n", ndim);
        return FAIL;
    }

    list->ndim = ndim;
    for (i = 0; i < ndim; i++) {
        list->start[i]  = region->offset[i];
        list->count[i]  = region->size[i];
        /* list->stride[i] = 0; */
    }
    
    return SUCCEED;
}


perr_t pdc_region_info_t_to_transfer(struct PDC_region_info *region, region_info_transfer_t *transfer)
{
    if (NULL==region || NULL==transfer ) {
        printf("    pdc_region_info_t_to_transfer(): NULL input!\n");
        return FAIL;
    }

    size_t ndim = region->ndim;
    if (ndim <= 0 || ndim >=5) {
        printf("pdc_region_info_t_to_transfer() unsupported dim: %lu\n", ndim);
        return FAIL;
    }
    
    transfer->ndim = ndim;
    if (ndim >= 1)      transfer->start_0  = region->offset[0];
    else                transfer->start_0  = 0;

    if (ndim >= 2)      transfer->start_1  = region->offset[1];
    else                transfer->start_1  = 0;

    if (ndim >= 3)      transfer->start_2  = region->offset[2];
    else                transfer->start_2  = 0;

    if (ndim >= 4)      transfer->start_3  = region->offset[3];
    else                transfer->start_3  = 0;


    if (ndim >= 1)      transfer->count_0  = region->size[0];
    else                transfer->count_0  = 0;

    if (ndim >= 2)      transfer->count_1  = region->size[1];
    else                transfer->count_1  = 0;

    if (ndim >= 3)      transfer->count_2  = region->size[2];
    else                transfer->count_2  = 0;

    if (ndim >= 4)      transfer->count_3  = region->size[3];
    else                transfer->count_3  = 0;

    /* if (ndim >= 1)      transfer->stride_0 = 0; */
    /* if (ndim >= 2)      transfer->stride_1 = 0; */
    /* if (ndim >= 3)      transfer->stride_2 = 0; */
    /* if (ndim >= 4)      transfer->stride_3 = 0; */

    /* transfer->stride_0 = 0; */
    /* transfer->stride_1 = 0; */
    /* transfer->stride_2 = 0; */
    /* transfer->stride_3 = 0; */

    return SUCCEED;
}

perr_t pdc_region_info_t_to_transfer_unit(struct PDC_region_info *region, region_info_transfer_t *transfer, size_t unit)
{
    if (NULL==region || NULL==transfer ) {
        printf("    pdc_region_info_t_to_transfer_unit(): NULL input!\n");
        return FAIL;
    }

    size_t ndim = region->ndim;
    if (ndim <= 0 || ndim >=5) {
        printf("pdc_region_info_t_to_transfer() unsupported dim: %lu\n", ndim);
        return FAIL;
    }

    transfer->ndim = ndim;
    if (ndim >= 1)      transfer->start_0  = unit * region->offset[0];
    else                transfer->start_0  = 0;

    if (ndim >= 2)      transfer->start_1  = unit * region->offset[1];
    else                transfer->start_1  = 0;

    if (ndim >= 3)      transfer->start_2  = unit * region->offset[2];
    else                transfer->start_2  = 0;

    if (ndim >= 4)      transfer->start_3  = unit * region->offset[3];
    else                transfer->start_3  = 0;


    if (ndim >= 1)      transfer->count_0  = unit * region->size[0];
    else                transfer->count_0  = 0;

    if (ndim >= 2)      transfer->count_1  = unit * region->size[1];
    else                transfer->count_1  = 0;

    if (ndim >= 3)      transfer->count_2  = unit * region->size[2];
    else                transfer->count_2  = 0;

    if (ndim >= 4)      transfer->count_3  = unit * region->size[3];
    else                transfer->count_3  = 0;

    /* if (ndim >= 1)      transfer->stride_0 = 0; */
    /* if (ndim >= 2)      transfer->stride_1 = 0; */
    /* if (ndim >= 3)      transfer->stride_2 = 0; */
    /* if (ndim >= 4)      transfer->stride_3 = 0; */

    /* transfer->stride_0 = 0; */
    /* transfer->stride_1 = 0; */
    /* transfer->stride_2 = 0; */
    /* transfer->stride_3 = 0; */

    return SUCCEED;
}

perr_t pdc_region_transfer_t_to_region_info(region_info_transfer_t *transfer, struct PDC_region_info *region)
{
    if (NULL==region || NULL==transfer ) {
        printf("    pdc_region_transfer_t_to_region_info(): NULL input!\n");
        return FAIL;
    }
    return 0; 
}

perr_t pdc_region_list_t_to_transfer(region_list_t *region, region_info_transfer_t *transfer)
{
    if (NULL==region || NULL==transfer ) {
        printf("    pdc_region_list_t_to_transfer(): NULL input!\n");
        return FAIL;
    }

    transfer->ndim          = region->ndim    ;
    transfer->start_0        = region->start[0];
    transfer->start_1        = region->start[1];
    transfer->start_2        = region->start[2];
    transfer->start_3        = region->start[3];

    transfer->count_0        = region->count[0];
    transfer->count_1        = region->count[1];
    transfer->count_2        = region->count[2];
    transfer->count_3        = region->count[3];

    /* transfer->stride_0       = region->stride[0]; */
    /* transfer->stride_1       = region->stride[1]; */
    /* transfer->stride_2       = region->stride[2]; */
    /* transfer->stride_3       = region->stride[3]; */

    return SUCCEED;
}

// Fill the structure of pdc_metadata_transfer_t with pdc_metadata_t
perr_t pdc_metadata_t_to_transfer_t(pdc_metadata_t *meta, pdc_metadata_transfer_t *transfer)
{
    if (NULL==meta || NULL==transfer) {
        printf("    pdc_metadata_t_to_transfer_t(): NULL input!\n");
        return FAIL;
    }
    transfer->user_id       = meta->user_id      ;
    transfer->app_name      = meta->app_name     ;
    transfer->obj_name      = meta->obj_name     ;
    transfer->time_step     = meta->time_step    ;
    transfer->obj_id        = meta->obj_id       ;
    transfer->cont_id       = meta->cont_id      ;
    transfer->ndim          = meta->ndim         ;
    transfer->dims0         = meta->dims[0]      ;
    transfer->dims1         = meta->dims[1]      ;
    transfer->dims2         = meta->dims[2]      ;
    transfer->dims3         = meta->dims[3]      ;
    transfer->tags          = meta->tags         ;
    transfer->data_location = meta->data_location;

    return SUCCEED;
}

perr_t pdc_transfer_t_to_metadata_t(pdc_metadata_transfer_t *transfer, pdc_metadata_t *meta)
{
    if (NULL==meta || NULL==transfer) {
        printf("    pdc_transfer_t_to_metadata_t(): NULL input!\n");
        return FAIL;
    }
    meta->user_id       = transfer->user_id;
    meta->obj_id        = transfer->obj_id;
    meta->cont_id       = transfer->cont_id;
    meta->time_step     = transfer->time_step;
    meta->ndim          = transfer->ndim;
    meta->dims[0]       = transfer->dims0;
    meta->dims[1]       = transfer->dims1;
    meta->dims[2]       = transfer->dims2;
    meta->dims[3]       = transfer->dims3;

    strcpy(meta->app_name, transfer->app_name);
    strcpy(meta->obj_name, transfer->obj_name);
    strcpy(meta->tags, transfer->tags);
    strcpy(meta->data_location, transfer->data_location);

    return SUCCEED;
}


#ifndef IS_PDC_SERVER
// Dummy function for client to compile, real function is used only by server and code is in pdc_server.c
pbool_t region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2) {return SUCCEED;}
hg_return_t PDC_Server_get_client_addr(const struct hg_cb_info *callback_info) {return SUCCEED;}
perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out) {return SUCCEED;}
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out) {return SUCCEED;}
perr_t PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts, pdc_metadata_t** out) {return SUCCEED;}
perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out) {return SUCCEED;}
perr_t PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out) {return SUCCEED;}
perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out) {return SUCCEED;}
perr_t PDC_Server_add_tag_metadata(metadata_add_tag_in_t *in, metadata_add_tag_out_t *out) {return SUCCEED;}
perr_t PDC_Meta_Server_buf_unmap(buf_unmap_in_t *in, hg_handle_t *handle) {return SUCCEED;}
perr_t PDC_Data_Server_buf_unmap(const struct hg_info *info, buf_unmap_in_t *in) {return SUCCEED;}
perr_t PDC_Meta_Server_buf_map(buf_map_in_t *in, region_buf_map_t *new_buf_map_ptr, hg_handle_t *handle) {return SUCCEED;}
perr_t PDC_Data_Server_region_release(region_lock_in_t *in, region_lock_out_t *out) {return SUCCEED;}
perr_t PDC_Data_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out, hg_handle_t *handle) {return SUCCEED;}
//perr_t PDC_Server_region_lock_status(pdcid_t obj_id, region_info_transfer_t *region, int *lock_status) {return SUCCEED;}
perr_t PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status) {return SUCCEED;}
perr_t PDC_Server_local_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status) {return SUCCEED;}
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs) {return SUCCEED;}
perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id) {return SUCCEED;}
perr_t PDC_Server_release_lock_request(uint64_t obj_id, struct PDC_region_info *region) {return SUCCEED;}
perr_t PDC_Server_data_write_out(uint64_t obj_id, struct PDC_region_info *region_info, void *buf) {return SUCCEED;}
perr_t PDC_Server_data_read_from(uint64_t obj_id, struct PDC_region_info *region_info, void *buf) {return SUCCEED;}
perr_t PDC_Server_data_read_in(uint64_t obj_id, struct PDC_region_info *region_info, void *buf) {return SUCCEED;}
perr_t PDC_Server_data_write_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf) {return SUCCEED;}
perr_t PDC_Server_data_read_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf) {return SUCCEED;}
perr_t PDC_SERVER_notify_region_update_to_client(uint64_t meta_id, uint64_t reg_id, int32_t client_id) {return SUCCEED;}
perr_t PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta) {return SUCCEED;}
perr_t PDC_Server_get_local_storage_location_of_region(uint64_t obj_id, region_list_t *region,
        uint32_t *n_loc, region_list_t **overlap_region_loc) {return SUCCEED;}
perr_t PDC_Server_get_total_str_len(region_list_t** regions, uint32_t n_region, uint32_t *len) {return SUCCEED;}
perr_t PDC_Server_serialize_regions_info(region_list_t** regions, uint32_t n_region, void *buf) {return SUCCEED;}
pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id) {return NULL;}
perr_t PDC_Server_update_region_storage_meta_bulk_local(update_region_storage_meta_bulk_t **bulk_ptrs, int cnt){return SUCCEED;}
hg_return_t PDC_Server_count_write_check_update_storage_meta_cb (const struct hg_cb_info *callback_info) {return SUCCEED;}
perr_t PDC_Server_create_container(gen_cont_id_in_t *in, gen_cont_id_out_t *out) {return SUCCEED;}

/* hg_return_t PDC_Server_s2s_recv_work_done_cb(const struct hg_cb_info *callback_info) {return SUCCEED;} */
perr_t PDC_Server_set_close() {return SUCCEED;}
hg_return_t PDC_Server_checkpoint_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;}

data_server_region_t *PDC_Server_get_obj_region(pdcid_t obj_id) {return NULL;}
region_buf_map_t *PDC_Data_Server_buf_map(const struct hg_info *info, buf_map_in_t *in, region_list_t *request_region, void *data_ptr) {return SUCCEED;}
void *PDC_Server_get_region_buf_ptr(pdcid_t obj_id, region_info_transfer_t region) {return NULL;}
void *PDC_Server_get_region_obj_ptr(pdcid_t obj_id, region_info_transfer_t region) {return NULL;}



hg_class_t *hg_class_g;

/* 
 * Data server related
 */
hg_return_t PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info) {return HG_SUCCESS;}
perr_t PDC_Server_read_check(data_server_read_check_in_t *in, data_server_read_check_out_t *out) {return SUCCEED;}
perr_t PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out) {return SUCCEED;}
hg_return_t PDC_Server_s2s_work_done_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;}

perr_t PDC_Server_container_add_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id) {return SUCCEED;}
perr_t PDC_Server_container_del_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id) {return SUCCEED;}
hg_return_t PDC_Server_query_read_names_cb(const struct hg_cb_info *callback_info) {return SUCCEED;}
hg_return_t PDC_Server_query_read_names_clinet_cb(const struct hg_cb_info *callback_info) {return SUCCEED;}

hg_return_t PDC_Server_storage_meta_name_query_bulk_respond(const struct hg_cb_info *callback_info)  {return HG_SUCCESS;};
perr_t PDC_Server_proc_storage_meta_bulk(int task_id, int n_regions, region_list_t *region_list_head) {return SUCCEED;}

#else
hg_return_t PDC_Client_work_done_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;};
hg_return_t PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;};
perr_t PDC_Client_query_read_complete(char *shm_addrs, int size, int n_shm, int seq_id) {return SUCCEED;}
hg_return_t PDC_Client_recv_bulk_storage_meta_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;}
perr_t PDC_Client_recv_bulk_storage_meta(process_bulk_storage_meta_args_t *process_args) {return HG_SUCCESS;}
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
    perr_t ret_value = HG_SUCCESS;
    
    FUNC_ENTER(NULL);

    // Decode input
    gen_obj_id_in_t in;
    gen_obj_id_out_t out;

    HG_Get_input(handle, &in);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&insert_metadata_mutex_g);
#endif
    // Insert to hash table
    ret_value = insert_metadata_to_hash_table(&in, &out);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&insert_metadata_mutex_g);
#endif
    /* printf("==PDC_SERVER: gen_obj_id_cb(): going to return %" PRIu64 "\n", out.obj_id); */
    /* fflush(stdout); */

    HG_Respond(handle, NULL, NULL, &out);

    /* printf("==PDC_SERVER: gen_obj_id_cb(): After HG_Respond%\n"); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* gen_cont_id_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(gen_cont_id, handle)
{
    perr_t ret_value = HG_SUCCESS;
    FUNC_ENTER(NULL);

    /* printf("==PDC_SERVER[x]: %s -  received container creation request\n", __func__); */
    /* fflush(stdout); */

    // Decode input
    gen_cont_id_in_t  in;
    gen_cont_id_out_t out;

    HG_Get_input(handle, &in);

    // Insert to hash table
    ret_value = PDC_Server_create_container(&in, &out);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER: error with container object creation\n");
    }

    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


// This is for the CLIENT
/* static hg_return_t */
/* server_lookup_client_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(server_lookup_client, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    server_lookup_client_in_t  in;
    server_lookup_client_out_t out;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    out.ret = in.server_id + 43210000;

    HG_Respond(handle, PDC_Client_work_done_cb, NULL, &out);

    /* printf("==PDC_CLIENT: server_lookup_client_cb(): Respond %" PRIu64 " to server[%d]\n", out.ret, in.server_id); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}


/* static hg_return_t */
/* server_lookup_remote_server_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(server_lookup_remote_server, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    server_lookup_remote_server_in_t  in;
    server_lookup_remote_server_out_t out;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    out.ret = in.server_id + 1024000;

    HG_Respond(handle, NULL, NULL, &out);

    /* printf("==PDC_SERVER[x]: %s - Responded %" PRIu64 " back to server[%d]\n", __func__, out.ret, in.server_id); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* client_test_connect_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(client_test_connect, handle)
{
    // SERVER EXEC
    hg_return_t ret_value = HG_SUCCESS;
    client_test_connect_in_t  in;
    client_test_connect_out_t out;
    client_test_connect_args *args = (client_test_connect_args*)calloc(1, sizeof(client_test_connect_args));
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    out.ret = in.client_id + 123400;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_client_info_mutex_g);
#endif
    args->client_id = in.client_id;
    args->nclient   = in.nclient;
    sprintf(args->client_addr, in.client_addr);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_client_info_mutex_g);
#endif
    
    /* printf("==PDC_SERVER: got client_test_connect req, going to return %" PRIu64 "\n", out.ret); */
    /* fflush(stdout); */
    /* HG_Respond(handle, NULL, NULL, &out); */
    HG_Respond(handle, PDC_Server_get_client_addr, args, &out);
    /* printf("==PDC_SERVER: client_test_connect(): Done respond to client test connect\n", out.ret); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* metadata_query_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(metadata_query, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    metadata_query_in_t  in;
    metadata_query_out_t out;
    pdc_metadata_t *query_result = NULL;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Received query with name: %s, hash value: %u\n", in.obj_name, in.hash_value); */
    /* fflush(stdout); */

    // Do the work
    /* PDC_Server_search_with_name_hash(in.obj_name, in.hash_value, &query_result); */
    PDC_Server_search_with_name_timestep(in.obj_name, in.hash_value, in.time_step, &query_result);

    // Convert for transfer
    if (query_result != NULL) {
        pdc_metadata_t_to_transfer_t(query_result, &out.ret);
        /* out.ret.user_id        = query_result->user_id; */
        /* out.ret.obj_id         = query_result->obj_id; */
        /* out.ret.time_step      = query_result->time_step; */
        /* out.ret.obj_name       = query_result->obj_name; */
        /* out.ret.app_name       = query_result->app_name; */
        /* out.ret.tags           = query_result->tags; */
        /* out.ret.data_location  = query_result->data_location; */ 
    }
    else {
        out.ret.user_id        = -1;
        out.ret.obj_id         = 0;
        out.ret.time_step      = -1;
        out.ret.obj_name       = "N/A";
        out.ret.app_name       = "N/A";
        out.ret.tags           = "N/A";
        out.ret.data_location  = "N/A"; 
    }

    /* printf("out.ret.data_location: %s\n", out.ret.data_location); */

    HG_Respond(handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER: metadata_query_cb(): Returned obj_name=%s, obj_id=%" PRIu64 "\n", out.ret.obj_name, out.ret.obj_id); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_delete_by_id_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_delete_by_id, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    metadata_delete_by_id_in_t  in;
    metadata_delete_by_id_out_t out;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got delete_by_id request: hash=%d, obj_id=%" PRIu64 "\n", in.hash_value, in.obj_id); */

    PDC_Server_delete_metadata_by_id(&in, &out);
    
    /* out.ret = 1; */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}


/* static hg_return_t */
// metadata_delete_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_delete, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    metadata_delete_in_t  in;
    metadata_delete_out_t out;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got delete request: hash=%d, obj_id=%" PRIu64 "\n", in.hash_value, in.obj_id); */

    delete_metadata_from_hash_table(&in, &out);
    
    /* out.ret = 1; */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_add_tag_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_add_tag, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    // Decode input
    metadata_add_tag_in_t  in;
    metadata_add_tag_out_t out;

    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got add_tag request: hash=%d, obj_id=%" PRIu64 "\n", in.hash_value, in.obj_id); */


    PDC_Server_add_tag_metadata(&in, &out);
    
    /* out.ret = 1; */
    HG_Respond(handle, NULL, NULL, &out);


    ret_value = HG_SUCCESS;

/* done: */
    HG_Free_input(handle, &in);
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// notify_io_complete_cb(hg_handle_t handle)
HG_TEST_RPC_CB(notify_io_complete, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    notify_io_complete_in_t  in;
    notify_io_complete_out_t out;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    PDC_access_t type = (PDC_access_t)in.io_type;

    /* printf("==PDC_CLIENT: Got %s complete notification from server: obj_id=%" PRIu64 ", shm_addr=[%s]\n", */ 
    /*         in.io_type == READ? "read":"write", in.obj_id, in.shm_addr); */
    /* fflush(stdout); */

    client_read_info_t * read_info = (client_read_info_t*)calloc(1, sizeof(client_read_info_t));
    read_info->obj_id = in.obj_id;
    strcpy(read_info->shm_addr, in.shm_addr);
    if (ret_value != HG_SUCCESS) {
        printf("Error with notify_io_complete_register\n");
        goto done;
    }

    out.ret = atoi(in.shm_addr) + 112000;
    if (type == READ) {
        HG_Respond(handle, PDC_Client_get_data_from_server_shm_cb, read_info, &out);
    }
    else if (type == WRITE) {
        HG_Respond(handle, PDC_Client_work_done_cb, read_info, &out);
        /* printf("==PDC_CLIENT: notify_io_complete_cb() respond write confirm confirmation %d\n", out.ret); */
    }
    else {
        printf("==PDC_CLIENT: notify_io_complete_cb() - error with io type!\n");
        HG_Respond(handle, NULL, NULL, &out);
    }


done:
    fflush(stdout);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// notify_region_update_cb(hg_handle_t handle)
HG_TEST_RPC_CB(notify_region_update, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    notify_region_update_in_t  in;
    notify_region_update_out_t out;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    out.ret = 1;
    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_update_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_update, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    metadata_update_in_t  in;
    metadata_update_out_t out;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got update request: hash=%d, obj_id=%" PRIu64 "\n", in.hash_value, in.obj_id); */

    PDC_Server_update_metadata(&in, &out);
    
    /* out.ret = 1; */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// close_server_cb(hg_handle_t handle)
HG_TEST_RPC_CB(close_server, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    close_server_in_t  in;
    /* close_server_out_t out; */
    
    FUNC_ENTER(NULL);

    HG_Get_input(handle, &in);

    PDC_Server_set_close();

    /* out.ret = 1; */
    /* HG_Respond(handle, NULL, NULL, &out); */

    /* printf("\n==PDC_SERVER: Respond back to close server request\n"); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    if (HG_Destroy(handle) != HG_SUCCESS)
        printf("==PDC_SERVER[x]: %s Error with HG_Destroy\n", __func__);

    FUNC_LEAVE(ret_value);
}

/*
//enter this function, transfer is done, data is pushed to mapped region
static hg_return_t
buf_map_region_update_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
    struct buf_region_update_bulk_args *bulk_args = NULL;

    FUNC_ENTER(NULL);

    bulk_args = (struct buf_region_update_bulk_args *)hg_cb_info->arg;
    out.ret = bulk_args->lock_ret;
    HG_Respond(bulk_args->handle, NULL, NULL, &out);

    // Send notification to mapped regions, when data transfer is done
    PDC_SERVER_notify_region_update_to_client(bulk_args->remote_obj_id, bulk_args->remote_reg_id, bulk_args->remote_client_id);

    HG_Bulk_free(bulk_args->bulk_handle);
    free(bulk_args->args->data_buf);
    free(bulk_args->args);
    HG_Destroy(bulk_args->handle);
    free(bulk_args);
}
*/

static HG_THREAD_RETURN_TYPE
pdc_region_write_out_progress(void *arg)
{
    HG_THREAD_RETURN_TYPE ret_value = (HG_THREAD_RETURN_TYPE) 0;
    struct buf_map_release_bulk_args *bulk_args = (struct buf_map_release_bulk_args *)arg;
    struct PDC_region_info *remote_reg_info = NULL;
    region_lock_out_t out;

    FUNC_ENTER(NULL);

    remote_reg_info = (struct PDC_region_info *)malloc(sizeof(struct PDC_region_info));
    if(remote_reg_info == NULL) {
        printf("pdc_region_write_out_progress: remote_reg_info memory allocation failed\n");
        goto done;
    }
    remote_reg_info->ndim = (bulk_args->remote_region).ndim;
    remote_reg_info->offset = (uint64_t *)malloc(sizeof(uint64_t));
    remote_reg_info->size = (uint64_t *)malloc(sizeof(uint64_t));
    (remote_reg_info->offset)[0] = (bulk_args->remote_region).start_0;
    (remote_reg_info->size)[0] = (bulk_args->remote_region).count_0;

    PDC_Server_data_write_out(bulk_args->remote_obj_id, remote_reg_info, bulk_args->data_buf);

    // Perform lock release function
    PDC_Data_Server_region_release(&(bulk_args->in), &out);

    PDC_Server_release_lock_request(bulk_args->remote_obj_id, remote_reg_info);

    free(remote_reg_info->offset);
    free(remote_reg_info->size);
    free(remote_reg_info);

done:
    fflush(stdout);
    HG_Bulk_free(bulk_args->remote_bulk_handle);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);
//    free(bulk_args);

    hg_thread_mutex_lock(&(bulk_args->work_mutex));
    bulk_args->work_completed = 1;
    hg_thread_cond_signal(&(bulk_args->work_cond));
    hg_thread_mutex_unlock(&(bulk_args->work_mutex));

    FUNC_LEAVE(ret_value);
}

//enter this function, transfer is done, data is pushed to buffer
static hg_return_t
obj_map_region_release_bulk_transfer_thread_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
    struct buf_map_release_bulk_args *bulk_args = NULL;
    int error = 0;
    
    FUNC_ENTER(NULL);

    bulk_args = (struct buf_map_release_bulk_args *)hg_cb_info->arg;
    handle = bulk_args->handle;
    
    hg_info = HG_Get_info(handle);
    
    if (hg_cb_info->ret == HG_CANCELED) {
        error = 1;
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "HG_Bulk_transfer() was successfully canceled\n");
    } else if (hg_cb_info->ret != HG_SUCCESS) {
        error = 1;
        out.ret = 0;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in region_release_bulk_transfer_cb()");
    }
    
    // release the lock
    PDC_Data_Server_region_release(&(bulk_args->in), &out);
    
    PDC_Server_release_lock_request(bulk_args->remote_obj_id, bulk_args->remote_reg_info);
//    HG_Respond(bulk_args->handle, NULL, NULL, &out);
    
done:
    free(bulk_args->remote_reg_info->offset);
    free(bulk_args->remote_reg_info->size);
    free(bulk_args->remote_reg_info);
    
    HG_Bulk_free(bulk_args->remote_bulk_handle);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);
//    free(bulk_args);
    
    hg_thread_mutex_lock(&(bulk_args->work_mutex));
    bulk_args->work_completed = 1;
    hg_thread_cond_signal(&(bulk_args->work_cond));
    hg_thread_mutex_unlock(&(bulk_args->work_mutex));
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static HG_THREAD_RETURN_TYPE
pdc_region_read_from_progress(void *arg)
{
    HG_THREAD_RETURN_TYPE ret_value = (HG_THREAD_RETURN_TYPE) 0;
    struct buf_map_release_bulk_args *bulk_args = (struct buf_map_release_bulk_args *)arg;
    const struct hg_info *hg_info = NULL;
    hg_op_id_t hg_bulk_op_id;
    hg_return_t hg_ret;
    size_t size;
    int error = 0;
    
    FUNC_ENTER(NULL);
    
    hg_info = HG_Get_info(bulk_args->handle);
    
    PDC_Server_data_read_from(bulk_args->remote_obj_id, bulk_args->remote_reg_info, bulk_args->data_buf);
    
    /* Push bulk data */
    size = HG_Bulk_get_size(bulk_args->local_bulk_handle);
    if(size != HG_Bulk_get_size(bulk_args->remote_bulk_handle)) {
        error = 1;
        PGOTO_ERROR(ret_value, "===PDC SERVER: pdc_region_read_from_progress local and remote bulk size does not match\n");
    }
    
    hg_ret = HG_Bulk_transfer(hg_info->context, obj_map_region_release_bulk_transfer_thread_cb, bulk_args, HG_BULK_PUSH, bulk_args->local_addr, bulk_args->local_bulk_handle, 0, bulk_args->remote_bulk_handle, 0, size, &hg_bulk_op_id);
    if (hg_ret != HG_SUCCESS) {
        error = 1;
        printf("===PDC SERVER: pdc_region_read_from_progress push data failed\n");
    }
    
done:
    if(error == 1) {
        fflush(stdout);
        HG_Bulk_free(bulk_args->remote_bulk_handle);
        HG_Free_input(bulk_args->handle, &(bulk_args->in));
        HG_Destroy(bulk_args->handle);
        //    free(bulk_args);
    
        hg_thread_mutex_lock(&(bulk_args->work_mutex));
        bulk_args->work_completed = 1;
        hg_thread_cond_signal(&(bulk_args->work_cond));
        hg_thread_mutex_unlock(&(bulk_args->work_mutex));
    }
    
    FUNC_LEAVE(ret_value);
}

//enter this function, transfer is done, data is in data server
static hg_return_t
buf_map_region_release_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
    struct buf_map_release_bulk_args *bulk_args = NULL;
    struct PDC_region_info *remote_reg_info = NULL;
    int error = 0;
#ifdef ENABLE_MULTITHREAD
    data_server_region_t *target_reg = NULL;
    region_buf_map_t *elt;
#endif

    FUNC_ENTER(NULL);

    bulk_args = (struct buf_map_release_bulk_args *)hg_cb_info->arg;
    handle = bulk_args->handle;
    hg_info = HG_Get_info(handle);
   
    if (hg_cb_info->ret == HG_CANCELED) {
        error = 1;
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "HG_Bulk_transfer() was successfully canceled\n");
    } else if (hg_cb_info->ret != HG_SUCCESS) {
        error = 1;
        out.ret = 0;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in region_release_bulk_transfer_cb()");
    }
   
    out.ret = 1;
    HG_Respond(bulk_args->handle, NULL, NULL, &out);

#ifdef ENABLE_MULTITHREAD
    bulk_args->work.func = pdc_region_write_out_progress;
    bulk_args->work.args = bulk_args; 

    target_reg = PDC_Server_get_obj_region(bulk_args->remote_obj_id);
    DL_FOREACH(target_reg->region_buf_map_head, elt) {
        if((bulk_args->remote_region).start_0 == elt->remote_region_unit.start_0 &&
            (bulk_args->remote_region).count_0 == elt->remote_region_unit.count_0)
            elt->bulk_args = bulk_args;
    }

    hg_thread_pool_post(hg_test_thread_pool_fs_g, &(bulk_args->work));
#else
    remote_reg_info = (struct PDC_region_info *)malloc(sizeof(struct PDC_region_info));
    if(remote_reg_info == NULL) {
        PGOTO_ERROR(HG_OTHER_ERROR, "remote_reg_info memory allocation failed\n");
    }
    remote_reg_info->ndim = (bulk_args->remote_region).ndim;
    remote_reg_info->offset = (uint64_t *)malloc(sizeof(uint64_t));
    remote_reg_info->size = (uint64_t *)malloc(sizeof(uint64_t));
    (remote_reg_info->offset)[0] = (bulk_args->remote_region).start_0;
    (remote_reg_info->size)[0] = (bulk_args->remote_region).count_0;

    PDC_Server_data_write_out(bulk_args->remote_obj_id, remote_reg_info, bulk_args->data_buf);

    // Perform lock release function
    PDC_Data_Server_region_release(&(bulk_args->in), &out);

    PDC_Server_release_lock_request(bulk_args->remote_obj_id, remote_reg_info);
#endif

done:
    fflush(stdout);

#ifndef ENABLE_MULTITHREAD
    free(remote_reg_info->offset);
    free(remote_reg_info->size);
    free(remote_reg_info);

    HG_Bulk_free(bulk_args->remote_bulk_handle);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);
    free(bulk_args);
#endif

    FUNC_LEAVE(ret_value);
}

//enter this function, transfer is done, data is pushed to buffer
static hg_return_t
obj_map_region_release_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
    struct buf_map_release_bulk_args *bulk_args = NULL;
    int error = 0;
    
    FUNC_ENTER(NULL);
    
    bulk_args = (struct buf_map_release_bulk_args *)hg_cb_info->arg;
    handle = bulk_args->handle;
    
    hg_info = HG_Get_info(handle);
    
    if (hg_cb_info->ret == HG_CANCELED) {
        error = 1;
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "HG_Bulk_transfer() was successfully canceled\n");
    } else if (hg_cb_info->ret != HG_SUCCESS) {
        error = 1;
        out.ret = 0;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in region_release_bulk_transfer_cb()");
    }
    
    // release the lock
    PDC_Data_Server_region_release(&(bulk_args->in), &out);
    HG_Respond(bulk_args->handle, NULL, NULL, &out);
    
done:
    free(bulk_args->remote_reg_info);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);
    free(bulk_args);
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

//enter this function, transfer is done, data is pushed to mapping region
static hg_return_t
region_update_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    region_lock_out_t out;
    struct region_update_bulk_args *bulk_args = NULL;

    bulk_args = (struct region_update_bulk_args *)hg_cb_info->arg;
//    handle = bulk_args->handle;

    // Send notification to mapped regions, when data transfer is done
    PDC_SERVER_notify_region_update_to_client(bulk_args->remote_obj_id, 
                                              bulk_args->remote_reg_id, bulk_args->remote_client_id);

    out.ret = 1;
    HG_Respond(bulk_args->handle, NULL, NULL, &out);

//    if(atomic_fetch_sub(&(bulk_args->refcount), 1) == 1) {
    if(hg_atomic_decr32(&(bulk_args->refcount)) == 1) {
        HG_Bulk_free(bulk_args->bulk_handle);
        free(bulk_args->args->data_buf);
        free(bulk_args->args);
        HG_Destroy(bulk_args->handle);
        free(bulk_args);
    }
    
    FUNC_LEAVE(hg_ret);
}

//enter this function, transfer is done, data is ready in data server
static hg_return_t
region_release_bulk_transfer_cb (const struct hg_cb_info *hg_cb_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    perr_t ret_value; 
    region_lock_out_t out;
    hg_handle_t handle;
    const struct hg_info *hg_info = NULL;
    struct lock_bulk_args *bulk_args = NULL;
    struct region_update_bulk_args *update_bulk_args = NULL;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    PDC_mapping_info_t *mapped_region = NULL;
    hg_op_id_t hg_bulk_op_id;
    size_t size, remote_size;
    int lock_status;
    int region_locked;
    int all_reg_locked;
/*
void      **from_data_ptrs;
void       *data_ptrs;
hg_size_t  *data_size;
hg_uint32_t count;
*/
    FUNC_ENTER(NULL);
    
    bulk_args = (struct lock_bulk_args *)hg_cb_info->arg;
    local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    handle = bulk_args->handle;
    hg_info = HG_Get_info(handle);
   
    if (hg_cb_info->ret == HG_CANCELED) {
        printf("HG_Bulk_transfer() was successfully canceled\n");
        out.ret = 0;
    } else if (hg_cb_info->ret != HG_SUCCESS) {
        printf("Error in region_release_bulk_transfer_cb()");
        hg_ret = HG_PROTOCOL_ERROR;
        out.ret = 0;
    }

    // Perform lock release function
    PDC_Data_Server_region_release(&(bulk_args->in), &out);
//    HG_Respond(bulk_args->handle, NULL, NULL, &out);
/*
    data_buf = (void *)malloc(size);
    server_region->ndim = 1;
    server_region->size = (uint64_t *)malloc(sizeof(uint64_t));
    server_region->offset = (uint64_t *)malloc(sizeof(uint64_t));
    (server_region->size)[0] = size;
    (server_region->offset)[0] = 0;  
    ret_value = PDC_Server_data_read_direct((bulk_args->in).obj_id, server_region, data_buf);
    if(ret_value != SUCCEED)
        printf("==PDC SERVER: PDC_Server_data_write_direct() failed\n");
printf("first data is %d\n", *(int *)data_buf);
printf("next is %d\n", *(int *)(data_buf+4));
printf("next is %d\n", *(int *)(data_buf+8));
printf("next is %d\n", *(int *)(data_buf+12));
printf("next is %d\n", *(int *)(data_buf+16));
printf("next is %d\n", *(int *)(data_buf+20));
fflush(stdout);
*/
 
    update_bulk_args = (struct region_update_bulk_args *) malloc(sizeof(struct region_update_bulk_args));
//    update_bulk_args->refcount = ATOMIC_VAR_INIT(0);
    hg_atomic_init32(&(update_bulk_args->refcount), 0);
    update_bulk_args->handle = handle;
    update_bulk_args->bulk_handle = local_bulk_handle;
    update_bulk_args->args = bulk_args;

    region_locked = 0;
    all_reg_locked = 1;
    size = HG_Bulk_get_size(local_bulk_handle);

    PDC_LIST_GET_FIRST(mapped_region, &(bulk_args->mapping_list)->ids);
    while(mapped_region != NULL) {
        // check status before tranfer if it is write/read lock
        // if region is locked, mark region as dirty
        // PDC_Server_region_lock_status(mapped_region->remote_obj_id, &(mapped_region->remote_region), &lock_status);
        PDC_Server_region_lock_status(mapped_region, &lock_status);
        if(lock_status == 0) {
            all_reg_locked = 0;
            // printf("region is not locked\n");
            remote_size = HG_Bulk_get_size(mapped_region->remote_bulk_handle);
            // printf("remote_size = %lld\n", remote_size);
            if(size != remote_size) {
                // TODO: skip to done
                printf("Transfer size does not match. Cannot start HG_Bulk_transfer()\n");
            }
            else {
                update_bulk_args->remote_obj_id = mapped_region->remote_obj_id;
                update_bulk_args->remote_reg_id = mapped_region->remote_reg_id;
                update_bulk_args->remote_client_id = mapped_region->remote_client_id;
/*
count = HG_Bulk_get_segment_count(mapped_region->remote_bulk_handle);
from_data_ptrs = (void **)malloc( count * sizeof(void *) );
data_size = (hg_size_t *)malloc( count * sizeof(hg_size_t) );
HG_Bulk_access(mapped_region->remote_bulk_handle, 0, size, HG_BULK_READWRITE, count, from_data_ptrs, data_size, NULL); 
printf("each size is %lld, %lld, %lld\n", data_size[0], data_size[1], data_size[2]);
printf("each addr is %lld, %lld, %lld\n", from_data_ptrs[0], from_data_ptrs[1], from_data_ptrs[2]);
count = HG_Bulk_get_segment_count(local_bulk_handle);
HG_Bulk_access(local_bulk_handle, 0, size, HG_BULK_READWRITE, count, from_data_ptrs, data_size, NULL);
printf("cout = %d\n", count);
printf("each size is %lld\n", data_size[0]);
printf("each addr is %lld\n",from_data_ptrs[0]);
printf("match addr %lld\n", bulk_args->data_buf);
fflush(stdout);
*/
                //increase ref
//                atomic_fetch_add(&(update_bulk_args->refcount), 1);
                hg_atomic_incr32(&(update_bulk_args->refcount));
                hg_ret = HG_Bulk_transfer(hg_info->context, region_update_bulk_transfer_cb, update_bulk_args, HG_BULK_PUSH, bulk_args->addr, mapped_region->remote_bulk_handle, 0, local_bulk_handle, 0, size, &hg_bulk_op_id);
                if (hg_ret != HG_SUCCESS) {
                    printf("==PDC SERVER ERROR: region_release_bulk_transfer_cb() could not write bulk data\n");
                }
            }
        }
        else { 
            region_locked = 1;
            // printf("region is locked\n");
        }
        PDC_LIST_TO_NEXT(mapped_region, entry);
    }

    if(region_locked == 1) {
        // Write to file system
        ret_value = PDC_Server_data_write_direct((bulk_args->in).obj_id, bulk_args->server_region, bulk_args->data_buf);
        if(ret_value != SUCCEED)
            printf("==PDC SERVER: PDC_Server_data_write_direct() failed\n");
    }
    // Tang: commented the following lines as PDC_Server_data_write_direct is executed as callback function
    //       after getting metadata from remote server in the next round of server_loop
    /* free(bulk_args->server_region->size); */
    /* free(bulk_args->server_region->offset); */
    /* free(bulk_args->server_region); */

/* //done: */
/* //    out.ret = 1; */
/* //    HG_Respond(bulk_args->handle, NULL, NULL, &out); */
    /* /1* printf("==PDC_SERVER: region_release_bulk_transfer_cb(): returned %" PRIu64 "\n", out.ret); *1/ */

    /* HG_Free_input(bulk_args->handle, &(bulk_args->in)); */
    
    if(all_reg_locked == 1) {
        HG_Respond(bulk_args->handle, NULL, NULL, &out);
        HG_Destroy(bulk_args->handle);
        HG_Bulk_free(local_bulk_handle);
        free(bulk_args->data_buf);
        free(bulk_args);
    }
//    HG_Bulk_free(local_bulk_handle);

//    HG_Destroy(bulk_args->handle);
//    free(bulk_args->data_buf);
//    free(bulk_args);

    FUNC_LEAVE(hg_ret);
}

static hg_return_t
region_release_update_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    region_lock_out_t out;
    hg_handle_t handle;
    struct region_lock_update_bulk_args *bulk_args = NULL;

    bulk_args = (struct region_lock_update_bulk_args *)hg_cb_info->arg;
    handle = bulk_args->handle;

    // Perform lock releae function
    PDC_Data_Server_region_release(&(bulk_args->in), &out);
    HG_Respond(handle, NULL, NULL, &out);

    // Send notification to mapped regions, when data transfer is done
    PDC_SERVER_notify_region_update_to_client(bulk_args->remote_obj_id, bulk_args->remote_reg_id, bulk_args->remote_client_id);

    free(bulk_args->server_region->size);
    free(bulk_args->server_region->offset);
    free(bulk_args->server_region);
    free(bulk_args->data_buf);

//    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &(bulk_args->in));
    HG_Destroy(handle); 
    free(bulk_args);

    FUNC_LEAVE(hg_ret);
}

HG_TEST_RPC_CB(region_release, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    region_lock_in_t in;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
    data_server_region_t *target_obj;
    int error = 0;
    int found = 0;
    int dirty_reg = 0;
    hg_size_t   size;
    hg_op_id_t hg_bulk_op_id;
    hg_return_t hg_ret;
    void       *data_buf;
    struct PDC_region_info *server_region;
    region_list_t *elt, *request_region, *tmp;
    struct region_lock_update_bulk_args *lock_update_bulk_args = NULL;
    struct buf_map_release_bulk_args *buf_map_bulk_args = NULL, *obj_map_bulk_args = NULL;
    hg_bulk_t lock_local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t remote_bulk_handle = HG_BULK_NULL;
    struct PDC_region_info *remote_reg_info;
    region_buf_map_t *eltt, *eltt2;
    hg_uint32_t remote_count;
    void **data_ptrs_to = NULL;
    size_t *data_size_to = NULL;
#ifdef ENABLE_MULTITHREAD
    data_server_region_t *target_reg = NULL;
#endif
    
    FUNC_ENTER(NULL);
    
    // Decode input
    HG_Get_input(handle, &in);
    /* Get info from handle */
    hg_info = HG_Get_info(handle);
    
    //  printf("release %lld\n", in.obj_id);
    if(in.access_type==READ) {
        // check region is dirty or not, if dirty transfer data
        request_region = (region_list_t *)malloc(sizeof(region_list_t));
        pdc_region_transfer_t_to_list_t(&in.region, request_region);
        target_obj = PDC_Server_get_obj_region(in.obj_id);
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
        DL_FOREACH(target_obj->region_lock_head, elt) {
#ifdef ENABLE_MULTITHREAD
            hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif
            if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty_from_buf == 1 && hg_atomic_get32(&(elt->buf_map_refcount)) == 0) {
                /* printf("==PDC SERVER: region_release start %" PRIu64 " \n", request_region->start[0]); */
                dirty_reg = 1;
                size = HG_Bulk_get_size(elt->bulk_handle);
                data_buf = (void *)calloc(1,size);
                // data transfer
                server_region = (struct PDC_region_info *)malloc(sizeof(struct PDC_region_info));
                server_region->ndim = 1;
                server_region->size = (uint64_t *)malloc(sizeof(uint64_t));
                server_region->offset = (uint64_t *)malloc(sizeof(uint64_t));
                (server_region->size)[0] = size;
                (server_region->offset)[0] = in.region.start_0;
                ret_value = PDC_Server_data_read_direct(elt->from_obj_id, server_region, data_buf);
                if(ret_value != SUCCEED)
                    printf("==PDC SERVER: PDC_Server_data_read_direct() failed\n");
                hg_ret = HG_Bulk_create(hg_info->hg_class, 1, &data_buf, &size, HG_BULK_READWRITE, &lock_local_bulk_handle);
                if (hg_ret != HG_SUCCESS) {
                    printf("==PDC SERVER ERROR: Could not create bulk data handle\n");
                }
                lock_update_bulk_args = (struct region_lock_update_bulk_args*) malloc(sizeof(struct region_lock_update_bulk_args));
                lock_update_bulk_args->handle = handle;
                lock_update_bulk_args->in = in;
                lock_update_bulk_args->remote_obj_id = elt->obj_id;
                lock_update_bulk_args->remote_reg_id= elt->reg_id;
                lock_update_bulk_args->remote_client_id = elt->client_id;
                lock_update_bulk_args->data_buf = data_buf;
                lock_update_bulk_args->server_region = server_region;
                
                hg_ret = HG_Bulk_transfer(hg_info->context, region_release_update_bulk_transfer_cb, lock_update_bulk_args, HG_BULK_PUSH, elt->addr, elt->bulk_handle, 0, lock_local_bulk_handle, 0, size, &hg_bulk_op_id);
                if (hg_ret != HG_SUCCESS) {
                    printf("==PDC SERVER ERROR: region_release_bulk_transfer_cb() could not write bulk data\n");
                }
                
            }
            
            if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty_from_buf == 1 && hg_atomic_get32(&(elt->buf_map_refcount)) > 0) {
                dirty_reg = 1;
                tmp = (region_list_t *)malloc(sizeof(region_list_t));
                DL_FOREACH(target_obj->region_buf_map_head, eltt2) {
                    pdc_region_transfer_t_to_list_t(&(eltt2->remote_region_unit), tmp);
                    if(PDC_is_same_region_list(tmp, request_region) == 1) {
                        // get remote object memory addr
                        data_buf = PDC_Server_get_region_buf_ptr(in.obj_id, in.region);
                        if(in.region.ndim == 1) {
                            remote_count = 1;
                            data_ptrs_to = (void **)malloc( sizeof(void *) );
                            data_size_to = (size_t *)malloc( sizeof(size_t) );
                            *data_ptrs_to = data_buf;
                            *data_size_to = (eltt2->remote_region_unit).count_0;
                        }
                        
                        hg_ret = HG_Bulk_create(hg_info->hg_class, remote_count, data_ptrs_to, (hg_size_t *)data_size_to, HG_BULK_READWRITE, &remote_bulk_handle);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            PGOTO_ERROR(HG_OTHER_ERROR, "==PDC SERVER: obj map Could not create bulk data handle\n");
                        }
                        free(data_ptrs_to);
                        free(data_size_to);
                        
                        remote_reg_info = (struct PDC_region_info *)malloc(sizeof(struct PDC_region_info));
                        if(remote_reg_info == NULL) {
                            error = 1;
                            PGOTO_ERROR(HG_OTHER_ERROR, "==PDC SERVER:HG_TEST_RPC_CB(region_release, handle) remote_reg_info memory allocation failed\n");
                        }
                        
                        obj_map_bulk_args = (struct buf_map_release_bulk_args *) malloc(sizeof(struct buf_map_release_bulk_args));
                        memset(obj_map_bulk_args, 0, sizeof(struct buf_map_release_bulk_args));
                        obj_map_bulk_args->handle = handle;
                        obj_map_bulk_args->data_buf = data_buf;
                        obj_map_bulk_args->in = in;
                        obj_map_bulk_args->remote_obj_id = eltt2->remote_obj_id;
                        obj_map_bulk_args->remote_reg_id = eltt2->remote_reg_id;
                        obj_map_bulk_args->remote_reg_info = remote_reg_info;
                        obj_map_bulk_args->remote_region = eltt2->remote_region_unit;
                        obj_map_bulk_args->remote_client_id = eltt2->remote_client_id;
                        obj_map_bulk_args->remote_bulk_handle = remote_bulk_handle;
                        obj_map_bulk_args->local_bulk_handle = eltt2->local_bulk_handle;
                        obj_map_bulk_args->local_addr = eltt2->local_addr;
                        
                        remote_reg_info->ndim = (obj_map_bulk_args->remote_region).ndim;
                        remote_reg_info->offset = (uint64_t *)malloc(sizeof(uint64_t));
                        remote_reg_info->size = (uint64_t *)malloc(sizeof(uint64_t));
                        (remote_reg_info->offset)[0] = (obj_map_bulk_args->remote_region).start_0;
                        (remote_reg_info->size)[0] = (obj_map_bulk_args->remote_region).count_0;
#ifdef ENABLE_MULTITHREAD
                        hg_thread_mutex_init(&(obj_map_bulk_args->work_mutex));
                        hg_thread_cond_init(&(obj_map_bulk_args->work_cond));
                        obj_map_bulk_args->work.func = pdc_region_read_from_progress;
                        obj_map_bulk_args->work.args = obj_map_bulk_args;
                        
                        eltt2->bulk_args = obj_map_bulk_args;
                        
                        hg_thread_pool_post(hg_test_thread_pool_fs_g, &(obj_map_bulk_args->work));
                        
                        out.ret = 1;
                        HG_Respond(handle, NULL, NULL, &out);
#else
                        PDC_Server_data_read_from(obj_map_bulk_args->remote_obj_id, remote_reg_info, data_buf);
                        
                        size = HG_Bulk_get_size(eltt2->local_bulk_handle);
                        if(size != HG_Bulk_get_size(remote_bulk_handle)) {
                            error = 1;
                            printf("===PDC SERVER: HG_TEST_RPC_CB(region_release, handle) local and remote bulk size does not match\n");
                        }
                        
                        hg_ret = HG_Bulk_transfer(hg_info->context, obj_map_region_release_bulk_transfer_cb, obj_map_bulk_args, HG_BULK_PUSH, eltt2->local_addr, eltt2->local_bulk_handle, 0, remote_bulk_handle, 0, size, &hg_bulk_op_id);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            printf("===PDC SERVER: HG_TEST_RPC_CB(region_release, handle) obj map Could not write bulk data\n");
                        }
#endif
                    }
                }
                free(tmp);
            }
#ifdef ENABLE_MULTITHREAD
            hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
        }
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif
        free(request_region);
        
        if(dirty_reg == 0) {
            // Perform lock release function
            PDC_Data_Server_region_release(&in, &out);
            HG_Respond(handle, NULL, NULL, &out);
            HG_Free_input(handle, &in);
            HG_Destroy(handle);
        }
    }
    // write lock release with mapping case
    // do data tranfer if it is write lock release with mapping.
    else {
        request_region = (region_list_t *)malloc(sizeof(region_list_t));
        pdc_region_transfer_t_to_list_t(&in.region, request_region);
        target_obj = PDC_Server_get_obj_region(in.obj_id);
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
        DL_FOREACH(target_obj->region_lock_head, elt) {
#ifdef ENABLE_MULTITHREAD
            hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif
            if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty_from_buf == 1 && hg_atomic_get32(&(elt->buf_map_refcount)) > 0) {
                dirty_reg = 1;
                tmp = (region_list_t *)malloc(sizeof(region_list_t));
                DL_FOREACH(target_obj->region_buf_map_head, eltt) {
                    pdc_region_transfer_t_to_list_t(&(eltt->remote_region_unit), tmp);
                    if(PDC_is_same_region_list(tmp, request_region) == 1) {
                        // get remote object memory addr
                        data_buf = PDC_Server_get_region_buf_ptr(in.obj_id, in.region);
                        if(in.region.ndim == 1) {
                            remote_count = 1;
                            data_ptrs_to = (void **)malloc( sizeof(void *) );
                            data_size_to = (size_t *)malloc( sizeof(size_t) );
                            *data_ptrs_to = data_buf;
                            *data_size_to = (eltt->remote_region_unit).count_0;
                        }
                        /*                            else if(in.region.ndim == 2) {
                         remote_count = (eltt->remote_region_nounit).count_0;
                         data_ptrs_to = (void **)malloc( remote_count * sizeof(void *) );
                         data_size_to = (size_t *)malloc( remote_count * sizeof(size_t) );
                         data_ptrs_to[0] = data_buf + eltt->remote_unit*(res_meta->dims[1]*(eltt->remote_region_nounit).start_0 + (eltt->remote_region_nounit).start_1);
                         data_size_to[0] = (eltt->remote_region_unit).count_1;
                         for(i=1; i<remote_count; i++) {
                         data_ptrs_to[i] = data_ptrs_to[i-1] + eltt->remote_unit*res_meta->dims[1];
                         data_size_to[i] = data_size_to[0];
                         }
                         }
                         else if(in.region.ndim == 3) {
                         remote_count = (eltt->remote_region_nounit).count_0 * (eltt->remote_region_nounit).count_1;
                         data_ptrs_to = (void **)malloc( remote_count * sizeof(void *) );
                         data_size_to = (size_t *)malloc( remote_count * sizeof(size_t) );
                         data_ptrs_to[0] = data_buf + eltt->remote_unit*(res_meta->dims[2]*res_meta->dims[1]*(eltt->remote_region_nounit).start_0 + res_meta->dims[2]*(eltt->remote_region_nounit).start_1 + (eltt->remote_region_nounit).start_2);
                         data_size_to[0] = (eltt->remote_region_unit).count_2;
                         for(i=0; i<(eltt->remote_region_nounit).count_0-1; i++) {
                         for(j=0; j<(eltt->remote_region_nounit).count_1-1; j++) {
                         data_ptrs_to[i*(eltt->remote_region_nounit).count_1+j+1] = data_ptrs_to[i*(eltt->remote_region_nounit).count_1+j] + eltt->remote_unit*res_meta->dims[2];
                         data_size_to[i*(eltt->remote_region_nounit).count_1+j+1] = data_size_to[0];
                         }
                         data_ptrs_to[i*(eltt->remote_region_nounit).count_1+(eltt->remote_region_nounit).count_1] = data_ptrs_to[i*(eltt->remote_region_nounit).count_1] + eltt->remote_unit*res_meta->dims[2]*res_meta->dims[1];
                         data_size_to[i*(eltt->remote_region_nounit).count_1+(eltt->remote_region_nounit).count_1] = data_size_to[0];
                         }
                         i = (eltt->remote_region_nounit).count_0 - 1;
                         for(j=0; j<(eltt->remote_region_nounit).count_1-1; j++) {
                         data_ptrs_to[i*(eltt->remote_region_nounit).count_1+j+1] = data_ptrs_to[i*(eltt->remote_region_nounit).count_1+j] + eltt->remote_unit*res_meta->dims[2];
                         data_size_to[i*(eltt->remote_region_nounit).count_1+j+1] = data_size_to[0];
                         }
                         }*/
                        /* Create a new block handle to read the data */
                        hg_ret = HG_Bulk_create(hg_info->hg_class, remote_count, data_ptrs_to, (hg_size_t *)data_size_to, HG_BULK_READWRITE, &remote_bulk_handle);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            PGOTO_ERROR(FAIL, "==PDC SERVER ERROR: Could not create bulk data handle\n");
                        }
                        free(data_ptrs_to);
                        free(data_size_to);
                        
                        buf_map_bulk_args = (struct buf_map_release_bulk_args *) malloc(sizeof(struct buf_map_release_bulk_args));
                        if(buf_map_bulk_args == NULL) {
                            printf("HG_TEST_RPC_CB(region_release, handle): buf_map_bulk_args memory allocation failed\n");
                            goto done;
                        }
                        memset(buf_map_bulk_args, 0, sizeof(struct buf_map_release_bulk_args));
                        buf_map_bulk_args->handle = handle;
                        buf_map_bulk_args->data_buf = data_buf;
                        buf_map_bulk_args->in = in;
                        buf_map_bulk_args->remote_obj_id = eltt->remote_obj_id;
                        buf_map_bulk_args->remote_reg_id = eltt->remote_reg_id;
                        buf_map_bulk_args->remote_region = eltt->remote_region_unit;
                        buf_map_bulk_args->remote_client_id = eltt->remote_client_id;
                        buf_map_bulk_args->remote_bulk_handle = remote_bulk_handle;
#ifdef ENABLE_MULTITHREAD
                        hg_thread_mutex_init(&(buf_map_bulk_args->work_mutex));
                        hg_thread_cond_init(&(buf_map_bulk_args->work_cond));
#endif
                        /* Pull bulk data */
                        size = HG_Bulk_get_size(eltt->local_bulk_handle);
                        if(size != HG_Bulk_get_size(remote_bulk_handle)) {
                            error = 1;
                            printf("===PDC SERVER: HG_TEST_RPC_CB(region_release, handle) local and remote bulk size does not match\n");
                        }
                        hg_ret = HG_Bulk_transfer(hg_info->context, buf_map_region_release_bulk_transfer_cb, buf_map_bulk_args, HG_BULK_PULL, eltt->local_addr, eltt->local_bulk_handle, 0, remote_bulk_handle, 0, size, &hg_bulk_op_id);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            printf("===PDC SERVER: HG_TEST_RPC_CB(region_release, handle) buf map Could not read bulk data\n");
                        }
                    }
                }
                free(tmp);
            }
#ifdef ENABLE_MULTITHREAD
            hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
        }
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif
        free(request_region);
        
        if(dirty_reg == 0) {
            // Perform lock release function
            PDC_Data_Server_region_release(&in, &out);
            HG_Respond(handle, NULL, NULL, &out);
            HG_Free_input(handle, &in);
            HG_Destroy(handle);
        }
    }
    
done:
    if(error == 1) {
        out.ret = 0;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }
    
    FUNC_LEAVE(ret_value);
}

HG_TEST_RPC_CB(region_lock, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    perr_t ret = SUCCEED;
    region_lock_in_t in;
    region_lock_out_t out;

    FUNC_ENTER(NULL);

    HG_Get_input(handle, &in);

    // Perform lock function
    ret = PDC_Data_Server_region_lock(&in, &out, &handle);
//    PDC_Data_Server_region_lock(&in, &out);

    HG_Free_input(handle, &in);
    if(ret == SUCCEED) {
        HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
    }

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// region_unmap_cb(hg_handle_t handle)
HG_TEST_RPC_CB(region_unmap, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    reg_unmap_in_t in;
    reg_unmap_out_t out;
    pdc_metadata_t *target_obj;
    region_map_t *elt, *tmp;
    PDC_mapping_info_t *tmp_ptr;
    const struct hg_info *info;
 
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    info = HG_Get_info(handle);
    out.ret = 0;

    target_obj = PDC_Server_get_obj_metadata(in.local_obj_id);
    if (target_obj == NULL) {
        printf("==PDC_SERVER: PDC_Server_object_unmap - requested object (id=%" PRIu64 ") does not exist\n", in.local_obj_id);
        goto done;
    }

    DL_FOREACH_SAFE(target_obj->region_map_head, elt, tmp) {
//        if(in.local_obj_id==elt->local_obj_id && in.local_reg_id==elt->local_reg_id) {
        if(in.local_obj_id==elt->local_obj_id && region_is_identical(in.local_region, elt->local_region)) {
            PDC_LIST_GET_FIRST(tmp_ptr, &elt->ids);
            while(tmp_ptr != NULL) {
                HG_Bulk_free(tmp_ptr->remote_bulk_handle);
                HG_Addr_free(info->hg_class, tmp_ptr->remote_addr);
                PDC_LIST_REMOVE(tmp_ptr, entry);
                free(tmp_ptr);
                PDC_LIST_GET_FIRST(tmp_ptr, &elt->ids);
            }
            HG_Bulk_free(elt->local_bulk_handle);
            HG_Addr_free(info->hg_class, elt->local_addr);
            DL_DELETE(target_obj->region_map_head, elt);
            free(elt);
            out.ret = 1;
        }
    }
    
done:
    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// buf_unmap_cb(hg_handle_t handle)
HG_TEST_RPC_CB(buf_unmap, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    perr_t ret;
    buf_unmap_in_t in;
    buf_unmap_out_t out;
    const struct hg_info *info;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    info = HG_Get_info(handle);
    out.ret = 0;

    ret = PDC_Data_Server_buf_unmap(info, &in);
    if(ret != SUCCEED) {
        out.ret = 0;
        printf("===PDC_DATA_SERVER: HG_TEST_RPC_CB(buf_unmap, handle) - PDC_Data_Server_buf_unmap() failed"); 
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }

    ret = PDC_Meta_Server_buf_unmap(&in, &handle);
    if(ret != SUCCEED) {
        printf("===PDC_DATA_SERVER: HG_TEST_RPC_CB(buf_unmap, handle) - PDC_Meta_Server_buf_unmap() failed");
    }

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// get_remote_metadata_cb(hg_handle_t handle)
HG_TEST_RPC_CB(get_remote_metadata, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    get_remote_metadata_in_t in;
    get_remote_metadata_out_t out;
    pdc_metadata_t *meta;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);

    meta = PDC_Server_get_obj_metadata(in.obj_id);
    if(meta != NULL)
        pdc_metadata_t_to_transfer_t(meta, &out.ret);
    else {
        out.ret.user_id        = -1;
        out.ret.obj_id         = 0;
        out.ret.time_step      = -1;
        out.ret.obj_name       = "N/A";
        out.ret.app_name       = "N/A";
        out.ret.tags           = "N/A";
        out.ret.data_location  = "N/A"; 
    }

    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// buf_unmap_server_cb(hg_handle_t handle)
HG_TEST_RPC_CB(buf_unmap_server, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    buf_unmap_in_t in;
    buf_unmap_out_t out;
    pdc_metadata_t *target_obj;
    const struct hg_info *info;
    region_buf_map_t *tmp, *elt;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    info = HG_Get_info(handle);

    target_obj = PDC_Server_get_obj_metadata(in.remote_obj_id);
    if (target_obj == NULL) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_SERVER: HG_TEST_RPC_CB(buf_unmap_server, handle) - requested object does not exist\n");
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
    DL_FOREACH_SAFE(target_obj->region_buf_map_head, elt, tmp) {
        if(in.remote_obj_id==elt->remote_obj_id && region_is_identical(in.remote_region, elt->remote_region_unit)) {
//            HG_Bulk_free(elt->local_bulk_handle);
//            HG_Addr_free(info->hg_class, elt->local_addr);
            DL_DELETE(target_obj->region_buf_map_head, elt);
            free(elt);
            out.ret = 1;
        }
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&meta_buf_map_mutex_g);
#endif

done:
    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}
/* static hg_return_t */
// buf_map_server_cb(hg_handle_t handle)
HG_TEST_RPC_CB(buf_map_server, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    buf_map_in_t in;
    buf_map_out_t out;
    pdc_metadata_t *target_obj;
    const struct hg_info *info;
    region_list_t *elt, *request_region;
    region_buf_map_t *buf_map_ptr;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);

    target_obj = PDC_Server_get_obj_metadata(in.remote_obj_id);
    if (target_obj == NULL) {
        printf("==PDC_SERVER: HG_TEST_RPC_CB(buf_map_server, handle) - requested object (id=%" PRIu64 ") does not exist\n", in.remote_obj_id);
        out.ret = 0;
        goto done;
    }
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    pdc_region_transfer_t_to_list_t(&in.remote_region_unit, request_region);
    DL_FOREACH(target_obj->region_lock_head, elt) {
        if (PDC_is_same_region_list(elt, request_region) == 1) {
            hg_atomic_incr32(&(elt->buf_map_refcount));
//            printf("mapped region is locked \n");
//            fflush(stdout); 
        }
    }
    buf_map_ptr = (region_buf_map_t *)malloc(sizeof(region_buf_map_t));
    buf_map_ptr->local_reg_id = in.local_reg_id;
    buf_map_ptr->local_region = in.local_region;
    buf_map_ptr->local_ndim = in.ndim;
    buf_map_ptr->local_data_type = in.local_type;
//    info = HG_Get_info(handle);
//    HG_Addr_dup(info->hg_class, info->addr, &(buf_map_ptr->local_addr));
//    HG_Bulk_ref_incr(in.local_bulk_handle);
//    buf_map_ptr->local_bulk_handle = in.local_bulk_handle;

    buf_map_ptr->remote_obj_id = in.remote_obj_id;
    buf_map_ptr->remote_reg_id = in.remote_reg_id;
    buf_map_ptr->remote_client_id = in.remote_client_id;
    buf_map_ptr->remote_ndim = in.ndim;
    buf_map_ptr->remote_unit = in.remote_unit;
    buf_map_ptr->remote_region_unit = in.remote_region_unit;
    buf_map_ptr->remote_region_nounit = in.remote_region_nounit;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
    DL_APPEND(target_obj->region_buf_map_head, buf_map_ptr);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&meta_buf_map_mutex_g);
#endif
    
    out.ret = 1;
    free(request_region);
 
done:
    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// buf_map_cb(hg_handle_t handle)
HG_TEST_RPC_CB(buf_map, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    perr_t ret;
    buf_map_in_t in;
    buf_map_out_t out;
    const struct hg_info *info;
    region_list_t *request_region;
    region_buf_map_t *new_buf_map_ptr = NULL;
    void *data_ptr;
    size_t ndim;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);

    // Use region dimension to allocate memory, rather than object dimension (different from client side)
    ndim = in.remote_region_unit.ndim;
    //allocate memory for the object by region size
    if(ndim == 1)
        data_ptr = (void *)malloc(in.remote_region_nounit.count_0 * in.remote_unit);
    else if(ndim == 2)
        data_ptr = (void *)malloc(in.remote_region_nounit.count_0 * in.remote_region_nounit.count_1 * in.remote_unit);
    else if(ndim == 3)
        data_ptr = (void *)malloc(in.remote_region_nounit.count_0 * in.remote_region_nounit.count_1 * in.remote_region_nounit.count_2 * in.remote_unit);
    else if(ndim == 4)
        data_ptr = (void *)malloc(in.remote_region_nounit.count_0 * in.remote_region_nounit.count_1 * in.remote_region_nounit.count_2 * in.remote_region_nounit.count_3 * in.remote_unit);
    else {
        out.ret = 0; 
        PGOTO_ERROR(HG_OTHER_ERROR, "===PDC Data Server: object dim is not supported");
    }
    if(data_ptr == NULL) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "===PDC Data Server: object memory allocation failed");
    }
    
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    pdc_region_transfer_t_to_list_t(&in.remote_region_unit, request_region);

    info = HG_Get_info(handle);
    new_buf_map_ptr = PDC_Data_Server_buf_map(info, &in, request_region, data_ptr);
 
    if(new_buf_map_ptr == NULL) {
        out.ret = 0;
        printf("===PDC Data Server: insert region to local data server failed");
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }
    else {
        ret = PDC_Meta_Server_buf_map(&in, new_buf_map_ptr, &handle);
        if(ret != SUCCEED)
            printf("===PDC Data Server: PDC_Meta_Server_buf_map() failed");
    }

done:
//    HG_Respond(handle, NULL, NULL, &out);
//    HG_Free_input(handle, &in);
//    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// region_map_cb(hg_handle_t handle)
HG_TEST_RPC_CB(region_map, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    reg_map_in_t in;
    reg_map_out_t out;
    pdc_metadata_t *target_obj;
    int found;
    region_map_t *elt;
    const struct hg_info *info;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);

    target_obj = PDC_Server_get_obj_metadata(in.local_obj_id);
    if (target_obj == NULL) {
        printf("==PDC_SERVER: PDC_Server_region_map - requested object (id=%" PRIu64 ") does not exist\n", in.local_obj_id);
        out.ret = 0;
        goto done;
    }
    
    found = 0;
    DL_FOREACH(target_obj->region_map_head, elt) {
//        if(in.local_obj_id==elt->local_obj_id && in.local_reg_id==elt->local_reg_id) {
        if(in.local_obj_id==elt->local_obj_id && region_is_identical(in.local_region, elt->local_region)) {
            found = 1;
            region_map_t *map_ptr = target_obj->region_map_head;
            PDC_mapping_info_t *tmp_ptr;
            PDC_LIST_GET_FIRST(tmp_ptr, &map_ptr->ids);         
//            while(tmp_ptr!=NULL && (tmp_ptr->remote_reg_id!=in.remote_reg_id || tmp_ptr->remote_obj_id!=in.remote_obj_id)) {
            while(tmp_ptr!=NULL && (tmp_ptr->remote_obj_id!=in.remote_obj_id || !region_is_identical(tmp_ptr->remote_region, in.remote_region))) {
                PDC_LIST_TO_NEXT(tmp_ptr, entry);
            }
            if(tmp_ptr!=NULL) {
                printf("==PDC SERVER ERROR: mapping from obj %" PRIu64 " (region %" PRIu64 ") to obj %" PRIu64 " (reg %" PRIu64 ") already exists\n", in.local_obj_id, in.local_reg_id, in.remote_obj_id, in.remote_reg_id);
                out.ret = 0;
                goto done;
            }
            else {
//                printf("add mapped region to current map list\n");
                PDC_mapping_info_t *m_info_ptr = (PDC_mapping_info_t *)malloc(sizeof(PDC_mapping_info_t));
                m_info_ptr->remote_obj_id = in.remote_obj_id;
                m_info_ptr->remote_reg_id = in.remote_reg_id;
                m_info_ptr->remote_client_id = in.remote_client_id;
                m_info_ptr->remote_ndim = in.ndim;
                m_info_ptr->remote_region = in.remote_region;
                m_info_ptr->remote_bulk_handle = in.remote_bulk_handle;
                m_info_ptr->remote_addr = map_ptr->local_addr;
                m_info_ptr->from_obj_id = map_ptr->local_obj_id;
                HG_Bulk_ref_incr(in.remote_bulk_handle);
                PDC_LIST_INSERT_HEAD(&map_ptr->ids, m_info_ptr, entry);
//                atomic_fetch_add(&(map_ptr->mapping_count), 1);
                hg_atomic_incr32(&(map_ptr->mapping_count));
                out.ret = 1;
            }
        }
    }
    if(found == 0) {
        region_map_t *map_ptr = (region_map_t *)malloc(sizeof(region_map_t));
        PDC_LIST_INIT(&map_ptr->ids);
//        map_ptr->mapping_count = ATOMIC_VAR_INIT(1);
        hg_atomic_init32(&(map_ptr->mapping_count), 1);
        map_ptr->local_obj_id = in.local_obj_id;
        map_ptr->local_reg_id = in.local_reg_id;
        map_ptr->local_region = in.local_region;
        map_ptr->local_ndim = in.ndim;
        map_ptr->local_data_type = in.local_type;
        info = HG_Get_info(handle);
        HG_Addr_dup(info->hg_class, info->addr, &(map_ptr->local_addr));
        HG_Bulk_ref_incr(in.local_bulk_handle);
        map_ptr->local_bulk_handle = in.local_bulk_handle;
        
        PDC_mapping_info_t *m_info_ptr = (PDC_mapping_info_t *)malloc(sizeof(PDC_mapping_info_t));
        m_info_ptr->remote_obj_id = in.remote_obj_id;
        m_info_ptr->remote_reg_id = in.remote_reg_id;
        m_info_ptr->remote_client_id = in.remote_client_id;
        m_info_ptr->remote_ndim = in.ndim;
        m_info_ptr->remote_region = in.remote_region;
        m_info_ptr->remote_bulk_handle = in.remote_bulk_handle;
        m_info_ptr->remote_addr = map_ptr->local_addr;
        m_info_ptr->from_obj_id = map_ptr->local_obj_id;
        HG_Bulk_ref_incr(in.remote_bulk_handle);
        PDC_LIST_INSERT_HEAD(&map_ptr->ids, m_info_ptr, entry);
        DL_APPEND(target_obj->region_map_head, map_ptr);
        out.ret = 1;
    }

done:
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}


// Bulk
/* static hg_return_t */
/* query_partial_cb(hg_handle_t handle) */
// Server execute
HG_TEST_RPC_CB(query_partial, handle)
{
    hg_return_t ret_value;
    hg_return_t hg_ret;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    uint32_t i;
    void  **buf_ptrs;
    size_t *buf_sizes;
    uint32_t *n_meta_ptr, n_buf;
    metadata_query_transfer_in_t in;
    metadata_query_transfer_out_t out;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);

    out.ret = -1;

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

/*
 * Data server related
 */

static hg_return_t
update_storage_meta_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int cnt, i;
    uint64_t *obj_id_ptr;
    pdc_int_ret_t out_struct;
    void **buf;
    void *buf_1d;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        ret = HG_PROTOCOL_ERROR;
        goto done;
    }
    else {

        cnt = bulk_args->cnt;
        buf = (void**)calloc(sizeof(void*), cnt);

        /* printf("==PDC_SERVER[x]: finished bulk xfer, with %d regions, %ld bytes\n",cnt-1,bulk_args->nbytes); */
        /* fflush(stdout); */

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf_1d, NULL, NULL);

        // Mercury combines the buffer into 1, so need to make it 2d ptr again
        buf[0] = buf_1d;
        buf[1] = buf_1d + sizeof(uint64_t);
        for (i = 2; i < cnt; i++) {
            buf[i] = buf[i-1] + sizeof(update_region_storage_meta_bulk_t);
        }

        // Now we have the storage info in buf
        // First elem is the obj id, following by cnt region infos
        obj_id_ptr = (uint64_t*)buf[0];
        if (*obj_id_ptr <= 0) {
            printf("==PDC_SERVER[ ]: error with bulk access, obj id invalid!\n");
            ret = EXIT_FAILURE;
            goto done;
        }

        /* printf("==PDC_SERVER[x]: obj id is %" PRIu64 " \n", *obj_id_ptr); */
        /* fflush(stdout); */

        if (PDC_Server_update_region_storage_meta_bulk_local((update_region_storage_meta_bulk_t**)buf, cnt) 
            == SUCCEED) {

            free(buf);
            // Should not free buf_1d here
            /* free(buf_1d); */
            
            out_struct.ret = 9999;
        }
    } // end of else

done:
    /* Free block handle */
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

    /* Send response back */
    ret = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    /* printf("==PDC_SERVER[ ]: Responded to client %d of storage meta update!\n", bulk_args->origin); */
    /* fflush(stdout); */

    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return ret;
}

/* static hg_return_t */
/* bulk_rpc_cb(hg_handle_t handle) */
// Server execute after receives the bulk_rpc from another server
HG_TEST_RPC_CB(bulk_rpc, handle)
{
    const struct hg_info *hg_info = NULL;
    hg_bulk_t origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    struct bulk_args_t *bulk_args = NULL;
    bulk_rpc_in_t in_struct;
    hg_return_t ret = HG_SUCCESS;
    int cnt;

    hg_op_id_t hg_bulk_op_id;

    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    bulk_args->origin = in_struct.origin;

    /* Get parameters */
    cnt = in_struct.cnt;
    origin_bulk_handle = in_struct.bulk_handle;

    /* printf("==PDC_SERVER[x]: received update storage meta bulk rpc from %d, with %d regions\n", */ 
    /*         in_struct.origin, cnt); */
    /* fflush(stdout); */

    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt = cnt;

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *) &bulk_args->nbytes, HG_BULK_READWRITE, 
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, update_storage_meta_bulk_cb,
                           bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
                           local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    /* printf("==PDC_SERVER[x]: Pulled data from %d\n", in_struct.origin); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in_struct);

    FUNC_LEAVE(ret);
}



// READ
/* static hg_return_t */
// data_server_read_cb(hg_handle_t handle)
HG_TEST_RPC_CB(data_server_read, handle)
{
    hg_return_t ret_value;
    data_server_read_in_t  in;
    data_server_read_out_t out;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got data server read request from client %d\n", in.client_id); */
    /* fflush(stdout); */

    data_server_io_info_t *io_info= (data_server_io_info_t*)malloc(sizeof(data_server_io_info_t));

    io_info->io_type   = READ;
    io_info->client_id = in.client_id;
    io_info->nclient   = in.nclient;
    io_info->nbuffer_request = in.nupdate;

    PDC_metadata_init(&io_info->meta);
    pdc_transfer_t_to_metadata_t(&(in.meta), &(io_info->meta));

    PDC_init_region_list( &(io_info->region));
    pdc_region_transfer_t_to_list_t(&(in.region), &(io_info->region));

    io_info->region.access_type = io_info->io_type;
    io_info->region.meta = &(io_info->meta);
    io_info->region.client_ids[0] = in.client_id;


    out.ret = 1;
    HG_Respond(handle, PDC_Server_data_io_via_shm, io_info, &out);

    ret_value = HG_SUCCESS;

    HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);
    if (ret_value != HG_SUCCESS) 
        printf("==PDC_SERVER: data_server_read_cb - Error with HG_Destroy\n");
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


// WRITE
// data_server_write_cb(hg_handle_t handle)
HG_TEST_RPC_CB(data_server_write, handle)
{
    hg_return_t ret_value;
    data_server_write_in_t  in;
    data_server_write_out_t out;

    FUNC_ENTER(NULL);

    HG_Get_input(handle, &in);

    data_server_io_info_t *io_info= (data_server_io_info_t*)malloc(sizeof(data_server_io_info_t));

    io_info->io_type   = WRITE;
    io_info->client_id = in.client_id;
    io_info->nclient   = in.nclient;
    io_info->nbuffer_request = in.nupdate;

    PDC_metadata_init(&io_info->meta);
    pdc_transfer_t_to_metadata_t(&(in.meta), &(io_info->meta));

    PDC_init_region_list( &(io_info->region));
    pdc_region_transfer_t_to_list_t(&(in.region), &(io_info->region));

    strcpy(io_info->region.shm_addr, in.shm_addr);
    io_info->region.access_type = io_info->io_type;
    io_info->region.meta = &(io_info->meta);
    io_info->region.client_ids[0] = in.client_id;

    out.ret = 1;
    HG_Respond(handle, PDC_Server_data_io_via_shm, io_info, &out);

    /* printf("==PDC_SERVER: respond write request confirmation to client %d\n", in.client_id); */
    /* fflush(stdout); */


    ret_value = HG_SUCCESS;

    HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);
    if (ret_value != HG_SUCCESS) 
        printf("==PDC_SERVER: data_server_write_cb - Error with HG_Destroy\n");
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


// IO CHECK
// data_server_read_check(hg_handle_t handle)
HG_TEST_RPC_CB(data_server_read_check, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;

    // Decode input
    data_server_read_check_in_t  in;
    data_server_read_check_out_t out;
    out.shm_addr = NULL;

    ret_value = HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER[x]: Got data server read_check request from client %d for [%s] " */
    /*        "start %" PRIu64", count %" PRIu64"\n", */ 
    /*         in.client_id, in.meta.obj_name, in.region.start_0, in.region.count_0); */

    PDC_Server_read_check(&in, &out);

    ret_value = HG_Respond(handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER[x]: server read_check returning ret=%d, shm_addr=%s\n", out.ret, out.shm_addr); */

    if (NULL != out.shm_addr && out.shm_addr[0] != ' ')
        free(out.shm_addr);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


// data_server_write_check_cb(hg_handle_t handle)
HG_TEST_RPC_CB(data_server_write_check, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;

    // Decode input
    data_server_write_check_in_t  in;
    data_server_write_check_out_t *out = (data_server_write_check_out_t*)calloc(sizeof(data_server_write_check_out_t), 1);

    ret_value = HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got data server write_check request from client %d\n", in.client_id); */

    PDC_Server_write_check(&in, out);

    ret_value = HG_Respond(handle, NULL, NULL, out);
    // After returning the last write check finish to client, start the storage meta bulk update process
    /* ret_value = HG_Respond(handle, PDC_Server_count_write_check_update_storage_meta_cb, out, out); */
    /* printf("==PDC_SERVER: server write_check returning ret=%d\n", out->ret); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/* update_region_loc_cb */
HG_TEST_RPC_CB(update_region_loc, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    update_region_loc_in_t  in;
    update_region_loc_out_t out;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got region location update request: obj_id=%" PRIu64 "\n", in.obj_id); */
    /* fflush(stdout); */

    region_list_t *input_region = (region_list_t*)malloc(sizeof(region_list_t));
    pdc_region_transfer_t_to_list_t(&in.region, input_region);
    strcpy(input_region->storage_location, in.storage_location);
    input_region->offset = in.offset;

    /* PDC_print_region_list(input_region); */
    /* fflush(stdout); */

    out.ret = 20171031;
    ret_value = PDC_Server_update_local_region_storage_loc(input_region, in.obj_id);
    if (ret_value != SUCCEED) {
        out.ret = -1;
        printf("==PDC_SERVER: FAILED to update region location: obj_id=%" PRIu64 "\n", in.obj_id);
        fflush(stdout);
    }

    /* HG_Respond(handle, NULL, NULL, &out); */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

// not finished yet
/* get_reg_lock_status_cb */
HG_TEST_RPC_CB(get_reg_lock_status, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    get_reg_lock_status_in_t  in;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
//    printf("==PDC_SERVER: Got metadata retrieval: obj_id=%" PRIu64 "\n", in.obj_id);

    FUNC_LEAVE(ret_value);
}

/* get_metadata_by_id_cb */
HG_TEST_RPC_CB(get_metadata_by_id, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    get_metadata_by_id_in_t  in;
    get_metadata_by_id_out_t out;
    pdc_metadata_t *target = NULL;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
//    printf("==PDC_SERVER: Got metadata retrieval: obj_id=%" PRIu64 "\n", in.obj_id);

    PDC_Server_get_local_metadata_by_id(in.obj_id, &target);

    if (target != NULL) 
        pdc_metadata_t_to_transfer_t(target, &out.res_meta);
    else {
        printf("==PDC_SERVER: no matching metadata of obj_id=%" PRIu64 "\n", in.obj_id);
        out.res_meta.user_id        = -1;
        out.res_meta.obj_id         = 0;
        out.res_meta.time_step      = -1;
        out.res_meta.obj_name       = "N/A";
        out.res_meta.app_name       = "N/A";
        out.res_meta.tags           = "N/A";
        out.res_meta.data_location  = "N/A"; 
    }
    
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}


/* // Replace the 0s in the buf so that Mercury can transfer the entire buf */
/* perr_t PDC_replace_char_fill_values(signed char *buf, uint32_t buf_size) */
/* { */
/*     perr_t ret_value = SUCCEED; */
/*     uint32_t zero_cnt, i; */
/*     signed char fill_value; */
/*     FUNC_ENTER(NULL); */

/*     fill_value = buf[buf_size-1]; */
/*     zero_cnt = 0; */

/*     for (i = 0; i < buf_size; i++) { */
/*         if (buf[i] == fill_value) { */
/*             buf[i] = 0; */
/*             zero_cnt++; */
/*         } */
/*         else if (buf[i] == 0) { */
/*             printf("==ERROR! PDC_replace_char_fill_values 0 exist at %d!\n", i); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*     } */

/*     /1* printf("==PDC_replace_char_fill_values replaced %d char fill values %d!\n", zero_cnt, (int)fill_value); *1/ */

/* done: */
/*     fflush(stdout); */
/*     FUNC_LEAVE(ret_value); */
/* } */


/* // Replace the 0s in the buf so that Mercury can transfer the entire buf */
/* perr_t PDC_replace_zero_chars(signed char *buf, uint32_t buf_size) */
/* { */
/*     perr_t ret_value = SUCCEED; */
/*     uint32_t zero_cnt, i, fill_value_valid = 0; */
/*     signed char fill_value; */
/*     char num_cnt[257] = {0}; */
/*     FUNC_ENTER(NULL); */

/*     memset(num_cnt, 0, 256); */

/*     for (i = 0; i < buf_size; i++) { */
/*         num_cnt[buf[i]+128]++; */
/*     } */

/*     for (i = 0; i < 256; i++) { */
/*         if (num_cnt[i] == 0) { */
/*             fill_value = i - 128; */
/*             fill_value_valid = 1; */
/*             break; */
/*         } */
/*     } */

/*     if (fill_value_valid == 0) { */
/*         printf("==PDC_replace_zero_chars cannot find a char value not used by buffer!\n"); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     zero_cnt = 0; */
/*     for (i = 0; i < buf_size; i++) { */
/*         if (buf[i] == fill_value) { */
/*             printf("==PDC_replace_zero_chars CHAR_FILL_VALUE exist at %d!\n", i); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         else if (buf[i] == 0) { */
/*             buf[i] = fill_value; */
/*             zero_cnt++; */
/*         } */
/*     } */

/*     // Last char is fill value */
/*     buf[buf_size-1] = fill_value; */

/*     /1* printf("==PDC_replace_zero_chars: replaced %d zero values with %d.\n", zero_cnt, (int)fill_value); *1/ */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

/*
 * Serialize the region info structure for network transfer,
 * including ndim, start[], count[], storage loc
 *
 * \param  regions[IN]       List of region info to be serialized
 * \param  n_region[IN]      Number of regions in the list
 * \param  buf[OUT]          Serialized data
 *
 * \return Non-negative on success/Negative on failure
 */
/* perr_t PDC_serialize_regions_lists(region_list_t** regions, uint32_t n_region, void *buf, uint32_t buf_size) */
/* { */
/*     perr_t ret_value = SUCCEED; */
/*     uint32_t i, j; */
/*     uint32_t ndim, loc_len, total_len; */
/*     uint32_t *uint32_ptr = NULL; */
/*     uint64_t *uint64_ptr = NULL; */
/*     signed char *char_ptr   = NULL; */

/*     FUNC_ENTER(NULL); */

/*     if (regions == NULL || regions[0] == NULL) { */
/*         printf("==PDC_SERVER: PDC_serialize_regions_lists NULL input!\n"); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     total_len = 0; */

/*     ndim = regions[0]->ndim; */

/*     // serialize format: */
/*     // n_region|ndim|start00|count00|...|start0n|count0n|loc_len|loc_str|offset|... */

/*     uint32_ptr  = (uint32_t*)buf; */
/*     *uint32_ptr = n_region; */

/*     /1* printf("==PDC_SERVER: serializing %u regions!\n", n_region); *1/ */

/*     uint32_ptr++; */
/*     total_len += sizeof(uint32_t); */

/*     *uint32_ptr = ndim; */

/*     uint32_ptr++; */
/*     total_len += sizeof(uint32_t); */

/*     uint64_ptr = (uint64_t*)uint32_ptr; */

/*     for (i = 0; i < n_region; i++) { */
/*         if (regions[i] == NULL) { */
/*             printf("==PDC_serialize_regions_lists NULL input!\n"); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         for (j = 0; j < ndim; j++) { */
/*             *uint64_ptr = regions[i]->start[j]; */
/*             uint64_ptr++; */
/*             total_len += sizeof(uint64_t); */
/*             *uint64_ptr = regions[i]->count[j]; */
/*             uint64_ptr++; */
/*             total_len += sizeof(uint64_t); */
/*         } */

/*         loc_len = strlen(regions[i]->storage_location); */
/*         uint32_ptr  = (uint32_t*)uint64_ptr; */
/*         *uint32_ptr = loc_len; */
/*         uint32_ptr++; */
/*         total_len += sizeof(uint32_t); */

/*         char_ptr = (signed char*)uint32_ptr; */
/*         if (loc_len <= 0) { */
/*             printf("==PDC_serialize_regions_lists invalid storage location [%s]!\n", */
/*                                 regions[i]->storage_location); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         strcpy((char*)char_ptr, regions[i]->storage_location); */
/*         char_ptr[loc_len] = PDC_STR_DELIM;  // Delim to replace 0 */
/*         char_ptr += (loc_len + 1); */
/*         total_len += (loc_len + 1); */

/*         uint64_ptr = (uint64_t*)char_ptr; */

/*         *uint64_ptr = regions[i]->offset; */
/*         uint64_ptr++; */
/*         total_len += sizeof(uint64_t); */

/*         if (total_len > buf_size) { */
/*             printf("==PDC_serialize_regions_lists total_len %u exceeds " */
/*                     "buf_len %u\n", total_len, buf_size); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         /1* PDC_print_region_list(regions[i]); *1/ */
/*     } */

/*     if (PDC_replace_zero_chars((char*)buf, buf_size) != SUCCEED) { */
/*         printf("==PDC_serialize_regions_lists PDC_replace_zero_chars ERROR! "); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     /1* if (is_debug_g == 1) { *1/ */
/*     /1*     printf("==PDC_serialize_regions_lists n_region: %u, buf len is %u \n", *1/ */
/*     /1*             n_region, total_len); *1/ */
/*     /1*     uint32_t nr_region; *1/ */

/*     /1*     region_list_t **r_regions = (region_list_t**)malloc(sizeof(region_list_t*) * PDC_MAX_OVERLAP_REGION_NUM     ); *1/ */
/*     /1*     for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++) { *1/ */
/*     /1*         r_regions[i] = (region_list_t*)calloc(1, sizeof(region_list_t)); *1/ */
/*     /1*     } *1/ */

/*     /1*     PDC_unserialize_region_lists(buf, r_regions, &nr_region); *1/ */

/*         /1* printf("==PDC_serialize_regions_lists after unserialize %d\n", nr_region); *1/ */
/*     /1*     for (i = 0; i < nr_region; i++) { *1/ */
/*     /1*         PDC_print_region_list(r_regions[i]); *1/ */
/*     /1*     } *1/ */
/*     /1* } *1/ */

/* done: */
/*     fflush(stdout); */
/*     FUNC_LEAVE(ret_value); */
/* } // PDC_serialize_regions_lists */

/*
 * Un-serialize the region info structure from network transfer,
 * including ndim, start[], count[], storage loc
 *
 * \param  buf[IN]            Serialized data
 * \param  regions[OUT]       List of region info that are un-serialized
 * \param  n_region[OUT]      Number of regions in the list
 *
 * \return Non-negative on success/Negative on failure
 */
/* perr_t PDC_unserialize_region_lists(void *buf, region_list_t** regions, uint32_t *n_region) */
/* { */
/*     perr_t ret_value = SUCCEED; */
/*     uint32_t i, j; */
/*     uint32_t ndim, loc_len, buf_size; */
/*     uint32_t *uint32_ptr = NULL; */
/*     uint64_t *uint64_ptr = NULL; */
/*     signed char *char_ptr   = NULL; */

/*     FUNC_ENTER(NULL); */

/*     if (buf == NULL || regions == NULL || n_region == NULL) { */
/*         printf("==PDC_SERVER: PDC_unserialize_region_lists NULL input!\n"); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     char_ptr = (signed char*)buf; */
/*     buf_size = strlen((char*)char_ptr); */

/*     ret_value = PDC_replace_char_fill_values(char_ptr, buf_size); */
/*     if (ret_value != SUCCEED) { */
/*         printf("==PDC_unserialize_region_lists replace_char_fill_values ERROR!\n"); */
/*         goto done; */
/*     } */

/*     // n_region|ndim|start00|count00|...|start0n|count0n|loc_len|loc_str|offset|... */

/*     uint32_ptr = (uint32_t*)buf; */
/*     *n_region = *uint32_ptr; */

/*     /1* printf("==PDC_unserialize_region_lists: %u regions!\n", *n_region); *1/ */

/*     uint32_ptr++; */
/*     ndim = *uint32_ptr; */

/*     if (ndim == 0 || ndim > 3) { */
/*         printf("==PDC_unserialize_region_lists ndim %"PRIu32" ERROR!\n", ndim); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     uint32_ptr++; */
/*     uint64_ptr = (uint64_t*)uint32_ptr; */

/*     for (i = 0; i < *n_region; i++) { */
/*         if (regions[i] == NULL) { */
/*             printf("==PDC_unserialize_region_lists NULL input," */
/*                     " try increade PDC_MAX_OVERLAP_REGION_NUM value!\n"); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         regions[i]->ndim = ndim; */

/*         for (j = 0; j < ndim; j++) { */
/*             regions[i]->start[j] = *uint64_ptr; */
/*             uint64_ptr++; */
/*             regions[i]->count[j] = *uint64_ptr; */
/*             uint64_ptr++; */
/*         } */

/*         /1* printf("==unserialize_regions_info start[0]: %" PRIu64 ", count[0]: %" PRIu64 "\n", *1/ */
/*         /1*         regions[i]->start[0], regions[i]->count[0]); *1/ */

/*         uint32_ptr = (uint32_t*)uint64_ptr; */
/*         loc_len = *uint32_ptr; */

/*         uint32_ptr++; */

/*         char_ptr = (signed char*)uint32_ptr; */
/*         // Verify delimiter */
/*         if (char_ptr[loc_len] != PDC_STR_DELIM) { */
/*             printf("==PDC_unserialize_region_lists delim error!\n"); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */

/*         strncpy(regions[i]->storage_location, (char*)char_ptr, loc_len); */
/*         // Add end of string */
/*         regions[i]->storage_location[loc_len] = 0; */

/*         char_ptr += (loc_len + 1); */

/*         uint64_ptr = (uint64_t*)char_ptr; */
/*         regions[i]->offset = *uint64_ptr; */

/*         uint64_ptr++; */
/*         // n_region|ndim|start00|count00|...|start0n|count0n|loc_len|loc_str|offset|... */
/*         // */
/*         /1* printf("==PDC_SERVER: unserialize_region_list\n"); *1/ */
/*         /1* PDC_print_region_list(regions[i]); *1/ */
/*     } */

/* done: */
/*     fflush(stdout); */
/*     FUNC_LEAVE(ret_value); */
/* } // PDC_unserialize_region_lists */

/*
 * Calculate the total string length of the regions to be serialized
 *
 * \param  regions[IN]       List of region info that are un-serialized
 * \param  n_region[IN]      Number of regions in the list
 * \param  len[OUT]          Length of the serialized string
 *
 * \return Non-negative on success/Negative on failure
 */
/* perr_t PDC_get_serialized_size(region_list_t** regions, uint32_t n_region, uint32_t *len) */
/* { */
/*     perr_t ret_value = SUCCEED; */
/*     uint32_t i; */

/*     FUNC_ENTER(NULL); */

/*     if (regions == NULL || n_region == 0 || len == NULL || regions[0] == NULL) { */
/*         printf("==PDC_SERVER: PDC_get_serialized_size NULL input!\n"); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     *len = 0; */
/*     for (i = 0; i < n_region; i++) { */
/*         if (regions[i] == NULL) { */
/*             printf("==PDC_SERVER: PDC_get_serialized_size NULL input in regions!\n"); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         *len += (strlen(regions[i]->storage_location) + 1); */
/*     } */

/*             // n_region | ndim | start00 | count00 | ... | startndim0 | countndim0 | loc_len | loc | */
/*             // delim | offset | char_fill_value | \0 */
/*      *len += ( sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t)*regions[0]->ndim*2*n_region + */
/*                sizeof(uint32_t)*n_region + sizeof(uint64_t)*n_region + 2); */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

/* get_storage_info_cb */
/* HG_TEST_RPC_CB(get_storage_info, handle) */
/* { */
/*     perr_t                        ret_value = HG_SUCCESS; */
/*     get_storage_info_in_t         in; */
/*     get_storage_info_single_out_t single_region_out; */
/*     region_list_t                 request_region; */
/*     region_list_t                 **result_regions; */

/*     uint32_t n_region, i; */
/*     void *buf = NULL; */

/*     FUNC_ENTER(NULL); */

/*     // Decode input */
/*     HG_Get_input(handle, &in); */

/*     /1* printf("==PDC_SERVER: Got storage info request from other server: obj_id=%" PRIu64 "\n", in.obj_id); *1/ */

/*     PDC_init_region_list(&request_region); */
/*     pdc_region_transfer_t_to_list_t(&in.req_region, &request_region); */

/*     result_regions = (region_list_t**)calloc(1, sizeof(region_list_t*)*PDC_MAX_OVERLAP_REGION_NUM); */
/*     for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++) */
/*         result_regions[i] = (region_list_t*)malloc(sizeof(region_list_t)); */

/*     if (PDC_Server_get_local_storage_location_of_region(in.obj_id, &request_region, &n_region, result_regions) != SUCCEED) { */
/*         printf("==PDC_SERVER: unable to get_local_storage_location_of_region\n"); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */
/*     else { */
/*         if (n_region == 1) { */
/*             // If there is only one matching region, send it with the easy way throught Mercury */
/*             pdc_region_list_t_to_transfer(result_regions[0], &single_region_out.region_transfer); */
/*             single_region_out.storage_loc = result_regions[0]->storage_location; */
/*             single_region_out.file_offset = result_regions[0]->offset; */
/*             HG_Respond(handle, PDC_Server_s2s_recv_work_done_cb, NULL, &single_region_out); */
/*         } */
/*         else { */
/*             // If there are multiple regions, serialze them to a 1D buf, replace the 0s */
/*             // and send as a string. */
/*             // FIXME: with large scale, i.e. more than 128 servers, this approach don't work correctly */

/*             /1* if (PDC_get_serialized_size(result_regions, n_region, &serialize_len) != SUCCEED) { *1/ */
/*             /1*     printf("==PDC_SERVER: fail to get_total_str_len\n"); *1/ */
/*             /1*     ret_value = FAIL; *1/ */
/*             /1*     goto done; *1/ */
/*             /1* } *1/ */

/*             /1* buf = (void*)calloc(1, serialize_len); *1/ */
/*             /1* if (PDC_serialize_regions_lists(result_regions, n_region, buf, serialize_len) != SUCCEED) { *1/ */
/*             /1*     printf("==PDC_SERVER: unable to serialize_regions_info\n"); *1/ */
/*             /1*     ret_value = FAIL; *1/ */
/*             /1*     goto done; *1/ */
/*             /1* } *1/ */
/*             /1* serialized_out.buf = buf; *1/ */

/*             /1* // Check if 0 still exists in buffer *1/ */
/*             /1* char_ptr = (signed char*)serialized_out.buf; *1/ */
/*             /1* buf_len = strlen(char_ptr); *1/ */
/*             /1* for (i = 0; i < serialize_len; i++) { *1/ */
/*             /1*     if (char_ptr[i] == 0) { *1/ */
/*             /1*         printf("==PDC_SERVER:ERROR with serialize buf, 0 detected in string, %u!\n", i); *1/ */
/*             /1*         fflush(stdout); *1/ */
/*             /1*     } *1/ */
/*             /1* } *1/ */

/*             // debug print */
/*             /1* printf("==PDC_SERVER: serialize_region allocated %u, real %u \n", serialize_len, buf_len); *1/ */
/*             /1* fflush(stdout); *1/ */
/*             /1* HG_Respond(handle, PDC_Server_s2s_recv_work_done_cb, NULL, &serialized_out); *1/ */

/*             printf("==PDC_SERVER: cannot handle more than 1 storage region match!\n"); */
/*             fflush(stdout); */
/*             ret_value = FAIL; */
/*             HG_Respond(handle, PDC_Server_s2s_recv_work_done_cb, NULL, &single_region_out); */
/*             goto done; */
/*         } // end of else (n_region is not 1) */
/*     } // end of else */

/* done: */
/*     if (result_regions != NULL) { */
/*         for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++) { */
/*             if (result_regions[i] != NULL) */
/*                 free(result_regions[i]); */
/*         } */
/*         free(result_regions); */
/*     } */

/*     if (buf != NULL) */
/*         free(buf); */

/*     HG_Free_input(handle, &in); */
/*     HG_Destroy(handle); */

/*     FUNC_LEAVE(ret_value); */
/* } */

/* aggregate_write_cb */
HG_TEST_RPC_CB(aggregate_write, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    pdc_aggregated_io_to_server_t in;
    pdc_int_ret_t                 out;

    region_list_t request_region;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got aggregate write request: obj_id=%" PRIu64 "\n", in.meta.obj_id); */
    PDC_init_region_list(&request_region);

    // Need to un-serialize regions from each client of the same node one by one


/*     pdc_region_transfer_t_to_list_t(&in.req_region, &request_region); */

/*     result_regions = (region_list_t**)calloc(1, sizeof(region_list_t*)*PDC_MAX_OVERLAP_REGION_NUM); */
/*     result_regions_all = (region_list_t*)calloc(sizeof(region_list_t), PDC_MAX_OVERLAP_REGION_NUM); */
/*     for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++) */ 
/*         result_regions[i] = (region_list_t*)malloc(sizeof(region_list_t)); */



    out.ret = 1;

    HG_Respond(handle, NULL, NULL, &out);

    /* if (result_regions != NULL) { */
    /*     for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++) { */
    /*         if (result_regions[i] != NULL) */ 
    /*             free(result_regions[i]); */
    /*     } */
    /*     free(result_regions); */
    /* } */

    
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}


/* 
 * NOTE:
 *   Because we use dlopen to dynamically open
 *   an executable, it may be necessary for the server
 *   to have the LD_LIBRARY_PATH of the client.
 *   This can/should be part of the UDF registration
 *   with the server, i.e. we provide the server
 *   with: 
 *      a) the full path to the client executable
 *         which must be compiled with the "-fpie -rdynamic"
 *         flags.
 *      b) the contents of the PATH and LD_LIBRARY_PATH
 *         environment variables.
 */

char *
remove_relative_dirs(char *workingDir, char *application)
{
     int k, levels_up = 0;
     char *appName = application;
     char *dotdot;
     while((dotdot = strstr(appName,"../")) != NULL) {
       levels_up++;
       appName = dotdot + 3;
     }
     for(k=0; k<levels_up; k++) {
       char *slash = strrchr(workingDir,'/');
       if (slash) *slash = 0;
     }
     k = strlen(workingDir);
     if ((appName[0] == '.') && (appName[1] == '/'))
       appName += 2;
     sprintf(&workingDir[k],"/%s", appName);
     return strdup(workingDir);
}

char *
find_in_path(char *workingDir, char *application)
{
     struct stat fileStat;
     char *pathVar = getenv("PATH");
     char colon = ':';
     char checkPath[PATH_MAX];
     char *next = strchr(pathVar,colon);
     while(next) {
       *next++ = 0;
       sprintf(checkPath,"%s/%s",pathVar,application);
       if (stat(checkPath,&fileStat) == 0) {
	 return strdup(checkPath);
       }
       pathVar = next;
       next = strchr(pathVar,colon);
     }
     if (application[0] == '.') {
       sprintf(checkPath, "%s/%s", workingDir, application);
       if (stat(checkPath,&fileStat) == 0) {
	 char *foundPath = strrchr(checkPath,'/');
	 char *appName = foundPath+1;
	 if (foundPath == NULL) {
	   return remove_relative_dirs(workingDir, application);
	 }
	 *foundPath = 0;
	 // Change directory (pushd) to the where we find the application
	 if (chdir(checkPath) == 0) {
	   int offset;
	   getcwd(checkPath,sizeof(checkPath));
	   offset = strlen(checkPath);
	   // Change back (popd) to where we started 
	   chdir(workingDir);
	   sprintf(&checkPath[offset], "/%s", appName);
	   return strdup(checkPath);
	 }
       }
     }
     return NULL;
}

// Update container with objects
static hg_return_t
cont_add_del_objs_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    // Server executes after received request from client
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int cnt, i, op;
    cont_add_del_objs_rpc_out_t out_struct;
    uint64_t *obj_ids, cont_id;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        ret = HG_PROTOCOL_ERROR;
        goto done;
    }
    else {

        cnt     = bulk_args->cnt;
        op      = bulk_args->op;
        cont_id = bulk_args->cont_id;
        obj_ids = (uint64_t*)calloc(sizeof(uint64_t), cnt);

        /* printf("==PDC_SERVER[x]: finished bulk xfer, with %d regions, %ld bytes\n",cnt-1,bulk_args->nbytes); */
        /* fflush(stdout); */

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, (void**)&obj_ids, NULL, NULL);

        if (op == ADD_OBJ) {
            if (PDC_Server_container_add_objs(cnt, obj_ids, cont_id) == SUCCEED) 
                out_struct.ret = 1;
            else
                printf("==PDC_SERVER[ ]: error updating objects to container\n");
        }
        else if (op == DEL_OBJ) {
            if (PDC_Server_container_del_objs(cnt, obj_ids, cont_id) == SUCCEED) 
                out_struct.ret = 1;
            else
                printf("==PDC_SERVER[ ]: error updating objects to container\n");
        }
        else {
            printf("==PDC_SERVER[ ]: %s - unsupported container operation type\n", __func__);
            out_struct.ret = 0;
            goto done;
        }

    } // end of else

done:
    /* Free block handle */
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

    /* Send response back */
    ret = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    /* printf("==PDC_SERVER[ ]: Responded to client %d of storage meta update!\n", bulk_args->origin); */
    /* fflush(stdout); */

    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return ret;
}

/* cont_add_del_objs_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(cont_add_del_objs_rpc, handle)
{
    const struct hg_info *hg_info = NULL;
    struct bulk_args_t *bulk_args = NULL;
    hg_bulk_t origin_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle   = HG_BULK_NULL;
    hg_return_t ret = HG_SUCCESS;

    cont_add_del_objs_rpc_in_t in_struct;

    hg_op_id_t hg_bulk_op_id;

    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    bulk_args->origin = in_struct.origin;

    /* Get parameters */
    origin_bulk_handle = in_struct.bulk_handle;

    /* printf("==PDC_SERVER[x]: received update container bulk rpc from %d, with %d regions\n", */ 
    /*         in_struct.origin, in_struct.cnt); */
    /* fflush(stdout); */

    bulk_args->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt     = in_struct.cnt;
    bulk_args->op      = in_struct.op;
    bulk_args->cont_id = in_struct.cont_id;

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE, 
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, cont_add_del_objs_bulk_cb,
                           bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
                           local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    /* printf("==PDC_SERVER[x]: Pulled data from %d\n", in_struct.origin); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret);
}


// Update container with objects
static hg_return_t
query_read_obj_name_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    // Server executes after received request from client
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int i, iter;
    char* tmp_buf;
    query_read_obj_name_out_t out_struct;
    query_read_names_args_t *query_read_names_args;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        goto done;
    }
    else {
        query_read_names_args = (query_read_names_args_t*)calloc(1, sizeof(query_read_names_args_t));
        query_read_names_args->cnt = bulk_args->cnt;
        query_read_names_args->client_seq_id = bulk_args->client_seq_id;
        query_read_names_args->client_id = bulk_args->origin;
        query_read_names_args->is_select_all = 1;
        query_read_names_args->obj_names = (char**)calloc(sizeof(char*), bulk_args->cnt);
        query_read_names_args->obj_names_1d = (char*)calloc(sizeof(char), bulk_args->nbytes);

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE,1,(void**)&tmp_buf, NULL, NULL);
        memcpy(query_read_names_args->obj_names_1d, tmp_buf, bulk_args->nbytes);

        // Parse the obj_names to the 2d obj_names
        iter = 0;
        query_read_names_args->obj_names[iter++] = query_read_names_args->obj_names_1d;
        for (i = 1; i < bulk_args->nbytes; i++) {
            if (query_read_names_args->obj_names_1d[i-1] == '\0')
                query_read_names_args->obj_names[iter++] = &query_read_names_args->obj_names_1d[i];
        }

        /* printf("==PDC_SERVER[x]: finished bulk xfer, with %d regions, %ld bytes\n",cnt-1,bulk_args->nbytes); */
        /* fflush(stdout); */

    }

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret = HG_Respond(bulk_args->handle, PDC_Server_query_read_names_cb, query_read_names_args, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    /* Free block handle */
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

    /* printf("==PDC_SERVER[ ]: Responded to client %d of storage meta update!\n", bulk_args->origin); */
    /* fflush(stdout); */

done:
    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return ret;
}


/* query_read_obj_name_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(query_read_obj_name_rpc, handle)
{
    const struct hg_info *hg_info = NULL;
    struct bulk_args_t *bulk_args = NULL;
    hg_bulk_t origin_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle   = HG_BULK_NULL;
    hg_return_t ret = HG_SUCCESS;

    query_read_obj_name_in_t in_struct;

    hg_op_id_t hg_bulk_op_id;

    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* printf("==PDC_SERVER[x]: received update container bulk rpc from %d, with %d regions\n", */ 
    /*         in_struct.origin, in_struct.cnt); */
    /* fflush(stdout); */
    origin_bulk_handle = in_struct.bulk_handle;
    bulk_args->client_seq_id = in_struct.client_seq_id;
    bulk_args->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt     = in_struct.cnt;
    bulk_args->origin  = in_struct.origin;

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE, 
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, query_read_obj_name_bulk_cb,
                           bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
                           local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    /* printf("==PDC_SERVER[x]: Pulled data from %d\n", in_struct.origin); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret);
}


// Receives the query with one name, return all storage metadata of the corresponding object with bulk transfer
/* storage_meta_name_query_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(storage_meta_name_query_rpc, handle)
{
    hg_return_t ret = HG_SUCCESS;
    pdc_int_ret_t out;

    storage_meta_name_query_in_t in;
    storage_meta_name_query_in_t *args;

    HG_Get_input(handle, &in);
    // Duplicate the structure so we can continue to use it after leaving this function
    args = (storage_meta_name_query_in_t*)calloc(1, sizeof(storage_meta_name_query_in_t));
    args->obj_name = strdup(in.obj_name);
    args->task_id  = in.task_id;
    args->origin_id = in.origin_id;

    /* printf("==PDC_SERVER[x]: received storage meta query, name [%s], task_id %d, origin_id %d\n", */ 
    /*         args->obj_name, args->task_id, args->origin_id); */
    /* fflush(stdout); */
    
    out.ret = 1;
    HG_Respond(handle, PDC_Server_storage_meta_name_query_bulk_respond, args, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret);
} // end storage_meta_name_query_rpc_cb

// Generic function to check the return value (RPC receipt) is 1
hg_return_t pdc_check_int_ret_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    pdc_int_ret_t output;

    FUNC_ENTER(NULL);

    hg_handle_t handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==%s() - Error with HG_Get_output\n", __func__);
        goto done;
    }

    if (output.ret != 1) {
        printf("==%s() - Return value [%d] is NOT expected\n", __func__, output.ret);
    }
done:
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

static hg_return_t
get_storage_meta_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    // Server executes after received request from client
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int i, task_id, n_regions;
    int *int_ptr;
    char *char_ptr, *file_path;
    uint64_t *uint64_ptr, offset;
    void *buf;
    region_info_transfer_t *region_info_ptr;
    region_list_t *region_list, *region_list_head = NULL;
    pdc_int_ret_t out_struct;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        goto done;
    }
    else {

        n_regions = bulk_args->cnt;
        buf = (void*)calloc(1, bulk_args->nbytes);

        /* printf("==PDC_SERVER[x]: finished bulk xfer, with %d regions, %ld bytes\n", n_regions,bulk_args->nbytes); */
        /* fflush(stdout); */

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, NULL, NULL);

        // buf_ptrs[0]: task_id
        // then for each next ptr, path, offset, region info (region_info_transfer_t)
        int_ptr = (int*)buf;
        task_id = *int_ptr;
        int_ptr++;

        for (i = 0; i < n_regions; i++) {
            char_ptr = (char*)int_ptr;
            file_path = char_ptr;
            uint64_ptr = (uint64_t*)(char_ptr+strlen(char_ptr)+1);
            offset = *uint64_ptr;
            uint64_ptr++;
            region_info_ptr = (region_info_transfer_t*)uint64_ptr;
            region_list = (region_list_t*)calloc(1, sizeof(region_list_t));
            PDC_init_region_list(region_list);
            pdc_region_transfer_t_to_list_t(region_info_ptr, region_list);
            strcpy(region_list->storage_location, file_path);
            region_list->offset = offset;

            DL_APPEND(region_list_head, region_list);
            
            region_info_ptr++;
            int_ptr = (int*)region_info_ptr;
        }
    }

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    PDC_Server_proc_storage_meta_bulk(task_id, n_regions, region_list_head);

    /* Free block handle */
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

done:
    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return ret;
}

/* get_storage_meta_name_query_bulk_result_rpc_cb */
HG_TEST_RPC_CB(get_storage_meta_name_query_bulk_result_rpc, handle)
{
    const struct hg_info *hg_info = NULL;
    struct bulk_args_t *bulk_args = NULL;
    hg_bulk_t origin_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle   = HG_BULK_NULL;
    hg_return_t ret = HG_SUCCESS;

    bulk_rpc_in_t in_struct;

    hg_op_id_t hg_bulk_op_id;

    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    bulk_args->origin = in_struct.origin;

    origin_bulk_handle = in_struct.bulk_handle;

    bulk_args->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt     = in_struct.cnt;

    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE, 
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, get_storage_meta_bulk_cb,
                           bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
                           local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }
    /* printf("==PDC_SERVER[x]: Pulled data from %d\n", in_struct.origin); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in_struct);

    FUNC_LEAVE(ret);
}

static hg_return_t
notify_client_multi_io_complete_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    // Client executes after received request from server
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int i, task_id, n_shm;
    void *buf;
    char *char_ptr;
    char *buf_cp;
    pdc_int_ret_t out_struct;

    /* printf("==PDC_CLIENT[x]: accessing multi io complete shm addrs\n"); */
    /* fflush(stdout); */

    out_struct.ret = 0;
    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        goto done;
    }
    else {
        n_shm = bulk_args->cnt;
        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, NULL, NULL);
        buf_cp = (char*)malloc(bulk_args->nbytes);
        memcpy(buf_cp, buf, bulk_args->nbytes);
    }

    /* printf("==PDC_CLIENT[x]: respond back to server\n"); */
    /* fflush(stdout); */

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    PDC_Client_query_read_complete(buf_cp, bulk_args->nbytes, n_shm, bulk_args->client_seq_id);

    /* Free block handle */
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

done:
    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return ret;
}

/* notify_client_multi_io_complete_rpc_cb*/
HG_TEST_RPC_CB(notify_client_multi_io_complete_rpc, handle)
{
    const struct hg_info *hg_info = NULL;
    struct bulk_args_t *bulk_args = NULL;
    hg_bulk_t origin_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle   = HG_BULK_NULL;
    hg_return_t ret = HG_SUCCESS;
    bulk_rpc_in_t in_struct;
    hg_op_id_t hg_bulk_op_id;



    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));
    bulk_args->handle = handle;

    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    bulk_args->origin        = in_struct.origin;
    origin_bulk_handle       = in_struct.bulk_handle;
    bulk_args->nbytes        = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt           = in_struct.cnt;
    bulk_args->client_seq_id = in_struct.seq_id;

    /* printf("==PDC_CLIENT[x]: received multi io complete bulk rpc, with %lu bytes!\n", bulk_args->nbytes); */
    /* fflush(stdout); */

    hg_info = HG_Get_info(handle);
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE, 
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, notify_client_multi_io_complete_bulk_cb,
                           bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
                           local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    HG_Free_input(handle, &in_struct);
    FUNC_LEAVE(ret);
}


/*
 * Add a callback function and its parameters to a task list
 *
 * \param  target_list[IN]      target task list
 * \param  cb[IN]               callback function pointer
 * \param  cb_args[IN]          callback function parameters
 * \param  curr_task_id[IN]     global task sequence id
 *
 * \return Non-negative on success/Negative on failure
 */
int PDC_add_task_to_list(pdc_task_list_t **target_list, perr_t (*cb)(), void *cb_args, int *curr_task_id, 
                         hg_thread_mutex_t *mutex)
{
    pdc_task_list_t *new_task;

    FUNC_ENTER(NULL);
    if (target_list == NULL ) {
        printf("== %s - NULL input!\n", __func__);
        return -1;
    }

    new_task = (pdc_task_list_t*)calloc(1, sizeof(pdc_task_list_t));
    new_task->cb = cb;
    new_task->cb_args = cb_args;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(mutex);
#endif

    new_task->task_id = *curr_task_id;
    (*curr_task_id)++;
    DL_APPEND(*target_list, new_task);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(mutex);
#endif

    fflush(stdout);
    FUNC_LEAVE(new_task->task_id);
} // end PDC_add_task_to_list

/*
 * Delete a callback function and its parameters to a task list
 *
 * \param  target_list[IN]      target task list
 * \param  del[IN]              target task pointer to be deleted
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_del_task_from_list(pdc_task_list_t **target_list, pdc_task_list_t *del, hg_thread_mutex_t *mutex) 
{
    perr_t ret_value;
    pdc_task_list_t *tmp;
    FUNC_ENTER(NULL);

    if (target_list == NULL || del == NULL) {
        printf("== %s - NULL input!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(mutex);
#endif

    tmp = del;
    DL_DELETE(*target_list, del);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(mutex);
#endif
    free(tmp);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);                                                                                  
} // end PDC_del_task_from_list

int PDC_is_valid_task_id(int id)
{
    if (id < PDC_SERVER_TASK_INIT_VALUE || id > 10000 ) {
        printf("== id %d is invalid!\n", id);
        return -1;
    }
    return 1;
}

/*
 * Delete a callback function and its parameters to a task list
 *
 * \param  list_head[IN]        target task list
 * \param  id[IN]               target task ID
 *
 * \return task pointer on success/NULL on failure
 */
pdc_task_list_t *PDC_find_task_from_list(pdc_task_list_t** target_list, int id, hg_thread_mutex_t *mutex)
{
    pdc_task_list_t *tmp;
    FUNC_ENTER(NULL);

    if (PDC_is_valid_task_id(id) != 1) {
        printf("== %s - NULL input!\n", __func__);
        goto done;
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(mutex);
#endif

    DL_FOREACH(*target_list, tmp) {
        if (tmp->task_id == id) {
            return tmp;
        }
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(mutex);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(NULL);
} // end PDC_find_task_from_list

perr_t PDC_del_task_from_list_id(pdc_task_list_t **target_list, int id, hg_thread_mutex_t *mutex)
{
    perr_t ret_value;
    pdc_task_list_t *tmp;
    FUNC_ENTER(NULL);

    if (target_list == NULL || PDC_is_valid_task_id(id) != 1) {
        printf("== %s - NULL input!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(mutex);
#endif

    tmp = PDC_find_task_from_list(target_list, id, mutex);
    DL_DELETE(*target_list, tmp);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(mutex);
#endif
    free(tmp);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end PDC_del_task_from_list_id

int PDC_is_valid_obj_id(uint64_t id)
{
    if (id <  PDC_SERVER_ID_INTERVEL) {
        printf("== id %" PRIu64 " is invalid!\n", id);
        return -1;
    }
    return 1;
}
/* server_checkpoint_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(server_checkpoint_rpc, handle)
{
    perr_t ret_value = HG_SUCCESS;
    FUNC_ENTER(NULL);

    pdc_int_send_t in;
    pdc_int_ret_t  out;

    HG_Get_input(handle, &in);

    out.ret = 1;
    HG_Respond(handle, PDC_Server_checkpoint_cb, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
query_read_obj_name_client_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    // Server executes after received request from client
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int i, iter;
    char* tmp_buf;
    query_read_obj_name_out_t out_struct;
    query_read_names_args_t *query_read_names_args;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        goto done;
    }
    else {
        query_read_names_args = (query_read_names_args_t*)calloc(1, sizeof(query_read_names_args_t));
        query_read_names_args->cnt = bulk_args->cnt;
        query_read_names_args->client_seq_id = bulk_args->client_seq_id;
        query_read_names_args->client_id = bulk_args->origin;
        query_read_names_args->is_select_all = 1;
        query_read_names_args->obj_names = (char**)calloc(sizeof(char*), bulk_args->cnt);
        query_read_names_args->obj_names_1d = (char*)calloc(sizeof(char), bulk_args->nbytes);

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE,1,(void**)&tmp_buf, NULL, NULL);
        memcpy(query_read_names_args->obj_names_1d, tmp_buf, bulk_args->nbytes);

        // Parse the obj_names to the 2d obj_names
        iter = 0;
        query_read_names_args->obj_names[iter++] = query_read_names_args->obj_names_1d;
        for (i = 1; i < bulk_args->nbytes; i++) {
            if (query_read_names_args->obj_names_1d[i-1] == '\0')
                query_read_names_args->obj_names[iter++] = &query_read_names_args->obj_names_1d[i];
        }

        /* printf("==PDC_SERVER[x]: finished bulk xfer, with %d regions, %ld bytes\n",cnt-1,bulk_args->nbytes); */
        /* fflush(stdout); */

    }

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret = HG_Respond(bulk_args->handle, PDC_Server_query_read_names_clinet_cb, query_read_names_args, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    /* Free block handle */
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

    /* printf("==PDC_SERVER[ ]: Responded to client %d of storage meta update!\n", bulk_args->origin); */
    /* fflush(stdout); */

done:
    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return ret;
}

/* query_read_obj_name_client_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(query_read_obj_name_client_rpc, handle)
{
    const struct hg_info *hg_info = NULL;
    struct bulk_args_t *bulk_args = NULL;
    hg_bulk_t origin_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle   = HG_BULK_NULL;
    hg_return_t ret = HG_SUCCESS;

    query_read_obj_name_in_t in_struct;

    hg_op_id_t hg_bulk_op_id;

    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* printf("==PDC_SERVER[x]: received query_read_obj_name_client bulk rpc from %d, with %d regions\n", */ 
    /*         in_struct.origin, in_struct.cnt); */
    /* fflush(stdout); */
    origin_bulk_handle = in_struct.bulk_handle;
    bulk_args->client_seq_id = in_struct.client_seq_id;
    bulk_args->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt     = in_struct.cnt;
    bulk_args->origin  = in_struct.origin;

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE, 
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, query_read_obj_name_client_bulk_cb,
                           bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
                           local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    /* printf("==PDC_SERVER[x]: Pulled data from %d\n", in_struct.origin); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret);
}

// Client receives bulk transfer rpc request, start transfer, then
// process the bulk data of all storage meta for a previous request
static hg_return_t
send_client_storage_meta_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int cnt, i;
    uint64_t *obj_id_ptr;
    void *buf = NULL, *buf_cp = NULL;
    process_bulk_storage_meta_args_t *process_args = NULL;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        ret = HG_PROTOCOL_ERROR;
        goto done;
    }
    else {

        ret = HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READ_ONLY, 1, &buf, NULL, NULL);
        if (ret != HG_SUCCESS) {
            printf("==PDC_CLIENT[x]: %s - Error with bulk access\n", __func__);
            goto done;
        }
        buf_cp= malloc(bulk_args->nbytes);
        memcpy(buf_cp, buf, bulk_args->nbytes);

        process_args = (process_bulk_storage_meta_args_t*)calloc(sizeof(process_bulk_storage_meta_args_t), 1);
        process_args->origin_id = bulk_args->origin;
        process_args->n_storage_meta = bulk_args->cnt;
        process_args->seq_id = *((int*)buf_cp);
        process_args->all_storage_meta = (region_storage_meta_t*)(buf_cp + sizeof(int));

        /* printf("==PDC_CLIENT[x]: %s - received %d storage meta\n", __func__, bulk_args->cnt); */
        /* fflush(stdout); */
    } // end of else

    // Need to free buf_cp later
    PDC_Client_recv_bulk_storage_meta(process_args);

done:
    /* Free bulk handle */
    HG_Bulk_free(local_bulk_handle);
    HG_Destroy(bulk_args->handle);

    free(bulk_args);

    return ret;
}

// Client receives bulk transfer request
/* send_client_storage_meta_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_client_storage_meta_rpc, handle)
{
    const struct hg_info *hg_info = NULL;
    hg_bulk_t origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    struct bulk_args_t *bulk_args = NULL;
    bulk_rpc_in_t in_struct;
    hg_return_t ret = HG_SUCCESS;
    pdc_int_ret_t out_struct;
    hg_op_id_t hg_bulk_op_id;
    int cnt;

    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    bulk_args->origin = in_struct.origin;

    /* Get parameters */
    cnt = in_struct.cnt;
    origin_bulk_handle = in_struct.bulk_handle;
    bulk_args->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt     = cnt;

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *) &bulk_args->nbytes, HG_BULK_READWRITE, 
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, send_client_storage_meta_bulk_cb,
                           bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
                           local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    HG_Free_input(handle, &in_struct);

    /* Send response back */
    out_struct.ret = 1;
    ret = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    FUNC_LEAVE(ret);
} // End send_client_storage_meta_rpc_cb


HG_TEST_THREAD_CB(server_lookup_client)
HG_TEST_THREAD_CB(gen_obj_id)
HG_TEST_THREAD_CB(gen_cont_id)
HG_TEST_THREAD_CB(cont_add_del_objs_rpc)
HG_TEST_THREAD_CB(query_read_obj_name_rpc)
HG_TEST_THREAD_CB(storage_meta_name_query_rpc)
HG_TEST_THREAD_CB(get_storage_meta_name_query_bulk_result_rpc)
HG_TEST_THREAD_CB(notify_client_multi_io_complete_rpc)
HG_TEST_THREAD_CB(server_checkpoint_rpc)
HG_TEST_THREAD_CB(client_test_connect)
HG_TEST_THREAD_CB(metadata_query)
HG_TEST_THREAD_CB(metadata_delete)
HG_TEST_THREAD_CB(metadata_delete_by_id)
HG_TEST_THREAD_CB(metadata_update)
HG_TEST_THREAD_CB(notify_io_complete)
HG_TEST_THREAD_CB(notify_region_update)
HG_TEST_THREAD_CB(close_server)
HG_TEST_THREAD_CB(region_lock)
HG_TEST_THREAD_CB(query_partial)
HG_TEST_THREAD_CB(data_server_read)
HG_TEST_THREAD_CB(data_server_write)
HG_TEST_THREAD_CB(data_server_read_check)
HG_TEST_THREAD_CB(data_server_write_check)
HG_TEST_THREAD_CB(update_region_loc)
HG_TEST_THREAD_CB(get_metadata_by_id)
/* HG_TEST_THREAD_CB(get_storage_info) */
HG_TEST_THREAD_CB(aggregate_write)
HG_TEST_THREAD_CB(region_release)
HG_TEST_THREAD_CB(metadata_add_tag)
HG_TEST_THREAD_CB(server_lookup_remote_server)
HG_TEST_THREAD_CB(bulk_rpc)
HG_TEST_THREAD_CB(buf_map)
HG_TEST_THREAD_CB(get_remote_metadata)
HG_TEST_THREAD_CB(buf_map_server)
HG_TEST_THREAD_CB(buf_unmap_server)
HG_TEST_THREAD_CB(buf_unmap)
HG_TEST_THREAD_CB(region_map)
HG_TEST_THREAD_CB(region_unmap)
HG_TEST_THREAD_CB(get_reg_lock_status)
HG_TEST_THREAD_CB(query_read_obj_name_client_rpc)
HG_TEST_THREAD_CB(send_client_storage_meta_rpc)

hg_id_t
gen_obj_id_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "gen_obj_id", gen_obj_id_in_t, gen_obj_id_out_t, gen_obj_id_cb);

    FUNC_LEAVE(ret_value);
}


/* query_read_obj_name_rpc_cb(hg_handle_t handle) */
hg_id_t
gen_cont_id_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "gen_cont_id", gen_cont_id_in_t, gen_cont_id_out_t, gen_cont_id_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
server_lookup_client_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "server_lookup_client", server_lookup_client_in_t, server_lookup_client_out_t, server_lookup_client_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
server_lookup_remote_server_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "server_lookup_remote_server", server_lookup_remote_server_in_t, server_lookup_remote_server_out_t, server_lookup_remote_server_cb);

    FUNC_LEAVE(ret_value);
}


hg_id_t
client_test_connect_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "client_test_connect", client_test_connect_in_t, client_test_connect_out_t, client_test_connect_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
notify_io_complete_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "notify_io_complete", notify_io_complete_in_t, notify_io_complete_out_t, notify_io_complete_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
query_read_obj_name_client_rpc_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    FUNC_ENTER(NULL);
    ret_value = MERCURY_REGISTER(hg_class, "query_read_obj_name_client_rpc", query_read_obj_name_in_t, query_read_obj_name_out_t, query_read_obj_name_client_rpc_cb);
    FUNC_LEAVE(ret_value);
}

// Receives the query with one name, return all storage metadata of the corresponding object with bulk transfer
/* storage_meta_name_query_rpc_cb(hg_handle_t handle) */
hg_id_t
notify_region_update_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "notify_region_update", notify_region_update_in_t,  notify_region_update_out_t,  notify_region_update_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
metadata_query_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "metadata_query", metadata_query_in_t, metadata_query_out_t, metadata_query_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
metadata_add_tag_register(hg_class_t *hg_class)
{
    FUNC_ENTER(NULL);

    hg_id_t ret_value;
    ret_value = MERCURY_REGISTER(hg_class, "metadata_add_tag", metadata_add_tag_in_t, metadata_add_tag_out_t, metadata_add_tag_cb);

    FUNC_LEAVE(ret_value);
}


hg_id_t
metadata_update_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "metadata_update", metadata_update_in_t, metadata_update_out_t, metadata_update_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
metadata_delete_by_id_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "metadata_delete_by_id", metadata_delete_by_id_in_t, metadata_delete_by_id_out_t, metadata_delete_by_id_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
metadata_delete_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "metadata_delete", metadata_delete_in_t, metadata_delete_out_t, metadata_delete_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
close_server_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "close_server", close_server_in_t, close_server_out_t, close_server_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
buf_map_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "buf_map", buf_map_in_t, buf_map_out_t, buf_map_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
get_remote_metadata_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "get_remote_metadata", get_remote_metadata_in_t, get_remote_metadata_out_t, get_remote_metadata_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
buf_map_server_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "buf_map_server", buf_map_in_t, buf_map_out_t, buf_map_server_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
buf_unmap_server_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "buf_unmap_server", buf_unmap_in_t, buf_unmap_out_t, buf_unmap_server_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
reg_map_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "region_map", reg_map_in_t, reg_map_out_t, region_map_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
buf_unmap_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "buf_unmap", buf_unmap_in_t, buf_unmap_out_t, buf_unmap_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
reg_unmap_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "region_unmap", reg_unmap_in_t, reg_unmap_out_t, region_unmap_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
region_lock_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "region_lock", region_lock_in_t, region_lock_out_t, region_lock_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
region_release_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "region_release", region_lock_in_t, region_lock_out_t, region_release_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
query_partial_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "query_partial", metadata_query_transfer_in_t, metadata_query_transfer_out_t, query_partial_cb);

    FUNC_LEAVE(ret_value);
}


/* hg_id_t */
/* get_storage_info_register(hg_class_t *hg_class) */
/* { */
/*     hg_id_t ret_value; */
    
/*     FUNC_ENTER(NULL); */

/*     // TODO: multiple storage region register */
/*     ret_value = MERCURY_REGISTER(hg_class, "get_storage_info", get_storage_info_in_t, */ 
/*                                 get_storage_info_single_out_t, get_storage_info_cb); */

/*     FUNC_LEAVE(ret_value); */
/* } */

hg_id_t
bulk_rpc_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);
    ret_value = MERCURY_REGISTER(hg_class, "bulk_rpc", bulk_rpc_in_t, bulk_rpc_out_t, bulk_rpc_cb);
    FUNC_LEAVE(ret_value);
}

hg_id_t
data_server_read_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "data_server_read", data_server_read_in_t, data_server_read_out_t, data_server_read_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
data_server_write_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "data_server_write", data_server_write_in_t, data_server_write_out_t, data_server_write_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
data_server_read_check_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "data_server_read_check", data_server_read_check_in_t, data_server_read_check_out_t, data_server_read_check_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
data_server_write_check_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "data_server_write_check", data_server_write_check_in_t, data_server_write_check_out_t, data_server_write_check_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
update_region_loc_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "update_region_loc", update_region_loc_in_t, 
                                 update_region_loc_out_t, update_region_loc_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
get_reg_lock_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "get_reg_lock_status", get_reg_lock_status_in_t,
                                 get_reg_lock_status_out_t, get_reg_lock_status_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
get_metadata_by_id_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "get_metadata_by_id", get_metadata_by_id_in_t, 
                                 get_metadata_by_id_out_t, get_metadata_by_id_cb);

    FUNC_LEAVE(ret_value);
}


hg_id_t
server_checkpoing_rpc_register(hg_class_t *hg_class)
{
    return  MERCURY_REGISTER(hg_class, "server_checkpoing_rpc_register", pdc_int_send_t, pdc_int_ret_t, server_checkpoint_rpc_cb);
}

hg_id_t
cont_add_del_objs_rpc_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "cont_add_del_objs_rpc", cont_add_del_objs_rpc_in_t, cont_add_del_objs_rpc_out_t, cont_add_del_objs_rpc_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
cont_add_tags_rpc_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "cont_add_tags_rpc", cont_add_tags_rpc_in_t, pdc_int_ret_t, pdc_check_int_ret_cb);

    FUNC_LEAVE(ret_value);
}


hg_id_t
query_read_obj_name_rpc_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "query_read_obj_name_rpc", query_read_obj_name_in_t, query_read_obj_name_out_t, query_read_obj_name_rpc_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
storage_meta_name_query_rpc_register(hg_class_t *hg_class)
{
    return MERCURY_REGISTER(hg_class, "storage_meta_name_query_rpc", storage_meta_name_query_in_t, pdc_int_ret_t, storage_meta_name_query_rpc_cb);
}

hg_id_t
get_storage_meta_name_query_bulk_result_rpc_register(hg_class_t *hg_class)
{
    return  MERCURY_REGISTER(hg_class, "get_storage_meta_name_query_bulk_result_rpc", bulk_rpc_in_t, pdc_int_ret_t, get_storage_meta_name_query_bulk_result_rpc_cb);
}
hg_id_t
notify_client_multi_io_complete_rpc_register(hg_class_t *hg_class)
{
    return  MERCURY_REGISTER(hg_class, "notify_client_multi_io_complete_rpc_register", bulk_rpc_in_t, pdc_int_ret_t, notify_client_multi_io_complete_rpc_cb);
}

hg_id_t
send_client_storage_meta_rpc_register(hg_class_t *hg_class)
{
    return  MERCURY_REGISTER(hg_class, "send_client_storage_meta_rpc_register", bulk_rpc_in_t, pdc_int_ret_t, send_client_storage_meta_rpc_cb);
}

/* For 1D boxes (intervals) we have: */
/* box1 = (xmin1, xmax1) */
/* box2 = (xmin2, xmax2) */
/* overlapping1D(box1,box2) = xmax1 >= xmin2 and xmax2 >= xmin1 */

/* For 2D boxes (rectangles) we have: */
/* box1 = (x:(xmin1,xmax1),y:(ymin1,ymax1)) */
/* box2 = (x:(xmin2,xmax2),y:(ymin2,ymax2)) */
/* overlapping2D(box1,box2) = overlapping1D(box1.x, box2.x) and */ 
/*                            overlapping1D(box1.y, box2.y) */

/* For 3D boxes we have: */
/* box1 = (x:(xmin1,xmax1),y:(ymin1,ymax1),z:(zmin1,zmax1)) */
/* box2 = (x:(xmin2,xmax2),y:(ymin2,ymax2),z:(zmin2,zmax2)) */
/* overlapping3D(box1,box2) = overlapping1D(box1.x, box2.x) and */ 
/*                            overlapping1D(box1.y, box2.y) and */
/*                            overlapping1D(box1.z, box2.z) */
 
/*
 * Check if two 1D segments overlaps
 *
 * \param  xmin1[IN]        Start offset of first segment
 * \param  xmax1[IN]        End offset of first segment
 * \param  xmin2[IN]        Start offset of second segment
 * \param  xmax2[IN]        End offset of second segment
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_overlap_1D(uint64_t xmin1, uint64_t xmax1, uint64_t xmin2, uint64_t xmax2)
{
    int ret_value = -1;
    
    if (xmax1 >= xmin2 && xmax2 >= xmin1) {
        ret_value = 1;
    }

    return ret_value;
}

/*
 * Check if two 2D box overlaps
 *
 * \param  xmin1[IN]        Start offset (x-axis) of first  box
 * \param  xmax1[IN]        End   offset (x-axis) of first  box
 * \param  ymin1[IN]        Start offset (y-axis) of first  box
 * \param  ymax1[IN]        End   offset (y-axis) of first  box
 * \param  xmin2[IN]        Start offset (x-axis) of second box
 * \param  xmax2[IN]        End   offset (x-axis) of second box
 * \param  ymin2[IN]        Start offset (y-axis) of second box
 * \param  ymax2[IN]        End   offset (y-axis) of second box
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_overlap_2D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, 
                         uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2)
{
    int ret_value = -1;
    
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2 ) == 1 &&                              
        is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1) {
        ret_value = 1;
    }

    return ret_value;
}

/*
 * Check if two 3D box overlaps
 *
 * \param  xmin1[IN]        Start offset (x-axis) of first  box
 * \param  xmax1[IN]        End   offset (x-axis) of first  box
 * \param  ymin1[IN]        Start offset (y-axis) of first  box
 * \param  ymax1[IN]        End   offset (y-axis) of first  box
 * \param  zmin2[IN]        Start offset (z-axis) of first  box
 * \param  zmax2[IN]        End   offset (z-axis) of first  box
 * \param  xmin2[IN]        Start offset (x-axis) of second box
 * \param  xmax2[IN]        End   offset (x-axis) of second box
 * \param  ymin2[IN]        Start offset (y-axis) of second box
 * \param  ymax2[IN]        End   offset (y-axis) of second box
 * \param  zmin2[IN]        Start offset (z-axis) of second box
 * \param  zmax2[IN]        End   offset (z-axis) of second box
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_overlap_3D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t zmin1, uint64_t zmax1,
                         uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2, uint64_t zmin2, uint64_t zmax2)
{
    int ret_value = -1;
    
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && 
        is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1 && 
        is_overlap_1D(zmin1, zmax1, zmin2, zmax2) == 1 ) 
    {
        ret_value = 1;
    }

    return ret_value;
}

/* static int is_overlap_4D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t zmin1, uint64_t zmax1, */
/*                          uint64_t mmin1, uint64_t mmax1, */
/*                          uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2, uint64_t zmin2, uint64_t zmax2, */
/*                          uint64_t mmin2, uint64_t mmax2 ) */
/* { */
/*     int ret_value = -1; */
    
/*     /1* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { *1/ */
/*     if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && */ 
/*         is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1 && */ 
/*         is_overlap_1D(zmin1, zmax1, zmin2, zmax2) == 1 && */ 
/*         is_overlap_1D(mmin1, mmax1, mmin2, mmax2) == 1 ) */ 
/*     { */
/*         ret_value = 1; */
/*     } */

/*     return ret_value; */
/* } */

/*
 * Check if two regions overlap
 *
 * \param  a[IN]     Pointer to first region
 * \param  b[IN]     Pointer to second region
 *
 * \return 1 if they overlap/-1 otherwise
 */
int is_contiguous_region_overlap(region_list_t *a, region_list_t *b)
{
    int ret_value = 1;
    
    if (a == NULL || b == NULL) {
        printf("==PDC_SERVER: is_contiguous_region_overlap() - passed NULL value!\n");
        ret_value = -1;
        goto done;
    }

    /* printf("==PDC_SERVER: is_contiguous_region_overlap adim=%d, bdim=%d\n", a->ndim, b->ndim); */
    if (a->ndim != b->ndim || a->ndim <= 0 || b->ndim <= 0) {
        ret_value = -1;
        goto done;
    }

    uint64_t xmin1 = 0, xmin2 = 0, xmax1 = 0, xmax2 = 0;
    uint64_t ymin1 = 0, ymin2 = 0, ymax1 = 0, ymax2 = 0;
    uint64_t zmin1 = 0, zmin2 = 0, zmax1 = 0, zmax2 = 0;
    /* uint64_t mmin1 = 0, mmin2 = 0, mmax1 = 0, mmax2 = 0; */

    if (a->ndim >= 1) {
        xmin1 = a->start[0];
        xmax1 = a->start[0] + a->count[0] - 1;
        xmin2 = b->start[0];
        xmax2 = b->start[0] + b->count[0] - 1;
        /* printf("xmin1, xmax1, xmin2, xmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", xmin1, xmax1, xmin2, xmax2); */
    }
    if (a->ndim >= 2) {
        ymin1 = a->start[1];
        ymax1 = a->start[1] + a->count[1] - 1;
        ymin2 = b->start[1];
        ymax2 = b->start[1] + b->count[1] - 1;
        /* printf("ymin1, ymax1, ymin2, ymax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", ymin1, ymax1, ymin2, ymax2); */
    }
    if (a->ndim >= 3) {
        zmin1 = a->start[2];
        zmax1 = a->start[2] + a->count[2] - 1;
        zmin2 = b->start[2];
        zmax2 = b->start[2] + b->count[2] - 1;
        /* printf("zmin1, zmax1, zmin2, zmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", zmin1, zmax1, zmin2, zmax2); */
    }
    /* if (a->ndim >= 4) { */
    /*     mmin1 = a->start[3]; */
    /*     mmax1 = a->start[3] + a->count[3] - 1; */
    /*     mmin2 = b->start[3]; */
    /*     mmax2 = b->start[3] + b->count[3] - 1; */
    /* } */
 
    if (a->ndim == 1) {
        ret_value = is_overlap_1D(xmin1, xmax1, xmin2, xmax2);
    }
    else if (a->ndim == 2) {
        ret_value = is_overlap_2D(xmin1, xmax1, ymin1, ymax1, xmin2, xmax2, ymin2, ymax2);
    }
    else if (a->ndim == 3) {
        ret_value = is_overlap_3D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2);
    }
    /* else if (a->ndim == 4) { */
    /*     ret_value = is_overlap_4D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, mmin1, mmax1, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2, mmin2, mmax2); */
    /* } */

done:
    /* printf("is overlap: %d\n", ret_value); */
    FUNC_LEAVE(ret_value);
}
/*
 * Check if two regions overlap
 *
 * \param  ndim[IN]        Dimension of the two region
 * \param  a_start[IN]     Start offsets of the the first region
 * \param  a_count[IN]     Size of the the first region
 * \param  b_start[IN]     Start offsets of the the second region
 * \param  b_count[IN]     Size of the the second region
 *
 * \return 1 if they overlap/-1 otherwise
 */
int is_contiguous_start_count_overlap(uint32_t ndim, uint64_t *a_start, uint64_t *a_count, uint64_t *b_start, uint64_t *b_count)
{
    int ret_value = 1;
    
    if (ndim > DIM_MAX || NULL == a_start || NULL == a_count ||NULL == b_start ||NULL == b_count) {
        printf("is_contiguous_start_count_overlap: invalid input !\n");
        ret_value = -1;
        goto done;
    }

    uint64_t xmin1 = 0, xmin2 = 0, xmax1 = 0, xmax2 = 0;
    uint64_t ymin1 = 0, ymin2 = 0, ymax1 = 0, ymax2 = 0;
    uint64_t zmin1 = 0, zmin2 = 0, zmax1 = 0, zmax2 = 0;
    /* uint64_t mmin1 = 0, mmin2 = 0, mmax1 = 0, mmax2 = 0; */

    if (ndim >= 1) {
        xmin1 = a_start[0];
        xmax1 = a_start[0] + a_count[0] - 1;
        xmin2 = b_start[0];
        xmax2 = b_start[0] + b_count[0] - 1;
        /* printf("xmin1, xmax1, xmin2, xmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", xmin1, xmax1, xmin2, xmax2); */
    }
    if (ndim >= 2) {
        ymin1 = a_start[1];
        ymax1 = a_start[1] + a_count[1] - 1;
        ymin2 = b_start[1];
        ymax2 = b_start[1] + b_count[1] - 1;
        /* printf("ymin1, ymax1, ymin2, ymax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", ymin1, ymax1, ymin2, ymax2); */
    }
    if (ndim >= 3) {
        zmin1 = a_start[2];
        zmax1 = a_start[2] + a_count[2] - 1;
        zmin2 = b_start[2];
        zmax2 = b_start[2] + b_count[2] - 1;
        /* printf("zmin1, zmax1, zmin2, zmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", zmin1, zmax1, zmin2, zmax2); */
    }
    /* if (ndim >= 4) { */
    /*     mmin1 = a_start[3]; */
    /*     mmax1 = a_start[3] + a_count[3] - 1; */
    /*     mmin2 = b_start[3]; */
    /*     mmax2 = b_start[3] + b_count[3] - 1; */
    /* } */
 
    if (ndim == 1) 
        ret_value = is_overlap_1D(xmin1, xmax1, xmin2, xmax2);
    else if (ndim == 2) 
        ret_value = is_overlap_2D(xmin1, xmax1, ymin1, ymax1, 
                                  xmin2, xmax2, ymin2, ymax2);
    else if (ndim == 3) 
        ret_value = is_overlap_3D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, 
                                  xmin2, xmax2, ymin2, ymax2, zmin2, zmax2);
    /* else if (ndim == 4) */ 
    /*     ret_value = is_overlap_4D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, mmin1, mmax1, */ 
    /*                               xmin2, xmax2, ymin2, ymax2, zmin2, zmax2, mmin2, mmax2); */

done:
    FUNC_LEAVE(ret_value);
}



/*
 * Get the overlapping region's information of two regions
 *
 * \param  ndim[IN]             Dimension of the two region
 * \param  start_a[IN]           Start offsets of the the first region
 * \param  count_a[IN]           Sizes of the the first region
 * \param  start_b[IN]           Start offsets of the the second region
 * \param  count_b[IN]           Sizes of the the second region
 * \param  overlap_start[IN]    Start offsets of the the overlapping region
 * \param  overlap_size[IN]     Sizes of the the overlapping region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t get_overlap_start_count(uint32_t ndim, uint64_t *start_a, uint64_t *count_a, 
                                                     uint64_t *start_b, uint64_t *count_b, 
                                       uint64_t *overlap_start, uint64_t *overlap_count)
{
    perr_t ret_value = SUCCEED;
    uint64_t i;

    if (NULL == start_a || NULL == count_a || NULL == start_b || NULL == count_b || 
            NULL == overlap_start || NULL == overlap_count) {

        printf("get_overlap NULL input!\n");
        ret_value = FAIL;
        return ret_value;
    }
    
    // Check if they are truly overlapping regions
    if (is_contiguous_start_count_overlap(ndim, start_a, count_a, start_b, count_b) != 1) {
        printf("== %s: non-overlap regions!\n", __func__);
        for (i = 0; i < ndim; i++) {
            printf("\t\tdim%" PRIu64 " - start_a: %" PRIu64 " count_a: %" PRIu64 ", "
                   "\t\tstart_b:%" PRIu64 " count_b:%" PRIu64 "\n", 
                    i, start_a[i], count_a[i], start_b[i], count_b[i]);
        }
        ret_value = FAIL;
        goto done;
    }

    for (i = 0; i < ndim; i++) {
        overlap_start[i] = PDC_MAX(start_a[i], start_b[i]);
        /* end = max(xmax2, xmax1); */
        overlap_count[i] = PDC_MIN(start_a[i]+count_a[i], start_b[i]+count_b[i]) - overlap_start[i];
    }

done:
    if (ret_value == FAIL) {
        for (i = 0; i < ndim; i++) {
            overlap_start[i] = 0;
            overlap_count[i] = 0;
        }
    }
    return ret_value;
}

/*
 * Create a shm segment 
 *
 * \param  size[IN]            Total size of the shared memroy segment
 * \param  shm_fd[OUT]         Shared memory file handle
 * \param  buf[OUT]            Shared memory mapped buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_create_shm_segment_ind(uint64_t size, int *shm_fd, void **buf)
{
    perr_t ret_value = SUCCEED;
    size_t i = 0;
    int retry;
    char shm_addr[ADDR_MAX];

    FUNC_ENTER(NULL);

    if (shm_addr == 0) {
        printf("== %s - Shared memory addr is NULL!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    /* create the shared memory segment as if it was a file */
    retry = 0;
    srand(time(0));
    while (retry < PDC_MAX_TRIAL_NUM) {
        snprintf(shm_addr, ADDR_MAX, "/PDCshm%d", rand());
        *shm_fd = shm_open(shm_addr, O_CREAT | O_RDWR, 0666);
        if (*shm_fd != -1) 
            break;
        retry++;
    }

    if (*shm_fd == -1) {
        printf("== %s - Shared memory create failed\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    /* configure the size of the shared memory segment */
    ftruncate(*shm_fd, size);

    /* map the shared memory segment to the address space of the process */
    *buf = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, *shm_fd, 0);
    if (*buf == MAP_FAILED) {
        printf("== %s - Shared memory mmap failed\n", __func__);
        // close and shm_unlink?
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_create_shm_segment_ind


/*
 * Create a shm segment and attach to the input region pointer
 *
 * \param  region[IN/OUT]       Request region, with shm_fd as the newly created shm segment
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_create_shm_segment(region_list_t *region)
{
    perr_t ret_value = SUCCEED;
    size_t i = 0;
    int retry;

    FUNC_ENTER(NULL);

    if (region->shm_addr[0] == 0) {
        printf("== %s - Shared memory addr is NULL!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    /* create the shared memory segment as if it was a file */
    retry = 0;
    while (retry < PDC_MAX_TRIAL_NUM) {
        region->shm_fd = shm_open(region->shm_addr, O_CREAT | O_RDWR, 0666);
        if (region->shm_fd != -1) 
            break;
        retry++;
    }

    if (region->shm_fd == -1) {
        printf("== %s - Shared memory create failed\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    // Calculate the actual size for reading the data if needed 
    if (region->data_size == 0) {
        region->data_size = region->count[0];
        for (i = 1; i < region->ndim; i++)
            region->data_size *= region->count[i];
    }

    /* configure the size of the shared memory segment */
    ftruncate(region->shm_fd, region->data_size);

    /* map the shared memory segment to the address space of the process */
    region->buf = mmap(0, region->data_size, PROT_READ | PROT_WRITE, MAP_SHARED, region->shm_fd, 0);
    if (region->buf == MAP_FAILED) {
        printf("== %s - Shared memory mmap failed\n", __func__);
        // close and shm_unlink?
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_create_shm_segment


#include "pdc_analysis_common.c"
#include "pdc_transforms_common.c"
