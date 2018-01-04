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

#include "config.h"
#include "../server/utlist.h"

#include "mercury.h"
#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"

#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"
#include "../server/pdc_server.h"
#include "pdc_malloc.h"
#include <inttypes.h>

// Thread
hg_thread_pool_t *hg_test_thread_pool_g = NULL;

uint64_t pdc_id_seq_g = PDC_SERVER_ID_INTERVEL;
// actual value for each server is set by PDC_Server_init()
//

#ifdef ENABLE_MULTITHREAD

extern struct hg_thread_work *
hg_core_get_thread_work(hg_handle_t handle);

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
            printf("HG_TEST_THREAD_CB func_name %s!\n", #func_name);\
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
    if (hash < 0)
        hash *= -1;

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

inline int PDC_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b)
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
    a->data_ptr           = NULL;
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

    a->ndim          = 0;
    memset(a->start,  0, sizeof(uint64_t)*DIM_MAX);
    memset(a->count,  0, sizeof(uint64_t)*DIM_MAX);
    /* memset(a->stride, 0, sizeof(uint64_t)*DIM_MAX); */
    memset(a->client_ids, 0, sizeof(uint32_t)*PDC_SERVER_MAX_PROC_PER_NODE);

    a->data_size     = 0;
    a->is_data_ready = 0;
    memset(a->shm_addr, 0, sizeof(char)*ADDR_MAX);
    a->shm_fd        = -1;
    a->buf           = NULL;
    a->data_loc_type = NONE;
    memset(a->storage_location, 0, sizeof(char)*ADDR_MAX);
    a->overlap_storage_regions = NULL;
    a->n_overlap_storage_region = 0;
    a->offset        = 0;
    hg_atomic_init32(&(a->buf_map_refcount), 0);
    a->reg_dirty     = 0;
    a->access_type   = NA;
    a->bulk_handle   = NULL;
    a->addr          = 0;
    a->meta          = NULL;
    a->obj_id        = 0;
    a->reg_id        = 0;
    a->from_obj_id   = 0;
    a->client_id     = 0;

    a->prev          = NULL;
    a->next          = NULL;
    // Init 25 attributes, double check to match the region_list_t def
    return ret_value;
}

// TODO: currently assumes both region are of same object, so only compare ndim, start, and count.
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
    int ret_value = 1;
    size_t i = 0;

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
    printf("   dirty: %d\n", a->reg_dirty);
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

perr_t pdc_region_list_t_deep_cp(region_list_t *from, region_list_t *to)
{
    if (NULL == from || NULL == to) {
        printf("pdc_region_list_t_deep_cp(): NULL input!\n");
        return FAIL;
    }

    if (from->ndim > 4 || from->ndim <= 0) {
        printf("pdc_region_list_t_deep_cp(): ndim %u ERROR!\n", from->ndim);
        return FAIL;
    }

    to->ndim = from->ndim;
    memcpy(to->start, from->start, sizeof(uint64_t)*from->ndim);
    memcpy(to->count, from->count, sizeof(uint64_t)*from->ndim);
    /* memcpy(to->stride, from->stride, sizeof(uint64_t)*from->ndim); */

    memcpy(to->client_ids, from->client_ids, sizeof(uint32_t)*PDC_SERVER_MAX_PROC_PER_NODE);

    to->data_size     = from->data_size;
    to->is_data_ready = from->is_data_ready;
    memcpy(to->shm_addr, from->shm_addr, sizeof(char) * ADDR_MAX);
    to->shm_fd        = from->shm_fd;
    to->buf           = from->buf;
    to->data_loc_type = from->data_loc_type;
    strcpy(to->storage_location, from->storage_location);
    to->overlap_storage_regions  = from->overlap_storage_regions;
    to->n_overlap_storage_region = from->n_overlap_storage_region;
    to->offset        = from->offset;
    to->buf_map_refcount = from->buf_map_refcount;
    to->reg_dirty     = from->reg_dirty;
    to->access_type   = from->access_type;
    to->bulk_handle   = from->bulk_handle;
    to->addr          = from->addr;
    to->obj_id        = from->obj_id;
    to->reg_id        = from->reg_id;
    to->from_obj_id   = from->from_obj_id;
    to->client_id     = from->client_id;

    to->meta          = from->meta;

    to->prev = from->prev;
    to->next = from->next;
    // Copy 23 attributes, double check to match the region_list_t def
    return SUCCEED;
}

perr_t pdc_region_transfer_t_to_list_t(region_info_transfer_t *transfer, region_list_t *region)
{
    if (NULL==region || NULL==transfer ) {
        printf("    pdc_region_info_t_to_region_list_t(): NULL input!\n");
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
        printf("    pdc_region_info_t_to_region_list_t(): NULL input!\n");
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
        printf("    pdc_region_info_t_to_region_list_t(): NULL input!\n");
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
        printf("    pdc_region_info_t_to_region_list_t(): NULL input!\n");
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
    
}

perr_t pdc_region_list_t_to_transfer(region_list_t *region, region_info_transfer_t *transfer)
{
    if (NULL==region || NULL==transfer ) {
        printf("    pdc_region_info_t_to_region_list_t(): NULL input!\n");
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
hg_return_t PDC_Server_get_client_addr(const struct hg_cb_info *callback_info) {return SUCCEED;}
perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out, void *data_ptr) {return SUCCEED;}
/* perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out) {return SUCCEED;} */
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out) {return SUCCEED;}
perr_t PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts, pdc_metadata_t** out) {return SUCCEED;}
perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out) {return SUCCEED;}
perr_t PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out) {return SUCCEED;}
perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out) {return SUCCEED;}
perr_t PDC_Server_add_tag_metadata(metadata_add_tag_in_t *in, metadata_add_tag_out_t *out) {return SUCCEED;}
perr_t PDC_Server_region_release(region_lock_in_t *in, region_lock_out_t *out) {return SUCCEED;}
perr_t PDC_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out) {return SUCCEED;}
//perr_t PDC_Server_region_lock_status(pdcid_t obj_id, region_info_transfer_t *region, int *lock_status) {return SUCCEED;}
perr_t PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status) {return SUCCEED;}
perr_t PDC_Server_local_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status) {return SUCCEED;}
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs) {return SUCCEED;}
perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id) {return NULL;}
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

hg_return_t PDC_Server_s2s_recv_work_done_cb(const struct hg_cb_info *callback_info) {return SUCCEED;}
perr_t PDC_Server_set_close() {return SUCCEED;}

hg_class_t *hg_class_g;

/* 
 * Data server related
 */
hg_return_t PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info) {return HG_SUCCESS;}
perr_t PDC_Server_read_check(data_server_read_check_in_t *in, data_server_read_check_out_t *out) {return SUCCEED;}
perr_t PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out) {return SUCCEED;}
hg_return_t PDC_Server_s2s_work_done_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;}

#else
hg_return_t PDC_Client_work_done_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;};
hg_return_t PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info) {return HG_SUCCESS;};

#endif

static pbool_t region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2)
{
    pbool_t ret_value = 0;

    FUNC_ENTER(NULL);

    if(reg1.ndim != reg2.ndim)
        PGOTO_DONE(ret_value);
    if (reg1.ndim >= 1) {
        if(reg1.count_0 != reg2.count_0 || reg1.start_0 != reg2.start_0)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 2) {
        if(reg1.count_1 != reg2.count_1 || reg1.start_1 != reg2.start_1)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 3) {
        if(reg1.count_2 != reg2.count_2 || reg1.start_2 != reg2.start_2)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 4) {
        if(reg1.count_3 != reg2.count_3 || reg1.start_3 != reg2.start_3)
            PGOTO_DONE(ret_value);
    }
    ret_value = 1;
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
/* static hg_return_t */
/* gen_obj_id_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(gen_obj_id, handle)
{
    perr_t ret_value = HG_SUCCESS;
    PDC_var_type_t data_type;
    void *data_ptr = NULL;
    size_t ndim;
    size_t unit;
    FUNC_ENTER(NULL);

    // Decode input
    gen_obj_id_in_t in;
    gen_obj_id_out_t out;

    HG_Get_input(handle, &in);

    // allocate memory for global object on server
    data_type = in.data_type;   //in.data.data_type
    if(data_type == PDC_DOUBLE)
        unit = sizeof(double);
    else if(data_type == PDC_FLOAT) 
        unit = sizeof(float);
    else if(data_type == PDC_INT)
        unit = sizeof(int);
    else 
        PGOTO_ERROR(HG_OTHER_ERROR, "===PDC Server: object data type is not supported yet");

    ndim = in.data.ndim;
    if(ndim == 1)
        data_ptr = (void *)malloc(in.data.dims0 * unit);    
    else if(ndim == 2)
        data_ptr = (void *)malloc(in.data.dims0 * in.data.dims1 * unit);
    else if(ndim == 3)
        data_ptr = (void *)malloc(in.data.dims0 * in.data.dims1 * in.data.dims2 * unit);
    else if(ndim == 4)
        data_ptr = (void *)malloc(in.data.dims0 * in.data.dims1 * in.data.dims2 * in.data.dims3 * unit);
    else 
        PGOTO_ERROR(HG_OTHER_ERROR, "===PDC Server: object dim is not supported");

    if(data_ptr == NULL)
        PGOTO_ERROR(HG_OTHER_ERROR, "===PDC Server: object memory allocation failed");

    // Insert to hash table
    ret_value = insert_metadata_to_hash_table(&in, &out, data_ptr);

    /* printf("==PDC_SERVER: gen_obj_id_cb(): going to return %" PRIu64 "\n", out.obj_id); */
    /* fflush(stdout); */

done:
    HG_Respond(handle, NULL, NULL, &out);

    /* printf("==PDC_SERVER: gen_obj_id_cb(): After HG_Respond%\n"); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

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

    args->client_id = in.client_id;
    args->nclient   = in.nclient;
    sprintf(args->client_addr, in.client_addr);

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
    close_server_out_t out;
    
    FUNC_ENTER(NULL);

    HG_Get_input(handle, &in);

    PDC_Server_set_close();

    out.ret = 1;
    HG_Respond(handle, NULL, NULL, &out);

    /* printf("\n==PDC_SERVER: Respond back to close server request\n"); */
    /* fflush(stdout); */

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

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

//enter this function, transfer is done, data is in server
static hg_return_t
buf_map_region_release_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    hg_handle_t handle;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
    struct buf_map_release_bulk_args *bulk_args = NULL;
    struct PDC_region_info *remote_reg_info = NULL;

    FUNC_ENTER(NULL);
    
    bulk_args = (struct buf_map_release_bulk_args *)hg_cb_info->arg;
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
//void *data_buf = bulk_args->data_buf;
/*
printf("address is %lld\n", data_buf);
printf("first data is %d\n", *(int *)(data_buf+24));
printf("next is %d\n", *(int *)(data_buf+28));
printf("next is %d\n", *(int *)(data_buf+40));
printf("next is %d\n", *(int *)(data_buf+44));
printf("next is %d\n", *(int *)(data_buf+56));
printf("next is %d\n", *(int *)(data_buf+60));
fflush(stdout);
*/
/*
printf("first data is %f\n", *(float *)(data_buf));
printf("next is %f\n", *(float *)(data_buf+4));
printf("next is %f\n", *(float *)(data_buf+8));
printf("next is %f\n", *(float *)(data_buf+12));
fflush(stdout);
*/
    // Perform lock release function
    PDC_Server_region_release(&(bulk_args->in), &out);
    HG_Respond(bulk_args->handle, NULL, NULL, &out);
  
    // Send notification to mapped regions, when data transfer is done
//    PDC_SERVER_notify_region_update_to_client(bulk_args->remote_obj_id, bulk_args->remote_reg_id, bulk_args->remote_client_id);

    remote_reg_info = (struct PDC_region_info *)malloc(sizeof(struct PDC_region_info));
    if(remote_reg_info == NULL) {
        printf("remote_reg_info memory allocation failed\n");
        fflush(stdout);
    }
//    pdc_region_transfer_t_to_region_info(bulk_args->remote_region, remote_reg_info);
    remote_reg_info->ndim = (bulk_args->remote_region).ndim;
    remote_reg_info->offset = (uint64_t *)malloc(sizeof(uint64_t));
    remote_reg_info->size = (uint64_t *)malloc(sizeof(uint64_t));
    (remote_reg_info->offset)[0] = (bulk_args->remote_region).start_0;
    (remote_reg_info->size)[0] = (bulk_args->remote_region).count_0;
    PDC_Server_data_write_direct(bulk_args->remote_obj_id, remote_reg_info, bulk_args->data_buf+(bulk_args->remote_region).start_0);

    HG_Destroy(bulk_args->handle);
    free(bulk_args);

done:
    FUNC_LEAVE(hg_ret);
}

//enter this function, transfer is done, data is pushed to mapping region
static hg_return_t
region_update_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    hg_handle_t handle;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
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
}

//enter this function, transfer is done, data is ready in data server
static hg_return_t
region_release_bulk_transfer_cb (const struct hg_cb_info *hg_cb_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    perr_t ret_value; 
    region_lock_out_t out;
    hg_handle_t handle = HG_BULK_NULL;
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
    PDC_Server_region_release(&(bulk_args->in), &out);
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
    free(bulk_args->server_region->size);
    free(bulk_args->server_region->offset);
    free(bulk_args->server_region);

//done:
//    out.ret = 1;
//    HG_Respond(bulk_args->handle, NULL, NULL, &out);
    /* printf("==PDC_SERVER: region_release_bulk_transfer_cb(): returned %" PRIu64 "\n", out.ret); */

    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    
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
    perr_t ret_value;
    region_lock_out_t out;
    hg_handle_t handle;
    const struct hg_info *hg_info = NULL;
    struct region_lock_update_bulk_args *bulk_args = NULL;

    bulk_args = (struct region_lock_update_bulk_args *)hg_cb_info->arg;
    handle = bulk_args->handle;

    // Perform lock releae function
    PDC_Server_region_release(&(bulk_args->in), &out);
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
    struct lock_bulk_args *bulk_args = NULL;
    pdc_metadata_t *target_obj;
    int i, j, ret;
    int error = 0;
    int found = 0;
    int dirty_reg = 0;
    hg_size_t   size;
    hg_uint32_t count;
    hg_op_id_t hg_bulk_op_id;
    hg_return_t hg_ret;
    void       *data_buf;
    void      **from_data_ptrs;
    void       *data_ptrs;
    hg_size_t  *data_size;
    struct PDC_region_info *server_region;
    pdc_metadata_t *res_meta;
    region_list_t *elt, *request_region, *tmp;
    struct region_lock_update_bulk_args *lock_update_bulk_args = NULL;
    struct buf_map_release_bulk_args *buf_map_bulk_args = NULL;
    hg_bulk_t origin_bulk_handle = HG_BULK_NULL; 
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t lock_local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t remote_bulk_handle = HG_BULK_NULL;
    region_map_t *map_elt;
    region_buf_map_t *eltt;
    hg_uint32_t remote_count;
    void **data_ptrs_to;
    size_t *data_size_to;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    /* Get info from handle */
    hg_info = HG_Get_info(handle);

//  printf("release %lld\n", in.obj_id);
    if(in.access_type==READ || in.mapping==0) {
        // check region is dirty or not, if dirty transfer data
//        if(in.lock_op == PDC_LOCK_OP_RELEASE) {
            request_region = (region_list_t *)malloc(sizeof(region_list_t));
            pdc_region_transfer_t_to_list_t(&in.region, request_region);
            PDC_Server_get_local_metadata_by_id(in.obj_id, &res_meta);
            DL_FOREACH(res_meta->region_lock_head, elt) {
                if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty == 1 && hg_atomic_get32(&(elt->buf_map_refcount)) == 0) {
//                    printf("==PDC SERVER: region_release start %" PRIu64 " \n", request_region->start[0]);
                    // printf("detect lock release dirty region\n");
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
// printf("read data %f, %f from obj %lld, with size %lu\n", *(float *)data_buf, *((float*)data_buf+1), elt->from_obj_id, size);
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
                if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty == 1 && hg_atomic_get32(&(elt->buf_map_refcount)) > 0) {
                    dirty_reg = 1;
                    tmp = (region_list_t *)malloc(sizeof(region_list_t));
                    DL_FOREACH(res_meta->region_buf_map_head, eltt) {
                        pdc_region_transfer_t_to_list_t(&(eltt->remote_region_unit), tmp);
                        if(PDC_is_same_region_list(tmp, request_region) == 1) {
                            // get remote object memory addr
                            data_buf = res_meta->data_ptr;
                            if(res_meta->ndim == 1) {
                                remote_count = 1;
                                data_ptrs_to = (void **)malloc( sizeof(void *) );
                                data_size_to = (size_t *)malloc( sizeof(size_t) );
                                *data_ptrs_to = data_buf + (eltt->remote_region_unit).start_0;
                                *data_size_to = (eltt->remote_region_unit).count_0;
                            }
                            else if(res_meta->ndim == 2) {
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
                            else if(res_meta->ndim == 3) {
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
                            }
                            /* Create a new block handle to read the data */
                            hg_ret = HG_Bulk_create(hg_info->hg_class, remote_count, data_ptrs_to, (hg_size_t *)data_size_to, HG_BULK_READWRITE, &remote_bulk_handle);
                            if (hg_ret != HG_SUCCESS) {
                                error = 1;
                                PGOTO_ERROR(FAIL, "==PDC SERVER ERROR: Could not create bulk data handle\n");
                            }
                            
                            buf_map_bulk_args = (struct buf_map_release_bulk_args *) malloc(sizeof(struct buf_map_release_bulk_args));
                            buf_map_bulk_args->handle = handle;
                            buf_map_bulk_args->data_buf = data_buf;
                            buf_map_bulk_args->in = in;
                            buf_map_bulk_args->remote_obj_id = eltt->remote_obj_id;
                            buf_map_bulk_args->remote_reg_id = eltt->remote_reg_id;
                            buf_map_bulk_args->remote_region = eltt->remote_region_unit;
                            buf_map_bulk_args->remote_client_id = eltt->remote_client_id;
                             
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
                }
            }
            free(request_region);
//        }
        if(dirty_reg == 0) {
            // TODO
            // Perform lock release function
            PDC_Server_region_release(&in, &out);

            HG_Respond(handle, NULL, NULL, &out);  // move to transfer_cb
            /* printf("==PDC_SERVER: region_lock_cb(): returned %" PRIu64 "\n", out.ret); */

            HG_Free_input(handle, &in);
            HG_Destroy(handle);   // move to transfer_cb
        }
    }
    // write lock release with mapping case
    // do data tranfer if it is write lock release with mapping. 
    else {
        bulk_args = (struct lock_bulk_args *) malloc(sizeof(struct lock_bulk_args));
        /* Keep handle to pass to callback */
        bulk_args->handle = handle;
        bulk_args->in = in;

        target_obj = PDC_Server_get_obj_metadata(in.obj_id);
        if (target_obj == NULL) {
            error = 1;
            printf("==PDC_SERVER: PDC_Server_region_release - requested object (id=%" PRIu64 ") does not exist\n", in.obj_id);
            goto done;
        }
        found = 0;
        if(target_obj->region_map_head != NULL) {
            DL_FOREACH(target_obj->region_map_head, map_elt) {
//                if(map_elt->local_obj_id == in.obj_id && map_elt->local_reg_id==in.local_reg_id) {
                  if(map_elt->local_obj_id == in.obj_id && region_is_identical(in.region, map_elt->local_region)) {
                    found = 1;
                    origin_bulk_handle = map_elt->local_bulk_handle;
                    local_bulk_handle = HG_BULK_NULL;

                    // copy data from client to server
                    // allocate contiguous space in the server or RDMA later
/*
                    count = HG_Bulk_get_segment_count(origin_bulk_handle);
                    from_data_ptrs = (void **)malloc( count * sizeof(void *) );
                    data_size = (hg_size_t *)malloc( count * sizeof(hg_size_t) );
                    HG_Bulk_access(origin_bulk_handle, 0, size, HG_BULK_READWRITE, count, from_data_ptrs, data_size, NULL); 
*/
                    size = HG_Bulk_get_size(origin_bulk_handle);
                    data_ptrs = (void *)malloc(size);
                    //HG_Bulk_access(origin_bulk_handle, 0, size, HG_BULK_READWRITE, count, from_data_ptrs, data_size, NULL);
                    /* Create a new block handle to read the data */
                    hg_ret = HG_Bulk_create(hg_info->hg_class, 1, &data_ptrs, &size, HG_BULK_READWRITE, &local_bulk_handle);
                    if (hg_ret != HG_SUCCESS) {
                        error = 1;
                        printf("==PDC SERVER ERROR: Could not create bulk data handle\n");
                    }
                    bulk_args->data_buf = data_ptrs;
// TODO: free in transfer callback
//                    free(data_ptrs);

                    // TODO: free
                    server_region = (struct PDC_region_info *)malloc(sizeof(struct PDC_region_info));
                    if(!server_region)
                        PGOTO_ERROR(FAIL,"server_region memory allocation failed\n");
/*
                    server_region->ndim = map_elt->local_ndim;
                    server_region->size = (uint64_t *)malloc(server_region->ndim * sizeof(uint64_t));
                    server_region->offset = (uint64_t *)malloc(server_region->ndim * sizeof(uint64_t));
                    if(map_elt->local_ndim >= 1) {
                        (server_region->offset)[0] = 0;
                        (server_region->size)[0] = in.region.count_0;
                    }
                    if(map_elt->local_ndim >= 2) {
                        (server_region->offset)[1] = 0;
                        (server_region->size)[1] = in.region.count_1; 
                    }
                    if(map_elt->local_ndim >= 3) {
                        (server_region->offset)[2] = 0;
                        (server_region->size)[2] = in.region.count_2;
                    }
                    if(map_elt->local_ndim >= 4) {
                        (server_region->offset)[3] = 0;
                        (server_region->size)[3] = in.region.count_3;
                    }
*/
                    server_region->ndim = 1;
                    server_region->size = (uint64_t *)malloc(sizeof(uint64_t));
                    server_region->offset = (uint64_t *)malloc(sizeof(uint64_t));
                    (server_region->size)[0] = size;
                    (server_region->offset)[0] = in.region.start_0; 
                    bulk_args->server_region = server_region;
                    bulk_args->mapping_list = map_elt;
                    bulk_args->addr = map_elt->local_addr;
                    /* Pull bulk data */
                    hg_ret = HG_Bulk_transfer(hg_info->context, region_release_bulk_transfer_cb, bulk_args, HG_BULK_PULL, map_elt->local_addr, origin_bulk_handle, 0, local_bulk_handle, 0, size, &hg_bulk_op_id);
                    if (hg_ret != HG_SUCCESS) {
                        error = 1;
                        printf("==PDC SERVER ERROR: Could not read bulk data\n");  
                    }
                }
            }
        }
        if(found == 0) {
            error = 1;
            printf("==PDC SERVER ERROR: Could not find local region %" PRIu64 " in server\n", in.local_reg_id);
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
    region_lock_in_t in;
    region_lock_out_t out;
    const struct hg_info *hg_info = NULL;
    struct lock_bulk_args *bulk_args = NULL;

    FUNC_ENTER(NULL);

    HG_Get_input(handle, &in);

    // Perform lock function
    PDC_Server_region_lock(&in, &out);

    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// obj_unmap_cb(hg_handle_t handle)
HG_TEST_RPC_CB(obj_unmap, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    obj_unmap_in_t in;
    obj_unmap_out_t out;
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
        if(in.local_obj_id==elt->local_obj_id) {
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
    buf_unmap_in_t in;
    buf_unmap_out_t out;
    pdc_metadata_t *target_obj;
    int found;
    const struct hg_info *info;
    region_buf_map_t *tmp, *elt;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    info = HG_Get_info(handle);
    out.ret = 0;

    target_obj = PDC_Server_get_obj_metadata(in.remote_obj_id);
    if (target_obj == NULL) {
        printf("==PDC_SERVER: HG_TEST_RPC_CB(buf_unmap, handle) - requested object (id=%" PRIu64 ") does not exist\n", in.remote_obj_id);
        out.ret = 0;
        goto done;
    }

    DL_FOREACH_SAFE(target_obj->region_buf_map_head, elt, tmp) {
        if(in.remote_obj_id==elt->remote_obj_id && region_is_identical(in.remote_region, elt->remote_region_unit)) {
            HG_Bulk_free(elt->local_bulk_handle);
            DL_DELETE(target_obj->region_buf_map_head, elt);
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
// buf_map_cb(hg_handle_t handle)
HG_TEST_RPC_CB(buf_map, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    buf_map_in_t in;
    buf_map_out_t out;
    pdc_metadata_t *target_obj;
    int found;
    const struct hg_info *info;
    region_list_t *elt, *request_region;
    region_buf_map_t *buf_map_ptr, *eltt;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);

    target_obj = PDC_Server_get_obj_metadata(in.remote_obj_id);
    if (target_obj == NULL) {
        printf("==PDC_SERVER: HG_TEST_RPC_CB(buf_map, handle) - requested object (id=%" PRIu64 ") does not exist\n", in.remote_obj_id);
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
    info = HG_Get_info(handle);
    HG_Addr_dup(info->hg_class, info->addr, &(buf_map_ptr->local_addr));
    HG_Bulk_ref_incr(in.local_bulk_handle);
    buf_map_ptr->local_bulk_handle = in.local_bulk_handle;

    buf_map_ptr->remote_obj_id = in.remote_obj_id;
    buf_map_ptr->remote_reg_id = in.remote_reg_id;
    buf_map_ptr->remote_client_id = in.remote_client_id;
    buf_map_ptr->remote_ndim = in.ndim;
    buf_map_ptr->remote_unit = in.remote_unit;
    buf_map_ptr->remote_region_unit = in.remote_region_unit;
    buf_map_ptr->remote_region_nounit = in.remote_region_nounit;

    DL_APPEND(target_obj->region_buf_map_head, buf_map_ptr);
    out.ret = 1;

done:
    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

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

// TODO
static hg_return_t
update_storage_meta_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    struct bulk_args_t *bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    hg_return_t ret = HG_SUCCESS;
    int cnt, i;
    uint64_t *obj_id_ptr;
    update_region_storage_meta_bulk_t *data_ptr;
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
    ret = HG_Respond(bulk_args->handle, PDC_Server_s2s_recv_work_done_cb, NULL, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    printf("==PDC_SERVER[ ]: Responded to client %d of storage meta update!\n", bulk_args->origin);
    fflush(stdout);

    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    return ret;
}

/* static hg_return_t */
/* bulk_rpc_cb(hg_handle_t handle) */
// Server execute after receives the bulk_rpc from another server
HG_TEST_RPC_CB(bulk_rpc, handle)
{
//TODO
    const struct hg_info *hg_info = NULL;
    hg_bulk_t origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    struct bulk_args_t *bulk_args = NULL;
    bulk_rpc_in_t in_struct;
    hg_return_t ret = HG_SUCCESS;
    int cnt;
    void **buf_ptrs = NULL;

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

    printf("==PDC_SERVER[x]: received update storage meta bulk rpc from %d, with %d regions\n", 
            in_struct.origin, cnt);
    fflush(stdout);

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

    printf("==PDC_SERVER[x]: Pulled data from %d\n", in_struct.origin);
    fflush(stdout);

    HG_Free_input(handle, &in_struct);

done:
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

done:
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

    // Decode input
    HG_Get_input(handle, &in);
    /* printf("==PDC_SERVER: Got data server write request from client %d\n", in.client_id); */
    /* fflush(stdout); */

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

done:
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

done:
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

    // TODO: After returning the last write check finish to client, start the storage meta bulk update process
    ret_value = HG_Respond(handle, PDC_Server_count_write_check_update_storage_meta_cb, out, out);
    /* printf("==PDC_SERVER: server write_check returning ret=%d\n", out->ret); */

done:
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
    fflush(stdout);

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
    HG_Respond(handle, PDC_Server_s2s_recv_work_done_cb, NULL, &out);

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
    get_reg_lock_status_out_t out;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
//    printf("==PDC_SERVER: Got metadata retrieval: obj_id=%" PRIu64 "\n", in.obj_id);

}

/* get_metadata_by_id_cb */
HG_TEST_RPC_CB(get_metadata_by_id, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    get_metadata_by_id_in_t  in;
    get_metadata_by_id_out_t out;
    pdc_metadata_t *target;
    
    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);
    printf("==PDC_SERVER: Got metadata retrieval: obj_id=%" PRIu64 "\n", in.obj_id);

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


// Replace the 0s in the buf so that Mercury can transfer the entire buf
perr_t PDC_replace_char_fill_values(signed char *buf, uint32_t buf_size)
{
    perr_t ret_value = SUCCEED;
    uint32_t zero_cnt, i;
    signed char fill_value;
    FUNC_ENTER(NULL);

    fill_value = buf[buf_size-1];
    zero_cnt = 0;

    for (i = 0; i < buf_size; i++) {
        if (buf[i] == fill_value) {
            buf[i] = 0;
            zero_cnt++;
        }
        else if (buf[i] == 0) {
            printf("==ERROR! PDC_replace_char_fill_values 0 exist at %d!\n", i);
            ret_value = FAIL;
            goto done;
        }
    }

    /* printf("==PDC_replace_char_fill_values replaced %d char fill values %d!\n", zero_cnt, (int)fill_value); */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


// Replace the 0s in the buf so that Mercury can transfer the entire buf
perr_t PDC_replace_zero_chars(signed char *buf, uint32_t buf_size)
{
    perr_t ret_value = SUCCEED;
    uint32_t zero_cnt, i, j, fill_value_valid = 0;
    signed char fill_value;
    char num_cnt[257] = {0};
    FUNC_ENTER(NULL);

    memset(num_cnt, 0, 256);

    for (i = 0; i < buf_size; i++) {
        num_cnt[buf[i]+128]++;
    }

    for (i = 0; i < 256; i++) {
        if (num_cnt[i] == 0) {
            fill_value = i - 128;
            fill_value_valid = 1;
            break;
        }
    }

    if (fill_value_valid == 0) {
        printf("==PDC_replace_zero_chars cannot find a char value not used by buffer!\n");
        ret_value = FAIL;
        goto done;
    }

    zero_cnt = 0;
    for (i = 0; i < buf_size; i++) {
        if (buf[i] == fill_value) {
            printf("==PDC_replace_zero_chars CHAR_FILL_VALUE exist at %d!\n", i);
            ret_value = FAIL;
            goto done;
        }
        else if (buf[i] == 0) {
            buf[i] = fill_value;
            zero_cnt++;
        }
    }

    // Last char is fill value
    buf[buf_size-1] = fill_value;

    /* printf("==PDC_replace_zero_chars: replaced %d zero values with %d.\n", zero_cnt, (int)fill_value); */

done:
    FUNC_LEAVE(ret_value);
}

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
perr_t PDC_serialize_regions_lists(region_list_t** regions, uint32_t n_region, void *buf, uint32_t buf_size)
{
    perr_t ret_value = SUCCEED;
    uint32_t i, j;
    uint32_t ndim, loc_len, total_len;
    uint32_t *uint32_ptr = NULL;
    uint64_t *uint64_ptr = NULL;
    signed char *char_ptr   = NULL;
    int       zero_cnt   = 0;

    FUNC_ENTER(NULL);

    if (regions == NULL || regions[0] == NULL) {
        printf("==PDC_SERVER: PDC_serialize_regions_lists NULL input!\n");
        ret_value = FAIL;
        goto done;
    }

    total_len = 0;

    ndim = regions[0]->ndim;

    // serialize format:
    // n_region|ndim|start00|count00|...|start0n|count0n|loc_len|loc_str|offset|...

    uint32_ptr  = (uint32_t*)buf;
    *uint32_ptr = n_region;

    /* printf("==PDC_SERVER: serializing %u regions!\n", n_region); */

    uint32_ptr++;
    total_len += sizeof(uint32_t);

    *uint32_ptr = ndim;

    uint32_ptr++;
    total_len += sizeof(uint32_t);

    uint64_ptr = (uint64_t*)uint32_ptr;

    for (i = 0; i < n_region; i++) {
        if (regions[i] == NULL) {
            printf("==PDC_serialize_regions_lists NULL input!\n");
            ret_value = FAIL;
            goto done;
        }
        for (j = 0; j < ndim; j++) {
            *uint64_ptr = regions[i]->start[j];
            uint64_ptr++;
            total_len += sizeof(uint64_t);
            *uint64_ptr = regions[i]->count[j];
            uint64_ptr++;
            total_len += sizeof(uint64_t);
        }

        loc_len = strlen(regions[i]->storage_location);
        uint32_ptr  = (uint32_t*)uint64_ptr;
        *uint32_ptr = loc_len;
        uint32_ptr++;
        total_len += sizeof(uint32_t);

        char_ptr = (signed char*)uint32_ptr;
        if (loc_len <= 0) {
            printf("==PDC_serialize_regions_lists invalid storage location [%s]!\n",
                                regions[i]->storage_location);
            ret_value = FAIL;
            goto done;
        }
        strcpy((char*)char_ptr, regions[i]->storage_location);
        char_ptr[loc_len] = PDC_STR_DELIM;  // Delim to replace 0
        char_ptr += (loc_len + 1);
        total_len += (loc_len + 1);

        uint64_ptr = (uint64_t*)char_ptr;

        *uint64_ptr = regions[i]->offset;
        uint64_ptr++;
        total_len += sizeof(uint64_t);

        if (total_len > buf_size) {
            printf("==PDC_serialize_regions_lists total_len %u exceeds "
                    "buf_len %u\n", total_len, buf_size);
            ret_value = FAIL;
            goto done;
        }
        /* PDC_print_region_list(regions[i]); */
    }

    if (PDC_replace_zero_chars((char*)buf, buf_size) != SUCCEED) {
        printf("==PDC_serialize_regions_lists PDC_replace_zero_chars ERROR! ");
        ret_value = FAIL;
        goto done;
    }

    /* if (is_debug_g == 1) { */
    /*     printf("==PDC_serialize_regions_lists n_region: %u, buf len is %u \n", */
    /*             n_region, total_len); */
    /*     uint32_t nr_region; */

    /*     region_list_t **r_regions = (region_list_t**)malloc(sizeof(region_list_t*) * PDC_MAX_OVERLAP_REGION_NUM     ); */
    /*     for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++) { */
    /*         r_regions[i] = (region_list_t*)calloc(1, sizeof(region_list_t)); */
    /*     } */

    /*     PDC_unserialize_region_lists(buf, r_regions, &nr_region); */

        /* printf("==PDC_serialize_regions_lists after unserialize %d\n", nr_region); */
    /*     for (i = 0; i < nr_region; i++) { */
    /*         PDC_print_region_list(r_regions[i]); */
    /*     } */
    /* } */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_serialize_regions_lists

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
perr_t PDC_unserialize_region_lists(void *buf, region_list_t** regions, uint32_t *n_region)
{
    perr_t ret_value = SUCCEED;
    uint32_t i, j;
    uint32_t ndim, loc_len, buf_size;
    uint32_t *uint32_ptr = NULL;
    uint64_t *uint64_ptr = NULL;
    signed char *char_ptr   = NULL;

    FUNC_ENTER(NULL);

    if (buf == NULL || regions == NULL || n_region == NULL) {
        printf("==PDC_SERVER: PDC_unserialize_region_lists NULL input!\n");
        ret_value = FAIL;
        goto done;
    }

    char_ptr = (signed char*)buf;
    buf_size = strlen((char*)char_ptr);

    ret_value = PDC_replace_char_fill_values(char_ptr, buf_size);
    if (ret_value != SUCCEED) {
        printf("==PDC_unserialize_region_lists replace_char_fill_values ERROR!\n");
        goto done;
    }

    // n_region|ndim|start00|count00|...|start0n|count0n|loc_len|loc_str|offset|...

    uint32_ptr = (uint32_t*)buf;
    *n_region = *uint32_ptr;

    /* printf("==PDC_unserialize_region_lists: %u regions!\n", *n_region); */

    uint32_ptr++;
    ndim = *uint32_ptr;

    if (ndim == 0 || ndim > 3) {
        printf("==PDC_unserialize_region_lists ndim %lu ERROR!\n", ndim);
        ret_value = FAIL;
        goto done;
    }

    uint32_ptr++;
    uint64_ptr = (uint64_t*)uint32_ptr;

    /* if (is_debug_g == 1) { */
    /*     printf("==unserialize_regions_info n_region: %u, ndim: %u \n", */
    /*             pdc_server_rank_g, *n_region, ndim); */
    /* } */

    for (i = 0; i < *n_region; i++) {
        if (regions[i] == NULL) {
            printf("==PDC_unserialize_region_lists NULL input,"
                    " try increade PDC_MAX_OVERLAP_REGION_NUM value!\n");
            ret_value = FAIL;
            goto done;
        }
        regions[i]->ndim = ndim;

        for (j = 0; j < ndim; j++) {
            regions[i]->start[j] = *uint64_ptr;
            uint64_ptr++;
            regions[i]->count[j] = *uint64_ptr;
            uint64_ptr++;
        }

        /* printf("==unserialize_regions_info start[0]: %" PRIu64 ", count[0]: %" PRIu64 "\n", */
        /*         regions[i]->start[0], regions[i]->count[0]); */

        uint32_ptr = (uint32_t*)uint64_ptr;
        loc_len = *uint32_ptr;

        uint32_ptr++;

        char_ptr = (signed char*)uint32_ptr;
        // Verify delimiter
        if (char_ptr[loc_len] != PDC_STR_DELIM) {
            printf("==PDC_unserialize_region_lists delim error!\n");
            ret_value = FAIL;
            goto done;
        }

        strncpy(regions[i]->storage_location, (char*)char_ptr, loc_len);
        // Add end of string
        regions[i]->storage_location[loc_len] = 0;

        char_ptr += (loc_len + 1);

        uint64_ptr = (uint64_t*)char_ptr;
        regions[i]->offset = *uint64_ptr;

        uint64_ptr++;
        // n_region|ndim|start00|count00|...|start0n|count0n|loc_len|loc_str|offset|...
        //
        /* printf("==PDC_SERVER: unserialize_region_list\n"); */
        /* PDC_print_region_list(regions[i]); */
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_unserialize_region_lists

/*
 * Calculate the total string length of the regions to be serialized
 *
 * \param  regions[IN]       List of region info that are un-serialized
 * \param  n_region[IN]      Number of regions in the list
 * \param  len[OUT]          Length of the serialized string
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_get_serialized_size(region_list_t** regions, uint32_t n_region, uint32_t *len)
{
    perr_t ret_value = SUCCEED;
    uint32_t i;

    FUNC_ENTER(NULL);

    if (regions == NULL || n_region == 0 || len == NULL || regions[0] == NULL) {
        printf("==PDC_SERVER: PDC_get_serialized_size NULL input!\n");
        ret_value = FAIL;
        goto done;
    }

    *len = 0;
    for (i = 0; i < n_region; i++) {
        if (regions[i] == NULL) {
            printf("==PDC_SERVER: PDC_get_serialized_size NULL input in regions!\n");
            ret_value = FAIL;
            goto done;
        }
        *len += (strlen(regions[i]->storage_location) + 1);
    }

            // n_region | ndim | start00 | count00 | ... | startndim0 | countndim0 | loc_len | loc |
            // delim | offset | char_fill_value | \0
     *len += ( sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t)*regions[0]->ndim*2*n_region +
               sizeof(uint32_t)*n_region + sizeof(uint64_t)*n_region + 2);

done:
    FUNC_LEAVE(ret_value);
}

/* get_storage_info_cb */
HG_TEST_RPC_CB(get_storage_info, handle)
{
    perr_t                        ret_value = HG_SUCCESS;
    get_storage_info_in_t         in;
    pdc_serialized_data_t         serialized_out;
    get_storage_info_single_out_t single_region_out;
    pdc_metadata_t               *target = NULL;
    region_list_t                 request_region;
    region_list_t                 **result_regions;

    uint32_t n_region, i;
    uint32_t serialize_len, buf_len;
    void *buf = NULL;
    signed char *char_ptr = NULL;

    FUNC_ENTER(NULL);

    // Decode input
    HG_Get_input(handle, &in);

    /* printf("==PDC_SERVER: Got storage info request from other server: obj_id=%" PRIu64 "\n", in.obj_id); */

    PDC_init_region_list(&request_region);
    pdc_region_transfer_t_to_list_t(&in.req_region, &request_region);

    result_regions = (region_list_t**)calloc(1, sizeof(region_list_t*)*PDC_MAX_OVERLAP_REGION_NUM);
    for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++)
        result_regions[i] = (region_list_t*)malloc(sizeof(region_list_t));

    if (PDC_Server_get_local_storage_location_of_region(in.obj_id, &request_region, &n_region, result_regions) !=      SUCCEED) {
        printf("==PDC_SERVER: unable to get_local_storage_location_of_region\n");
        ret_value = FAIL;
        goto done;
    }
    else {

        if (n_region == 1) {
            // If there is only one matching region, send it with the easy way throught Mercury
            pdc_region_list_t_to_transfer(result_regions[0], &single_region_out.region_transfer);
            single_region_out.storage_loc = result_regions[0]->storage_location;
            single_region_out.file_offset = result_regions[0]->offset;
            HG_Respond(handle, PDC_Server_s2s_recv_work_done_cb, NULL, &single_region_out);
        }
        else {
            // If there are multiple regions, serialze them to a 1D buf, replace the 0s
            // and send as a string.
            // FIXME: with large scale, i.e. more than 128 servers, this approach don't work correctly

            /* if (PDC_get_serialized_size(result_regions, n_region, &serialize_len) != SUCCEED) { */
            /*     printf("==PDC_SERVER: fail to get_total_str_len\n"); */
            /*     ret_value = FAIL; */
            /*     goto done; */
            /* } */

            /* buf = (void*)calloc(1, serialize_len); */
            /* if (PDC_serialize_regions_lists(result_regions, n_region, buf, serialize_len) != SUCCEED) { */
            /*     printf("==PDC_SERVER: unable to serialize_regions_info\n"); */
            /*     ret_value = FAIL; */
            /*     goto done; */
            /* } */
            /* serialized_out.buf = buf; */

            /* // Check if 0 still exists in buffer */
            /* char_ptr = (signed char*)serialized_out.buf; */
            /* buf_len = strlen(char_ptr); */
            /* for (i = 0; i < serialize_len; i++) { */
            /*     if (char_ptr[i] == 0) { */
            /*         printf("==PDC_SERVER:ERROR with serialize buf, 0 detected in string, %u!\n", i); */
            /*         fflush(stdout); */
            /*     } */
            /* } */

            // debug print
            /* printf("==PDC_SERVER: serialize_region allocated %u, real %u \n", serialize_len, buf_len); */
            /* fflush(stdout); */
            /* HG_Respond(handle, PDC_Server_s2s_recv_work_done_cb, NULL, &serialized_out); */

            printf("==PDC_SERVER: cannot handle more than 1 storage region match!\n");
            fflush(stdout);
            ret_value = FAIL;
            HG_Respond(handle, PDC_Server_s2s_recv_work_done_cb, NULL, &single_region_out);
            goto done;
        } // end of else (n_region is not 1)
    } // end of else

done:
    if (result_regions != NULL) {
        for (i = 0; i < PDC_MAX_OVERLAP_REGION_NUM; i++) {
            if (result_regions[i] != NULL)
                free(result_regions[i]);
        }
        free(result_regions);
    }

    if (buf != NULL)
        free(buf);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* aggregate_write_cb */
HG_TEST_RPC_CB(aggregate_write, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    pdc_aggregated_io_to_server_t in;
    pdc_int_ret_t                 out;

    pdc_metadata_t *target = NULL;
    region_list_t request_region;
    region_list_t **result_regions;
    uint32_t n_region, i;
    uint32_t serialize_len, buf_len;
    void *buf = NULL;
    signed char *char_ptr = NULL;
    
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

done:
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


HG_TEST_THREAD_CB(server_lookup_client)
HG_TEST_THREAD_CB(gen_obj_id)
/* HG_TEST_THREAD_CB(send_obj_name_marker) */
HG_TEST_THREAD_CB(client_test_connect)
HG_TEST_THREAD_CB(metadata_query)
HG_TEST_THREAD_CB(metadata_delete)
HG_TEST_THREAD_CB(metadata_delete_by_id)
HG_TEST_THREAD_CB(metadata_update)
HG_TEST_THREAD_CB(notify_io_complete)
HG_TEST_THREAD_CB(notify_region_update)
HG_TEST_THREAD_CB(close_server)
HG_TEST_THREAD_CB(reg_map)
HG_TEST_THREAD_CB(reg_unmap)
HG_TEST_THREAD_CB(obj_unmap)
HG_TEST_THREAD_CB(region_lock)
HG_TEST_THREAD_CB(query_partial)
HG_TEST_THREAD_CB(data_server_read)
HG_TEST_THREAD_CB(data_server_write)
HG_TEST_THREAD_CB(data_server_read_check)
HG_TEST_THREAD_CB(data_server_write_check)
HG_TEST_THREAD_CB(update_region_loc)
HG_TEST_THREAD_CB(get_metadata_by_id)
HG_TEST_THREAD_CB(get_storage_info)
HG_TEST_THREAD_CB(aggregate_write)
HG_TEST_THREAD_CB(region_release)
HG_TEST_THREAD_CB(metadata_add_tag)
HG_TEST_THREAD_CB(server_lookup_remote_server)
HG_TEST_THREAD_CB(bulk_rpc)

hg_id_t
gen_obj_id_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "gen_obj_id", gen_obj_id_in_t, gen_obj_id_out_t, gen_obj_id_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
server_lookup_client_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "server_lookup_client", server_lookup_client_in_t, server_lookup_client_out_t, server_lookup_client_cb);

done:
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

done:
    FUNC_LEAVE(ret_value);
}

hg_id_t
notify_region_update_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "notify_region_update", notify_region_update_in_t,  notify_region_update_out_t,  notify_region_update_cb);

done:
    FUNC_LEAVE(ret_value);
}

/* hg_id_t */
/* send_obj_name_marker_register(hg_class_t *hg_class) */
/* { */
/*     FUNC_ENTER(NULL); */

/*     hg_id_t ret_value; */
/*     ret_value = MERCURY_REGISTER(hg_class, "send_obj_name_marker", send_obj_name_marker_in_t, send_obj_name_marker_out_t, send_obj_name_marker_cb); */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

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

done:
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
obj_unmap_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "obj_unmap", obj_unmap_in_t, obj_unmap_out_t, obj_unmap_cb);

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


hg_id_t
get_storage_info_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;
    
    FUNC_ENTER(NULL);

    // TODO: multiple storage region register
    ret_value = MERCURY_REGISTER(hg_class, "get_storage_info", get_storage_info_in_t, 
                                get_storage_info_single_out_t, get_storage_info_cb);

    FUNC_LEAVE(ret_value);
}

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
