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
#include "pdc_malloc.h"
#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"
#include "pdc_query.h"
#include "pdc_region.h"
#include "pdc_hist_pkg.h"
#include "pdc_utlist.h"
#include "pdc_server.h"
#include "pdc_server_data.h"
#include "pdc_region.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_tf_server.h"
#include "pdc_an_server.h"

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
#include "pdc_timing.h"
#include "pdc_server_region_cache.h"

#ifdef ENABLE_MULTITHREAD
hg_thread_mutex_t insert_metadata_mutex_g = HG_THREAD_MUTEX_INITIALIZER;

// Thread
hg_thread_pool_t *hg_test_thread_pool_g    = NULL;
hg_thread_pool_t *hg_test_thread_pool_fs_g = NULL;
#endif

uint64_t pdc_id_seq_g = PDC_SERVER_ID_INTERVEL;
// actual value for each server is set by PDC_Server_init()

struct timeval last_cache_activity_timeval_g;

/* Defined here (rather than pdc_server.c, where PDC_Server_set_close --
 * this feature's only caller -- actually lives) because this file is
 * compiled into both pdc_server_lib and the client-side pdc library
 * (see both CMakeLists.txt), and PDC_stats_record is called from
 * pdc_tf_server.c/pdc_an_server.c, which are *also* compiled into both.
 * A definition living only in pdc_server.c resolves fine for pdc.so
 * itself (shared libraries tolerate undefined symbols at link time),
 * but any executable that links pdc.so without also linking
 * pdc_server_lib (e.g. close_server) then fails to link with an
 * undefined reference to PDC_stats_record -- confirmed by hand while
 * developing this. Anywhere this file's callers run applies -- the
 * counters just stay all-zero in a pure client process, since nothing
 * there calls PDC_stats_record. */
static const char *pdc_stat_metric_names_g[PDC_STAT_NUM_METRICS] = {
    "compress", "decompress", "magnitude", "posix_open", "posix_close", "posix_write", "posix_read",
};

typedef struct pdc_stat_counter_t {
    uint64_t count;
    double   total_time; /* seconds */
} pdc_stat_counter_t;

/* Not an array of atomics -- see PDC_stats_record's own doc comment
 * (pdc_client_server_common.h) on why that's fine. */
static pdc_stat_counter_t pdc_stats_g[PDC_STAT_NUM_METRICS];

void
PDC_stats_record(pdc_stat_metric_t metric, double elapsed_sec)
{
    if (metric < 0 || metric >= PDC_STAT_NUM_METRICS)
        return;
    pdc_stats_g[metric].count++;
    pdc_stats_g[metric].total_time += elapsed_sec;
}

void
PDC_stats_print_csv(int my_rank, int nranks)
{
    double *  all_avg_time = NULL;
    uint64_t *all_count    = NULL;

    if (my_rank == 0) {
        all_avg_time = (double *)malloc(sizeof(double) * (size_t)nranks);
        all_count    = (uint64_t *)malloc(sizeof(uint64_t) * (size_t)nranks);

        char header[4096];
        int  off = snprintf(header, sizeof(header), "metric");
        for (int r = 0; r < nranks; r++)
            off += snprintf(header + off, sizeof(header) - (size_t)off, ",rank%d_avg_s/n", r);
        LOG_WARNING("%s\n", header);
    }

    for (int m = 0; m < PDC_STAT_NUM_METRICS; m++) {
        double avg_time =
            pdc_stats_g[m].count > 0 ? pdc_stats_g[m].total_time / (double)pdc_stats_g[m].count : 0.0;
        unsigned long long count = (unsigned long long)pdc_stats_g[m].count;

#ifdef ENABLE_MPI
        MPI_Gather(&avg_time, 1, MPI_DOUBLE, all_avg_time, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        MPI_Gather(&count, 1, MPI_UNSIGNED_LONG_LONG, all_count, 1, MPI_UNSIGNED_LONG_LONG, 0,
                   MPI_COMM_WORLD);
#else
        if (all_avg_time != NULL)
            all_avg_time[0] = avg_time;
        if (all_count != NULL)
            all_count[0] = count;
#endif

        if (my_rank == 0) {
            char line[4096];
            int  off = snprintf(line, sizeof(line), "%s", pdc_stat_metric_names_g[m]);
            for (int r = 0; r < nranks; r++)
                off += snprintf(line + off, sizeof(line) - (size_t)off, ",%.6f/%llu", all_avg_time[r],
                                (unsigned long long)all_count[r]);
            LOG_WARNING("%s\n", line);
        }
    }

    if (my_rank == 0) {
        free(all_avg_time);
        free(all_count);
    }
}

#include "pdc_server_region_request_handler.h"

hg_return_t
hg_proc_pdc_query_xfer_t(hg_proc_t proc, void *data)
{
    FUNC_ENTER(NULL);

    hg_return_t       ret;
    pdc_query_xfer_t *struct_data = (pdc_query_xfer_t *)data;

    ret = hg_proc_int32_t(proc, &struct_data->query_id);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->get_op);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->manager);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->n_unique_obj);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->query_op);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->next_server_id);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->prev_server_id);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->n_constraints);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_int32_t(proc, &struct_data->n_combine_ops);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
    if (ret != HG_SUCCESS) {
        FUNC_LEAVE(ret);
    }
    if (struct_data->n_constraints > 0) {
        switch (hg_proc_get_op(proc)) {
            case HG_DECODE:
                struct_data->combine_ops = PDC_malloc(struct_data->n_combine_ops * sizeof(int));
                struct_data->constraints =
                    PDC_malloc(struct_data->n_constraints * sizeof(pdc_query_constraint_t));
                // HG_FALLTHROUGH();
                /* FALLTHRU */
            case HG_ENCODE:
                ret = hg_proc_raw(proc, struct_data->combine_ops, struct_data->n_combine_ops * sizeof(int));
                ret = hg_proc_raw(proc, struct_data->constraints,
                                  struct_data->n_constraints * sizeof(pdc_query_constraint_t));
                break;
            case HG_FREE:
                // FIXME:: need to free struct_data->combine_ops & struct_data->constraints
            default:
                break;
        }
    }

    FUNC_LEAVE(ret);
}

static hg_return_t
hg_proc_send_shm_in_t(hg_proc_t proc, void *data)
{
    FUNC_ENTER(NULL);

    hg_return_t    ret_value   = HG_SUCCESS;
    send_shm_in_t *struct_data = (send_shm_in_t *)data;

    ret_value = hg_proc_uint32_t(proc, &struct_data->client_id);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Proc error");

    ret_value = hg_proc_hg_string_t(proc, &struct_data->shm_addr);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Proc error");

    ret_value = hg_proc_uint64_t(proc, &struct_data->size);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Proc error");

done:
    FUNC_LEAVE(ret_value);
}

double
PDC_get_elapsed_time_double(struct timeval *tstart, struct timeval *tend)
{
    FUNC_ENTER(NULL);

    double ret_value = 0;

    ret_value =
        (double)(((tend->tv_sec - tstart->tv_sec) * 1000000LL + tend->tv_usec - tstart->tv_usec) / 1000000.0);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_get_self_addr(hg_class_t *hg_class, char *self_addr_string)
{
    FUNC_ENTER(NULL);

    perr_t    ret_value = SUCCEED;
    hg_addr_t self_addr;
    hg_size_t self_addr_string_size = ADDR_MAX;

    // Get self addr to tell client about
    HG_Addr_self(hg_class, &self_addr);
    HG_Addr_to_string(hg_class, self_addr_string, &self_addr_string_size, self_addr);
    HG_Addr_free(hg_class, self_addr);

    FUNC_LEAVE(ret_value);
}

uint32_t
PDC_get_local_server_id(int my_rank, int n_client_per_server, int n_server)
{
    FUNC_ENTER(NULL);

    uint32_t ret_value = 0;
    ret_value          = (my_rank / n_client_per_server) % n_server;

    FUNC_LEAVE(ret_value);
}

uint32_t
PDC_get_server_by_obj_id(uint64_t obj_id, int n_server)
{
    FUNC_ENTER(NULL);

    // TODO: need a smart way to deal with server number change
    uint32_t ret_value = 0;

    ret_value = (uint32_t)(obj_id / PDC_SERVER_ID_INTERVEL) - 1;
    ret_value %= n_server;

    FUNC_LEAVE(ret_value);
}

int
PDC_get_var_type_size(pdc_var_type_t dtype)
{
    FUNC_ENTER(NULL);

    int ret_value = 0;

    /* TODO: How to determine the size of compound types and or
     * the other enumerated types currently handled by the default
     * case which returns 0.
     */
    switch (dtype) {
        case PDC_INT:
            PGOTO_DONE(sizeof(int));
            break;
        case PDC_FLOAT:
            PGOTO_DONE(sizeof(float));
            break;
        case PDC_DOUBLE:
            PGOTO_DONE(sizeof(double));
            break;
        case PDC_CHAR:
            PGOTO_DONE(sizeof(char));
            break;
        case PDC_INT16:
            PGOTO_DONE(sizeof(int16_t));
            break;
        case PDC_INT8:
            PGOTO_DONE(sizeof(int8_t));
            break;
        case PDC_UINT8:
            PGOTO_DONE(sizeof(uint8_t));
            break;
        case PDC_UINT16:
            PGOTO_DONE(sizeof(uint16_t));
            break;
        case PDC_INT32:
            PGOTO_DONE(sizeof(int32_t));
            break;
        case PDC_INT64:
            PGOTO_DONE(sizeof(int64_t));
            break;
        case PDC_UINT64:
            PGOTO_DONE(sizeof(uint64_t));
            break;
        case PDC_UINT:
            PGOTO_DONE(sizeof(uint));
            break;
        case PDC_UNKNOWN:
        default:
            PGOTO_ERROR(
                0,
                "PDC_get_var_type_size: WARNING - Using an unknown datatype"); /* Probably a poor default */
            break;
    }

done:
    FUNC_LEAVE(ret_value);
}

static uint32_t
pdc_hash_djb2(const char *pc)
{
    FUNC_ENTER(NULL);

    uint32_t ret_value = 0;
    uint32_t hash      = 5381, c;

    while ((c = *pc++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    ret_value = hash;

    FUNC_LEAVE(ret_value);
}

int
PDC_msleep(unsigned long milisec)
{
    FUNC_ENTER(NULL);

    int             ret_value = 0;
    struct timespec req       = {0};

    time_t sec  = (int)(milisec / 1000);
    milisec     = milisec - (sec * 1000);
    req.tv_sec  = sec;
    req.tv_nsec = milisec * 1000000L;
    while (nanosleep(&req, &req) == -1)
        continue;
    ret_value = 1;

    FUNC_LEAVE(ret_value);
}

uint32_t
PDC_get_hash_by_name(const char *name)
{
    FUNC_ENTER(NULL);

    uint32_t ret_value = 0;
    ret_value          = pdc_hash_djb2(name);

    FUNC_LEAVE(ret_value);
}

uint32_t
PDC_get_server_by_name(char *name, int n_server)
{
    FUNC_ENTER(NULL);

    // TODO: need a smart way to deal with server number change
    uint32_t ret_value;
    ret_value = PDC_get_hash_by_name(name) % n_server;

    FUNC_LEAVE(ret_value);
}

int
PDC_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b)
{
    FUNC_ENTER(NULL);

    int ret_value = 0;

    // Timestep
    if (a->time_step >= 0 && b->time_step >= 0) {
        ret_value = (a->time_step - b->time_step);
    }
    if (ret_value != 0)
        PGOTO_DONE(ret_value);

    // Object name
    if (a->obj_name[0] != '\0' && b->obj_name[0] != '\0') {
        ret_value = strcmp(a->obj_name, b->obj_name);
    }
    if (ret_value != 0)
        PGOTO_DONE(ret_value);

    // UID
    if (a->user_id > 0 && b->user_id > 0) {
        ret_value = (a->user_id - b->user_id);
    }
    if (ret_value != 0)
        PGOTO_DONE(ret_value);

    // Application name
    if (a->app_name[0] != '\0' && b->app_name[0] != '\0') {
        ret_value = strcmp(a->app_name, b->app_name);
    }

done:
    FUNC_LEAVE(ret_value);
}

void
PDC_mkdir(const char *dir)
{
    FUNC_ENTER(NULL);

    char  tmp[TMP_DIR_STRING_LEN];
    char *p = NULL;

    snprintf(tmp, sizeof(tmp), "%s", dir);

    for (p = tmp + 1; *p; p++)
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU | S_IRWXG);
            *p = '/';
        }

    FUNC_LEAVE_VOID();
}

void
PDC_print_metadata(pdc_metadata_t *a)
{
    FUNC_ENTER(NULL);

    size_t         i;
    region_list_t *elt;

    if (a == NULL)
        PGOTO_ERROR_VOID("Empty metadata structure");

    LOG_JUST_PRINT("================================\n");
    LOG_JUST_PRINT("  data_type = [%d]\n", a->data_type);
    LOG_JUST_PRINT("  obj_id    = %" PRIu64 "\n", a->obj_id);
    LOG_JUST_PRINT("  cont_id   = %" PRIu64 "\n", a->cont_id);
    LOG_JUST_PRINT("  uid       = %d\n", a->user_id);
    LOG_JUST_PRINT("  app_name  = [%s]\n", a->app_name);
    LOG_JUST_PRINT("  obj_name  = [%s]\n", a->obj_name);
    LOG_JUST_PRINT("  obj_loc   = [%s]\n", a->data_location);
    LOG_JUST_PRINT("  time_step = %d\n", a->time_step);
    LOG_JUST_PRINT("  tags      = [%s]\n", a->tags);
    LOG_JUST_PRINT("  ndim      = %lu\n", a->ndim);
    LOG_JUST_PRINT("  dims = %" PRIu64 "", a->dims[0]);
    for (i = 1; i < a->ndim; i++)
        LOG_JUST_PRINT(", %" PRIu64 "", a->dims[i]);

    DL_FOREACH(a->storage_region_list_head, elt)
    PDC_print_region_list(elt);
    LOG_JUST_PRINT("\n================================\n\n");

done:
    FUNC_LEAVE_VOID();
}

perr_t
PDC_metadata_init(pdc_metadata_t *a)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (a == NULL)
        PGOTO_ERROR(FAIL, "Unable to init NULL pdc_metadata_t");

    memset(a, 0, sizeof(pdc_metadata_t));

    a->user_id            = 0;
    a->time_step          = -1;
    a->data_type          = -1;
    a->obj_id             = 0;
    a->cont_id            = 0;
    a->create_time        = 0;
    a->last_modified_time = 0;
    a->ndim               = 0;
    a->data_server_id     = 0;

    memset(a->app_name, 0, sizeof(char) * NAME_MAX);
    memset(a->obj_name, 0, sizeof(char) * NAME_MAX);
    memset(a->tags, 0, sizeof(char) * TAG_LEN_MAX);
    memset(a->data_location, 0, sizeof(char) * ADDR_MAX);
    memset(a->dims, 0, sizeof(uint64_t) * DIM_MAX);

    a->storage_region_list_head = NULL;
    a->region_lock_head         = NULL;
    a->region_map_head          = NULL;
    a->region_buf_map_head      = NULL;
    a->prev                     = NULL;
    a->next                     = NULL;
    a->bloom                    = NULL;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_init_region_list(region_list_t *a)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    memset(a, 0, sizeof(region_list_t));
    a->shm_fd        = -1;
    a->data_loc_type = PDC_NONE;
    hg_atomic_init32(&(a->buf_map_refcount), 0);
    a->reg_dirty_from_buf = 0;
    a->lock_handle        = NULL;
    a->access_type        = PDC_NA;

    FUNC_LEAVE(ret_value);
}

// currently assumes both region are of same object, so only compare ndim, start, and count.
int
PDC_is_same_region_shape(region_list_t *a, size_t extent_a, region_list_t *b, size_t extent_b)
{
    FUNC_ENTER(NULL);

    int    ret_value = 1;
    size_t i         = 0;

    if (NULL == a || NULL == b)
        PGOTO_ERROR(-1, "Empty region_list_t structure");

    if (a->ndim != b->ndim)
        PGOTO_DONE(-1);

    for (i = 0; i < a->ndim; i++) {
        if (a->start[i] != b->start[i])
            PGOTO_DONE(-1);
        if ((a->count[i] / extent_a) != (b->count[i] / extent_b))
            PGOTO_DONE(-1);
    }

done:
    FUNC_LEAVE(ret_value);
}

// currently assumes both region are of same object, so only compare ndim, start, and count.

int
PDC_is_same_region_list(region_list_t *a, region_list_t *b)
{
    FUNC_ENTER(NULL);

    int    ret_value = 1;
    size_t i         = 0;

    if (NULL == a || NULL == b)
        PGOTO_ERROR(FAIL, "Empty region_list_t structure");

    if (a->ndim != b->ndim)
        PGOTO_DONE(-1);

    for (i = 0; i < a->ndim; i++) {
        if (a->start[i] != b->start[i])
            PGOTO_DONE(-1);
        if (a->count[i] != b->count[i])
            PGOTO_DONE(-1);
    }

done:
    FUNC_LEAVE(ret_value);
}

void
PDC_print_storage_region_list(region_list_t *a)
{
    FUNC_ENTER(NULL);

    size_t i;

    if (a == NULL) {
        PGOTO_ERROR_VOID("Empty region_list_t structure");
    }

    if (a->ndim > 4) {
        PGOTO_ERROR_VOID("Error with ndim %lu", a->ndim);
    }

    LOG_JUST_PRINT("================================\n");
    LOG_JUST_PRINT("  ndim      = %lu\n", a->ndim);
    LOG_JUST_PRINT("  start    count\n");

    for (i = 0; i < a->ndim; i++) {
        LOG_JUST_PRINT("  %5" PRIu64 "    %5" PRIu64 "\n", a->start[i], a->count[i]);
    }

    LOG_JUST_PRINT("    path: %s\n", a->storage_location);
    LOG_JUST_PRINT(" buf_map: %d\n", a->buf_map_refcount);
    LOG_JUST_PRINT("   dirty: %d\n", a->reg_dirty_from_buf);
    LOG_JUST_PRINT("  offset: %" PRIu64 "\n", a->offset);

    LOG_JUST_PRINT("================================\n\n");

done:
    FUNC_LEAVE_VOID();
}

void
PDC_print_region_list(region_list_t *a)
{
    FUNC_ENTER(NULL);

    size_t i;

    if (a == NULL)
        PGOTO_ERROR_VOID("Empty region_list_t structure");

    LOG_JUST_PRINT("\n  == Region Info ==\n");
    LOG_JUST_PRINT("    ndim      = %lu\n", a->ndim);
    if (a->ndim > 4)
        PGOTO_ERROR_VOID("Error with dim %lu", a->ndim);
    for (i = 0; i < a->ndim; i++) {
        LOG_JUST_PRINT("    %5" PRIu64 "    %5" PRIu64 "\n", a->start[i], a->count[i]);
    }
    LOG_JUST_PRINT("    Storage location: [%s]\n", a->storage_location);
    LOG_JUST_PRINT("    Storage offset  : %" PRIu64 " \n", a->offset);
    LOG_JUST_PRINT("    Client IDs:");
    i = 0;
    while (1) {
        LOG_JUST_PRINT("%u, ", a->client_ids[i]);
        i++;
        if (a->client_ids[i] == 0)
            break;
    }
    LOG_JUST_PRINT("\n  =================\n");

done:
    FUNC_LEAVE_VOID();
}

uint64_t
PDC_get_region_size(region_list_t *a)

{
    FUNC_ENTER(NULL);

    uint64_t ret_value = 0;
    uint64_t size      = 1;
    unsigned i;

    for (i = 0; i < a->ndim; i++)
        size *= a->count[i];

    ret_value = size;

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_list_t_deep_cp(region_list_t *from, region_list_t *to)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (NULL == from || NULL == to)
        PGOTO_ERROR(FAIL, "NULL input");

    if (from->ndim > 4 || from->ndim <= 0)
        PGOTO_ERROR(FAIL, "ndim %zu error", from->ndim);

    memcpy(to, from, sizeof(region_list_t));

    memcpy(to->start, from->start, sizeof(uint64_t) * from->ndim);
    memcpy(to->count, from->count, sizeof(uint64_t) * from->ndim);
    memcpy(to->client_ids, from->client_ids, sizeof(uint32_t) * PDC_SERVER_MAX_PROC_PER_NODE);
    memcpy(to->shm_addr, from->shm_addr, sizeof(char) * ADDR_MAX);
    memcpy(to->storage_location, from->storage_location, ADDR_MAX);
    memcpy(to->cache_location, from->cache_location, ADDR_MAX);
    to->prev = NULL;
    to->next = NULL;

done:
    FUNC_LEAVE(ret_value);
}

int
PDC_region_list_seq_id_cmp(region_list_t *a, region_list_t *b)
{
    FUNC_ENTER(NULL);

    int ret_value = 0;
    ret_value     = (a->seq_id > b->seq_id) ? 1 : -1;

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_transfer_t_to_list_t(region_info_transfer_t *transfer, region_list_t *region)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    if (NULL == region || NULL == transfer)
        PGOTO_ERROR(FAIL, "NULL input");

    region->ndim = transfer->ndim;

    PDC_copy_region_desc(transfer->start, region->start, region->ndim, region->ndim);
    PDC_copy_region_desc(transfer->count, region->count, region->ndim, region->ndim);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_info_to_list_t(struct pdc_region_info *region, region_list_t *list)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    size_t i;

    if (NULL == region || NULL == list)
        PGOTO_ERROR(FAIL, "NULL input");

    size_t ndim = region->ndim;
    if (ndim <= 0 || ndim >= 5)
        PGOTO_ERROR(FAIL, "Unsupported dim: %lu", ndim);

    list->ndim = ndim;
    for (i = 0; i < ndim; i++) {
        list->start[i] = region->offset[i];
        list->count[i] = region->size[i];
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_info_t_to_transfer(struct pdc_region_info *region, region_info_transfer_t *transfer)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    size_t ndim      = region->ndim;

    if (NULL == region || NULL == transfer)
        PGOTO_ERROR(FAIL, "NULL input");

    if (ndim <= 0 || ndim >= 5)
        PGOTO_ERROR(FAIL, "Unsupported dim: %lu", ndim);

    transfer->ndim = ndim;

    PDC_copy_region_desc(region->offset, transfer->start, transfer->ndim, transfer->ndim);
    PDC_copy_region_desc(region->size, transfer->count, transfer->ndim, transfer->ndim);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_info_t_to_transfer_unit(struct pdc_region_info *region, region_info_transfer_t *transfer,
                                   size_t unit)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (NULL == region || NULL == transfer)
        PGOTO_ERROR(FAIL, "NULL input");

    size_t ndim = region->ndim;
    if (ndim <= 0 || ndim >= 5)
        PGOTO_ERROR(FAIL, "Unsupported dim: %lu", ndim);

    transfer->ndim = ndim;
    PDC_copy_region_desc_elements_to_bytes(region->offset, transfer->start, unit, ndim);
    PDC_copy_region_desc_elements_to_bytes(region->size, transfer->count, unit, ndim);

done:
    FUNC_LEAVE(ret_value);
}

struct pdc_region_info *
PDC_region_transfer_t_to_region_info(region_info_transfer_t *transfer)
{
    FUNC_ENTER(NULL);

    struct pdc_region_info *ret_value = NULL;
    int                     ndim;
    struct pdc_region_info *region;

    if (NULL == transfer)
        PGOTO_ERROR(NULL, "NULL input");

    region = (struct pdc_region_info *)PDC_calloc(1, sizeof(struct pdc_region_info));
    ndim = region->ndim = transfer->ndim;
    region->offset      = (uint64_t *)PDC_calloc(sizeof(uint64_t), ndim);
    region->size        = (uint64_t *)PDC_calloc(sizeof(uint64_t), ndim);

    PDC_copy_region_desc(transfer->start, region->offset, ndim, ndim);
    PDC_copy_region_desc(transfer->count, region->size, ndim, ndim);

    ret_value = region;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_list_t_to_transfer(region_list_t *region, region_info_transfer_t *transfer)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (NULL == region || NULL == transfer)
        PGOTO_ERROR(FAIL, "NULL input");

    transfer->ndim = region->ndim;
    PDC_copy_region_desc(region->start, transfer->start, transfer->ndim, transfer->ndim);
    PDC_copy_region_desc(region->count, transfer->count, transfer->ndim, transfer->ndim);

done:
    FUNC_LEAVE(ret_value);
}

// Fill the structure of pdc_metadata_transfer_t with pdc_metadata_t
perr_t
PDC_metadata_t_to_transfer_t(pdc_metadata_t *meta, pdc_metadata_transfer_t *transfer)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (NULL == meta || NULL == transfer)
        PGOTO_ERROR(FAIL, "NULL input");

    transfer->user_id          = meta->user_id;
    transfer->app_name         = meta->app_name;
    transfer->obj_name         = meta->obj_name;
    transfer->time_step        = meta->time_step;
    transfer->data_type        = meta->data_type;
    transfer->obj_id           = meta->obj_id;
    transfer->cont_id          = meta->cont_id;
    transfer->data_server_id   = meta->data_server_id;
    transfer->region_partition = meta->region_partition;
    transfer->consistency      = meta->consistency;
    transfer->ndim             = meta->ndim;
    transfer->dims0            = meta->dims[0];
    transfer->dims1            = meta->dims[1];
    transfer->dims2            = meta->dims[2];
    transfer->dims3            = meta->dims[3];
    transfer->tags             = meta->tags;
    transfer->data_location    = meta->data_location;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_transfer_t_to_metadata_t(pdc_metadata_transfer_t *transfer, pdc_metadata_t *meta)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (NULL == meta || NULL == transfer)
        PGOTO_ERROR(FAIL, "NULL input");

    meta->user_id          = transfer->user_id;
    meta->data_type        = transfer->data_type;
    meta->obj_id           = transfer->obj_id;
    meta->cont_id          = transfer->cont_id;
    meta->data_server_id   = transfer->data_server_id;
    meta->region_partition = transfer->region_partition;
    meta->consistency      = transfer->consistency;
    meta->time_step        = transfer->time_step;
    meta->ndim             = transfer->ndim;
    meta->dims[0]          = transfer->dims0;
    meta->dims[1]          = transfer->dims1;
    meta->dims[2]          = transfer->dims2;
    meta->dims[3]          = transfer->dims3;

    strcpy(meta->app_name, transfer->app_name);
    strcpy(meta->obj_name, transfer->obj_name);
    strcpy(meta->tags, transfer->tags);
    strcpy(meta->data_location, transfer->data_location);

done:
    FUNC_LEAVE(ret_value);
}

#ifndef IS_PDC_SERVER
hg_return_t
PDC_Server_get_client_addr(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_insert_metadata_to_hash_table(gen_obj_id_in_t *in   ATTRIBUTE(unused),
                                  gen_obj_id_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_search_with_name_hash(const char *obj_name ATTRIBUTE(unused), uint32_t hash_key ATTRIBUTE(unused),
                                 pdc_metadata_t **out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    return SUCCEED;
}
perr_t
PDC_Server_search_with_name_timestep(const char *obj_name ATTRIBUTE(unused),
                                     uint32_t hash_key ATTRIBUTE(unused), uint32_t ts ATTRIBUTE(unused),
                                     pdc_metadata_t **out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_delete_metadata_from_hash_table(metadata_delete_in_t *in   ATTRIBUTE(unused),
                                    metadata_delete_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in   ATTRIBUTE(unused),
                                 metadata_delete_by_id_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_update_metadata(metadata_update_in_t *in   ATTRIBUTE(unused),
                           metadata_update_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_add_tag_metadata(metadata_add_tag_in_t *in   ATTRIBUTE(unused),
                            metadata_add_tag_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_add_kvtag(metadata_add_kvtag_in_t *in ATTRIBUTE(unused),
                     metadata_add_tag_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_del_kvtag(metadata_get_kvtag_in_t *in ATTRIBUTE(unused),
                     metadata_add_tag_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_get_kvtag(metadata_get_kvtag_in_t *in   ATTRIBUTE(unused),
                     metadata_get_kvtag_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Meta_Server_buf_unmap(buf_unmap_in_t *in ATTRIBUTE(unused), hg_handle_t *handle ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Data_Server_buf_unmap(const struct hg_info *info ATTRIBUTE(unused), buf_unmap_in_t *in ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Meta_Server_buf_map(buf_map_in_t *in                  ATTRIBUTE(unused),
                        region_buf_map_t *new_buf_map_ptr ATTRIBUTE(unused),
                        hg_handle_t *handle               ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Data_Server_region_release(region_lock_in_t *in   ATTRIBUTE(unused),
                               region_lock_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Data_Server_region_lock(region_lock_in_t *in ATTRIBUTE(unused), region_lock_out_t *out ATTRIBUTE(unused),
                            hg_handle_t *handle ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region ATTRIBUTE(unused),
                              int *lock_status                  ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_local_region_lock_status(PDC_mapping_info_t *mapped_region ATTRIBUTE(unused),
                                    int *lock_status                  ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in ATTRIBUTE(unused),
                                    uint32_t *n_meta ATTRIBUTE(unused), void ***buf_ptrs ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_get_kvtag_query_result(pdc_kvtag_t *in ATTRIBUTE(unused), uint32_t *n_meta ATTRIBUTE(unused),
                                  uint64_t **buf_ptrs ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_update_local_region_storage_loc(region_list_t *region ATTRIBUTE(unused),
                                           uint64_t obj_id ATTRIBUTE(unused), int type ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_release_lock_request(uint64_t obj_id                ATTRIBUTE(unused),
                                struct pdc_region_info *region ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_data_write_out(uint64_t obj_id                     ATTRIBUTE(unused),
                          struct pdc_region_info *region_info ATTRIBUTE(unused), void *buf ATTRIBUTE(unused),
                          size_t unit ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_data_read_from(uint64_t obj_id                     ATTRIBUTE(unused),
                          struct pdc_region_info *region_info ATTRIBUTE(unused), void *buf ATTRIBUTE(unused),
                          size_t unit ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_data_read_in(uint64_t obj_id                     ATTRIBUTE(unused),
                        struct pdc_region_info *region_info ATTRIBUTE(unused), void *buf ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_data_write_direct(uint64_t obj_id                     ATTRIBUTE(unused),
                             struct pdc_region_info *region_info ATTRIBUTE(unused),
                             void *buf                           ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_data_read_direct(uint64_t obj_id                     ATTRIBUTE(unused),
                            struct pdc_region_info *region_info ATTRIBUTE(unused),
                            void *buf                           ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_notify_region_update_to_client(uint64_t meta_id  ATTRIBUTE(unused),
                                          uint64_t reg_id   ATTRIBUTE(unused),
                                          int32_t client_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_get_local_metadata_by_id(uint64_t obj_id           ATTRIBUTE(unused),
                                    pdc_metadata_t **res_meta ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_get_local_storage_location_of_region(uint64_t obj_id                    ATTRIBUTE(unused),
                                                region_list_t *region              ATTRIBUTE(unused),
                                                uint32_t *n_loc                    ATTRIBUTE(unused),
                                                region_list_t **overlap_region_loc ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_get_total_str_len(region_list_t **regions ATTRIBUTE(unused), uint32_t n_region ATTRIBUTE(unused),
                             uint32_t *len ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_serialize_regions_info(region_list_t **regions ATTRIBUTE(unused),
                                  uint32_t n_region ATTRIBUTE(unused), void *buf ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
pdc_metadata_t *
PDC_Server_get_obj_metadata(pdcid_t obj_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_update_region_storage_meta_bulk_local(
    update_region_storage_meta_bulk_t **bulk_ptrs ATTRIBUTE(unused), int cnt ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
hg_return_t
PDC_Server_count_write_check_update_storage_meta_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_create_container(gen_cont_id_in_t *in ATTRIBUTE(unused), gen_cont_id_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_set_close()
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}

hg_return_t
PDC_Server_checkpoint_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
hg_return_t
PDC_Server_recv_shm_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}

data_server_region_t *
PDC_Server_get_obj_region(pdcid_t obj_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(NULL);
}
perr_t
PDC_Server_register_obj_region(pdcid_t obj_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(0);
}
perr_t
PDC_Server_unregister_obj_region(pdcid_t obj_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(0);
}
perr_t
PDC_Server_register_obj_region_by_pointer(data_server_region_t **new_obj_reg ATTRIBUTE(unused),
                                          pdcid_t obj_id ATTRIBUTE(unused), int close_flag ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(0);
}
perr_t
PDC_Server_unregister_obj_region_by_pointer(data_server_region_t *new_obj_reg ATTRIBUTE(unused),
                                            int close_flag                    ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(0);
}
region_buf_map_t *
PDC_Data_Server_buf_map(const struct hg_info *info ATTRIBUTE(unused), buf_map_in_t *in ATTRIBUTE(unused),
                        region_list_t *request_region ATTRIBUTE(unused), void *data_ptr ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
void *
PDC_Server_maybe_allocate_region_buf_ptr(pdcid_t obj_id                ATTRIBUTE(unused),
                                         region_info_transfer_t region ATTRIBUTE(unused),
                                         size_t type_size              ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(NULL);
}
void *
PDC_Server_get_region_buf_ptr(pdcid_t obj_id                ATTRIBUTE(unused),
                              region_info_transfer_t region ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(NULL);
}
void *
PDC_Server_get_region_obj_ptr(pdcid_t obj_id                ATTRIBUTE(unused),
                              region_info_transfer_t region ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(NULL);
}
perr_t
PDC_Server_find_container_by_name(const char *cont_name             ATTRIBUTE(unused),
                                  pdc_cont_hash_table_entry_t **out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
};

hg_return_t
PDC_Server_recv_data_query(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
hg_return_t
PDC_recv_read_coords(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
hg_return_t
PDC_Server_recv_read_sel_obj_data(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
hg_return_t
PDC_recv_query_metadata_bulk(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}

hg_class_t *hg_class_g;

/*
 * Data server related
 */
hg_return_t
PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
perr_t
PDC_Server_read_check(data_server_read_check_in_t *in ATTRIBUTE(unused),
                      server_read_check_out_t *out    ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_write_check(data_server_write_check_in_t *in   ATTRIBUTE(unused),
                       data_server_write_check_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
hg_return_t
PDC_Server_s2s_work_done_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}

perr_t
PDC_Server_container_add_objs(int n_obj ATTRIBUTE(unused), uint64_t *obj_ids ATTRIBUTE(unused),
                              uint64_t cont_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_container_del_objs(int n_obj ATTRIBUTE(unused), uint64_t *obj_ids ATTRIBUTE(unused),
                              uint64_t cont_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
hg_return_t
PDC_Server_query_read_names_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
hg_return_t
PDC_Server_query_read_names_clinet_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}

hg_return_t
PDC_Server_storage_meta_name_query_bulk_respond(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
};
perr_t
PDC_Server_proc_storage_meta_bulk(int task_id ATTRIBUTE(unused), int n_regions ATTRIBUTE(unused),
                                  region_list_t *region_list_head ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_add_client_shm_to_cache(int cnt ATTRIBUTE(unused), void *buf_cp ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_container_add_tags(uint64_t cont_id ATTRIBUTE(unused), char *tags ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
hg_return_t
PDC_cache_region_to_bb_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}

hg_return_t
PDC_Server_recv_data_query_region(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
hg_return_t
PDC_Server_recv_get_sel_data(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
perr_t
PDC_Server_dart_get_server_info(dart_get_server_info_in_t *in   ATTRIBUTE(unused),
                                dart_get_server_info_out_t *out ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDC_Server_dart_perform_one_server(dart_perform_one_server_in_t *in   ATTRIBUTE(unused),
                                   dart_perform_one_server_out_t *out ATTRIBUTE(unused),
                                   uint64_t *n_obj_ids_ptr            ATTRIBUTE(unused),
                                   uint64_t **buf_ptrs                ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}

#else
hg_return_t
PDC_Client_work_done_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
};
hg_return_t
PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
};
perr_t
PDC_Client_query_read_complete(char *shm_addrs ATTRIBUTE(unused), int size ATTRIBUTE(unused),
                               int n_shm ATTRIBUTE(unused), int seq_id ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
hg_return_t
PDC_Client_recv_bulk_storage_meta_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}
perr_t
PDC_Client_recv_bulk_storage_meta(process_bulk_storage_meta_args_t *process_args ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}

hg_return_t
PDC_recv_read_coords_data(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(HG_SUCCESS);
}

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

    perr_t ret_value = HG_SUCCESS;

    // Decode input
    gen_obj_id_in_t  in;
    gen_obj_id_out_t out;

    HG_Get_input(handle, &in);

    // Insert to hash table
    ret_value = PDC_insert_metadata_to_hash_table(&in, &out);

#ifdef ENABLE_MPI
    int server_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &server_rank);

#endif
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* gen_cont_id_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(gen_cont_id, handle)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = HG_SUCCESS;

    // Decode input
    gen_cont_id_in_t  in;
    gen_cont_id_out_t out;

    HG_Get_input(handle, &in);

    // Insert to hash table
    ret_value = PDC_Server_create_container(&in, &out);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(ret_value, "Error with container object creation");

    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

done:
    FUNC_LEAVE(ret_value);
}

// This is for the CLIENT
/* static hg_return_t */
/* server_lookup_client_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(server_lookup_client, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                ret_value = HG_SUCCESS;
    server_lookup_client_in_t  in;
    server_lookup_client_out_t out;

    // Decode input
    HG_Get_input(handle, &in);
    out.ret = in.server_id + 43210000;

    HG_Respond(handle, PDC_Client_work_done_cb, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* server_lookup_remote_server_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(server_lookup_remote_server, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                       ret_value = HG_SUCCESS;
    server_lookup_remote_server_in_t  in;
    server_lookup_remote_server_out_t out;

    // Decode input
    HG_Get_input(handle, &in);
    out.ret = in.server_id + 1024000;

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* client_test_connect_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(client_test_connect, handle)
{
    FUNC_ENTER(NULL);

    // SERVER EXEC
    hg_return_t               ret_value = HG_SUCCESS;
    client_test_connect_in_t  in;
    client_test_connect_out_t out;
    client_test_connect_args *args =
        (client_test_connect_args *)PDC_calloc(1, sizeof(client_test_connect_args));

    // Decode input
    HG_Get_input(handle, &in);
    out.ret = in.client_id + 123400;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_client_info_mutex_g);
#endif
    args->client_id = in.client_id;
    args->nclient   = in.nclient;
    args->is_init   = in.is_init;
    sprintf(args->client_addr, "%s", in.client_addr);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_client_info_mutex_g);
#endif

    HG_Respond(handle, PDC_Server_get_client_addr, args, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* container_query_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(container_query, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                  ret_value = HG_SUCCESS;
    container_query_in_t         in;
    container_query_out_t        out;
    pdc_cont_hash_table_entry_t *cont_entry;

    // Decode input
    HG_Get_input(handle, &in);

    PDC_Server_find_container_by_name(in.cont_name, &cont_entry);
    if (cont_entry)
        out.cont_id = cont_entry->cont_id;
    else
        out.cont_id = 0;

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* metadata_query_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(metadata_query, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t          ret_value = HG_SUCCESS;
    metadata_query_in_t  in;
    metadata_query_out_t out;
    pdc_metadata_t *     query_result = NULL;

    // Decode input
    HG_Get_input(handle, &in);

    // Do the work
    PDC_Server_search_with_name_timestep(in.obj_name, in.hash_value, in.time_step, &query_result);

    // Convert for transfer
    if (query_result != NULL) {
        PDC_metadata_t_to_transfer_t(query_result, &out.ret);
    }
    else {
        out.ret.user_id        = -1;
        out.ret.data_type      = -1;
        out.ret.obj_id         = 0;
        out.ret.cont_id        = 0;
        out.ret.data_server_id = 0;
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
/* obj_reset_dims_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(obj_reset_dims, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t          ret_value = HG_SUCCESS;
    obj_reset_dims_in_t  in;
    obj_reset_dims_out_t out;
    pdc_metadata_t *     query_result = NULL;

    // Decode input
    HG_Get_input(handle, &in);

    if (!can_reset_dims()) {
        LOG_WARNING("Cannot resize object dims due to storage strategy\n");
        out.ret = 0;
        PGOTO_DONE(ret_value);
    }

    // Get the metdata_t struct.
    PDC_Server_search_with_name_timestep(in.obj_name, in.hash_value, in.time_step, &query_result);

    // Convert for transfer
    if (query_result != NULL) {
        out.ret = 2;
        if (in.ndim >= 1) {
            query_result->dims[0] = in.dims0;
        }
        if (in.ndim >= 2) {
            query_result->dims[1] = in.dims1;
        }
        if (in.ndim == 3) {
            query_result->dims[2] = in.dims2;
        }
    }
    else {
        out.ret = 1;
    }

done:
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_delete_by_id_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_delete_by_id, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                ret_value = HG_SUCCESS;
    metadata_delete_by_id_in_t in;

    metadata_delete_by_id_out_t out;

    // Decode input
    HG_Get_input(handle, &in);

    PDC_Server_delete_metadata_by_id(&in, &out);

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_delete_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_delete, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value = HG_SUCCESS;
    metadata_delete_in_t  in;
    metadata_delete_out_t out;

    // Decode input
    HG_Get_input(handle, &in);

    PDC_delete_metadata_from_hash_table(&in, &out);

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// send_rpc_cb(hg_handle_t handle)
HG_TEST_RPC_CB(send_rpc, handle)
{
    FUNC_ENTER(NULL);

    send_rpc_in_t  in;
    send_rpc_out_t out;
    hg_return_t    ret_value = HG_SUCCESS;

    HG_Get_input(handle, &in);
    LOG_ERROR("Received value from client %d\n", in.value);

    out.value = 1;
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

    hg_return_t ret_value = HG_SUCCESS;

    // Decode input
    metadata_add_tag_in_t  in;
    metadata_add_tag_out_t out;

    HG_Get_input(handle, &in);

    PDC_Server_add_tag_metadata(&in, &out);

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_get_kvtag_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_get_kvtag, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t              ret_value = HG_SUCCESS;
    metadata_get_kvtag_in_t  in;
    metadata_get_kvtag_out_t out;

    memset(&out, 0, sizeof(metadata_get_kvtag_out_t));
    memset(&out.kvtag, 0, sizeof(pdc_kvtag_t));
    HG_Get_input(handle, &in);
    PDC_Server_get_kvtag(&in, &out);
    ret_value = HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_del_kvtag_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_del_kvtag, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t             ret_value = HG_SUCCESS;
    metadata_get_kvtag_in_t in;
    metadata_add_tag_out_t  out;

    HG_Get_input(handle, &in);
    PDC_Server_del_kvtag(&in, &out);
    ret_value = HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// metadata_add_kvtag_cb(hg_handle_t handle)
HG_TEST_RPC_CB(metadata_add_kvtag, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t             ret_value = HG_SUCCESS;
    metadata_add_kvtag_in_t in;
    metadata_add_tag_out_t  out;

    HG_Get_input(handle, &in);
    if (strcmp(in.kvtag.name, "PDC_NOOP") != 0) {
        PDC_Server_add_kvtag(&in, &out);
    }
    else {
        LOG_INFO("Received NOOP\n");
        out.ret = 1;
    }

    ret_value = HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// notify_io_complete_cb(hg_handle_t handle)
HG_TEST_RPC_CB(notify_io_complete, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t              ret_value = HG_SUCCESS;
    notify_io_complete_in_t  in;
    notify_io_complete_out_t out;

    // Decode input
    HG_Get_input(handle, &in);
    pdc_access_t type = (pdc_access_t)in.io_type;

    client_read_info_t *read_info = (client_read_info_t *)PDC_calloc(1, sizeof(client_read_info_t));
    read_info->obj_id             = in.obj_id;
    strcpy(read_info->shm_addr, in.shm_addr);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with notify_io_complete_register");

    out.ret = atoi(in.shm_addr) + 112000;
    if (type == PDC_READ) {
        HG_Respond(handle, PDC_Client_get_data_from_server_shm_cb, read_info, &out);
    }
    else if (type == PDC_WRITE) {
        HG_Respond(handle, PDC_Client_work_done_cb, read_info, &out);
    }
    else {
        LOG_ERROR("Error with io type\n");
        HG_Respond(handle, NULL, NULL, &out);
    }

done:
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// notify_region_update_cb(hg_handle_t handle)
HG_TEST_RPC_CB(notify_region_update, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                ret_value = HG_SUCCESS;
    notify_region_update_in_t  in;
    notify_region_update_out_t out;

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
    FUNC_ENTER(NULL);

    hg_return_t           ret_value = HG_SUCCESS;
    metadata_update_in_t  in;
    metadata_update_out_t out;

    // Decode input
    HG_Get_input(handle, &in);

    PDC_Server_update_metadata(&in, &out);

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// flush_obj_all_cb(hg_handle_t handle)
HG_TEST_RPC_CB(flush_obj_all, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t         ret_value = HG_SUCCESS;
    flush_obj_all_in_t  in;
    flush_obj_all_out_t out;

    HG_Get_input(handle, &in);

    if (in.tag != 44) {
        PGOTO_ERROR(ret_value, "Error with input tag");
    }

    ret_value = HG_Free_input(handle, &in);

    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Destroy");

    out.ret = 1;
    HG_Respond(handle, NULL, NULL, &out);
    HG_Destroy(handle);

#ifdef PDC_SERVER_CACHE
    PDC_region_cache_flush_all();
#endif

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// flush_obj_cb(hg_handle_t handle)
HG_TEST_RPC_CB(flush_obj, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t     ret_value = HG_SUCCESS;
    flush_obj_in_t  in;
    flush_obj_out_t out;
    uint64_t        obj_id;

    HG_Get_input(handle, &in);

    obj_id = in.obj_id;

    ret_value = HG_Free_input(handle, &in);

    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Destroy");

    out.ret = 1;
    HG_Respond(handle, NULL, NULL, &out);
    HG_Destroy(handle);

#ifdef PDC_SERVER_CACHE
    pthread_mutex_lock(&pdc_obj_cache_list_mutex);
    PDC_region_cache_flush(obj_id);
    pthread_mutex_unlock(&pdc_obj_cache_list_mutex);
#endif

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// close_server_cb(hg_handle_t handle)
HG_TEST_RPC_CB(close_server, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t       ret_value = HG_SUCCESS;
    close_server_in_t in;
    // close_server_out_t out;

    HG_Get_input(handle, &in);
    HG_Free_input(handle, &in);

    close_all_server_handle_g = handle;

    PDC_Server_set_close();

    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Destroy");

done:
    FUNC_LEAVE(ret_value);
}

#ifdef ENABLE_MULTITHREAD
static HG_THREAD_RETURN_TYPE
pdc_region_write_out_progress(void *arg)
{
    FUNC_ENTER(NULL);

    HG_THREAD_RETURN_TYPE             ret_value       = (HG_THREAD_RETURN_TYPE)0;
    struct buf_map_release_bulk_args *bulk_args       = (struct buf_map_release_bulk_args *)arg;
    struct pdc_region_info *          remote_reg_info = NULL;
    region_lock_out_t                 out;

    remote_reg_info = (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));
    if (remote_reg_info == NULL) {
        PGOTO_ERROR(ret_value, "pdc_region_write_out_progress: remote_reg_info memory allocation failed");
    }
    remote_reg_info->ndim   = (bulk_args->remote_region_nounit).ndim;
    remote_reg_info->offset = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));
    remote_reg_info->size   = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));
    if (remote_reg_info->ndim >= 1) {
        (remote_reg_info->offset)[0] = (bulk_args->remote_region_nounit).start[0];
        (remote_reg_info->size)[0]   = (bulk_args->remote_region_nounit).count[0];
    }
    if (remote_reg_info->ndim >= 2) {
        (remote_reg_info->offset)[1] = (bulk_args->remote_region_nounit).start[1];
        (remote_reg_info->size)[1]   = (bulk_args->remote_region_nounit).count[1];
    }
    if (remote_reg_info->ndim >= 3) {
        (remote_reg_info->offset)[2] = (bulk_args->remote_region_nounit).start[2];
        (remote_reg_info->size)[2]   = (bulk_args->remote_region_nounit).count[2];
    }

    PDC_Server_data_write_out(bulk_args->remote_obj_id, remote_reg_info, bulk_args->data_buf,
                              (bulk_args->in).data_unit);

    // Perform lock release function
    PDC_Data_Server_region_release(&(bulk_args->in), &out);

    PDC_Server_release_lock_request(bulk_args->remote_obj_id, remote_reg_info);

    remote_reg_info->offset = (uint64_t *)PDC_free(remote_reg_info->offset);
    remote_reg_info->size   = (uint64_t *)PDC_free(remote_reg_info->size);
    remote_reg_info         = (struct pdc_region_info *)PDC_free(remote_reg_info);

done:
    HG_Bulk_free(bulk_args->remote_bulk_handle);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);

    hg_thread_mutex_lock(&(bulk_args->work_mutex));
    bulk_args->work_completed = 1;
    hg_thread_cond_signal(&(bulk_args->work_cond));
    hg_thread_mutex_unlock(&(bulk_args->work_mutex));

    FUNC_LEAVE(ret_value);
}

// enter this function, transfer is done, data is pushed to buffer
static hg_return_t
obj_map_region_release_bulk_transfer_thread_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                       ret_value = HG_SUCCESS;
    region_lock_out_t                 out;
    struct buf_map_release_bulk_args *bulk_args = NULL;

    bulk_args = (struct buf_map_release_bulk_args *)hg_cb_info->arg;

    if (hg_cb_info->ret == HG_CANCELED) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "HG_Bulk_transfer() was successfully canceled");
    }
    else if (hg_cb_info->ret != HG_SUCCESS) {
        out.ret = 0;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in region_release_bulk_transfer_cb()");
    }

    // release the lock
    PDC_Data_Server_region_release(&(bulk_args->in), &out);

    PDC_Server_release_lock_request(bulk_args->remote_obj_id, bulk_args->remote_reg_info);

done:
    bulk_args->remote_reg_info->offset = (uint64_t *)PDC_free(bulk_args->remote_reg_info->offset);
    bulk_args->remote_reg_info->size   = (uint64_t *)PDC_free(bulk_args->remote_reg_info->size);
    bulk_args->remote_reg_info         = (struct pdc_region_info *)PDC_free(bulk_args->remote_reg_info);

    HG_Bulk_free(bulk_args->remote_bulk_handle);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);

    hg_thread_mutex_lock(&(bulk_args->work_mutex));
    bulk_args->work_completed = 1;
    hg_thread_cond_signal(&(bulk_args->work_cond));
    hg_thread_mutex_unlock(&(bulk_args->work_mutex));

    FUNC_LEAVE(ret_value);
}

static HG_THREAD_RETURN_TYPE
pdc_region_read_from_progress(void *arg)
{
    FUNC_ENTER(NULL);

    HG_THREAD_RETURN_TYPE             ret_value = (HG_THREAD_RETURN_TYPE)0;
    struct buf_map_release_bulk_args *bulk_args = (struct buf_map_release_bulk_args *)arg;
    const struct hg_info *            hg_info   = NULL;
    hg_return_t                       hg_ret;
    size_t                            size;
    int                               error = 0;

    hg_info = HG_Get_info(bulk_args->handle);

    PDC_Server_data_read_from(bulk_args->remote_obj_id, bulk_args->remote_reg_info, bulk_args->data_buf,
                              (bulk_args->in).data_unit);

    // Push bulk data
    size = HG_Bulk_get_size(bulk_args->local_bulk_handle);
    if (size != HG_Bulk_get_size(bulk_args->remote_bulk_handle)) {
        error = 1;
        PGOTO_ERROR(ret_value, "pdc_region_read_from_progress local and remote bulk size does not match");
    }

    hg_ret = HG_Bulk_transfer(hg_info->context, obj_map_region_release_bulk_transfer_thread_cb, bulk_args,
                              HG_BULK_PUSH, bulk_args->local_addr, bulk_args->local_bulk_handle, 0,
                              bulk_args->remote_bulk_handle, 0, size, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS) {
        error = 1;
        PGOTO_ERROR(ret_value, "pdc_region_read_from_progress push data failed");
    }

done:
    if (error == 1) {
        HG_Bulk_free(bulk_args->remote_bulk_handle);
        HG_Free_input(bulk_args->handle, &(bulk_args->in));
        HG_Destroy(bulk_args->handle);

        hg_thread_mutex_lock(&(bulk_args->work_mutex));
        bulk_args->work_completed = 1;
        hg_thread_cond_signal(&(bulk_args->work_cond));
        hg_thread_mutex_unlock(&(bulk_args->work_mutex));
    }

    FUNC_LEAVE(ret_value);
}
#endif

// enter this function, transfer is done, data is in data server
static hg_return_t
buf_map_region_release_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                       ret_value = HG_SUCCESS;
    region_lock_out_t                 out;
    struct buf_map_release_bulk_args *bulk_args = NULL;
#ifdef ENABLE_MULTITHREAD
    data_server_region_t *target_reg = NULL;
    region_buf_map_t *    elt;
#else
    struct pdc_region_info *remote_reg_info = NULL;
#endif

    bulk_args = (struct buf_map_release_bulk_args *)hg_cb_info->arg;
#ifdef PDC_TIMING
    double end, start;

    end = MPI_Wtime();
    pdc_server_timings->PDCreg_release_lock_bulk_transfer_write_rpc += end - bulk_args->start_time;
    pdc_timestamp_register(pdc_release_lock_bulk_transfer_write_timestamps, bulk_args->start_time, end);

    start = MPI_Wtime();
#endif

    if (hg_cb_info->ret == HG_CANCELED) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "HG_Bulk_transfer() was successfully canceled");
    }
    else if (hg_cb_info->ret != HG_SUCCESS) {
        out.ret = 0;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in region_release_bulk_transfer_cb()");
    }

    out.ret = 1;
    HG_Respond(bulk_args->handle, NULL, NULL, &out);

#ifdef ENABLE_MULTITHREAD
    bulk_args->work.func = pdc_region_write_out_progress;
    bulk_args->work.args = bulk_args;

    target_reg = PDC_Server_get_obj_region(bulk_args->remote_obj_id);
    DL_FOREACH(target_reg->region_buf_map_head, elt)
    {
        if (PDC_region_info_transfer_t_is_equal(&(bulk_args->remote_region_unit),
                                                &(elt->remote_region_unit))) {
            elt->bulk_args = bulk_args;
        }
    }

    hg_thread_pool_post(hg_test_thread_pool_fs_g, &(bulk_args->work));

#else
    remote_reg_info = (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));
    if (remote_reg_info == NULL)
        PGOTO_ERROR(HG_OTHER_ERROR, "remote_reg_info memory allocation failed");

    remote_reg_info->ndim = (bulk_args->remote_region_nounit).ndim;
    remote_reg_info->offset = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));
    remote_reg_info->size = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));

    PDC_copy_region_desc(bulk_args->remote_region_nounit.start, remote_reg_info->offset,
                         remote_reg_info->ndim, remote_reg_info->ndim);
    PDC_copy_region_desc(bulk_args->remote_region_nounit.count, remote_reg_info->size, remote_reg_info->ndim,
                         remote_reg_info->ndim);
#ifdef PDC_SERVER_CACHE
    PDC_transfer_request_data_write_out(bulk_args->remote_obj_id, 0, NULL, remote_reg_info,
                                        (void *)bulk_args->data_buf, (bulk_args->in).data_unit);
#else
    PDC_Server_transfer_request_io(bulk_args->remote_obj_id, 0, NULL, remote_reg_info, bulk_args->data_buf,
                                   (bulk_args->in).data_unit, 1);
#endif

    // Perform lock release function
    PDC_Data_Server_region_release(&(bulk_args->in), &out);

    PDC_Server_release_lock_request(bulk_args->remote_obj_id, remote_reg_info);
#endif

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_server_timings->PDCreg_release_lock_bulk_transfer_inner_write_rpc += end - start;
    pdc_timestamp_register(pdc_release_lock_bulk_transfer_inner_write_timestamps, start, end);
#endif

done:
#ifndef ENABLE_MULTITHREAD
    remote_reg_info->offset = (uint64_t *)PDC_free(remote_reg_info->offset);
    remote_reg_info->size   = (uint64_t *)PDC_free(remote_reg_info->size);
    remote_reg_info         = (struct pdc_region_info *)PDC_free(remote_reg_info);

    HG_Bulk_free(bulk_args->remote_bulk_handle);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct buf_map_release_bulk_args *)PDC_free(bulk_args);
#endif

    FUNC_LEAVE(ret_value);
}

#ifndef ENABLE_MULTITHREAD
// enter this function, transfer is done, data is pushed to buffer
static hg_return_t
obj_map_region_release_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                       ret_value = HG_SUCCESS;
    region_lock_out_t                 out;
    struct buf_map_release_bulk_args *bulk_args = NULL;

    bulk_args = (struct buf_map_release_bulk_args *)hg_cb_info->arg;

#ifdef PDC_TIMING
    double end, start;

    end = MPI_Wtime();
    pdc_server_timings->PDCreg_release_lock_bulk_transfer_read_rpc += end - bulk_args->start_time;
    pdc_timestamp_register(pdc_release_lock_bulk_transfer_read_timestamps, bulk_args->start_time, end);

    start = MPI_Wtime();
#endif

    if (hg_cb_info->ret == HG_CANCELED) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "HG_Bulk_transfer() was successfully canceled");
    }
    else if (hg_cb_info->ret != HG_SUCCESS) {
        out.ret = 0;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in region_release_bulk_transfer_cb()");
    }

    // release the lock
    PDC_Data_Server_region_release(&(bulk_args->in), &out);
    HG_Respond(bulk_args->handle, NULL, NULL, &out);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_server_timings->PDCreg_release_lock_bulk_transfer_inner_read_rpc += end - start;
    pdc_timestamp_register(pdc_release_lock_bulk_transfer_inner_read_timestamps, start, end);
#endif

done:
    bulk_args->remote_reg_info = (struct pdc_region_info *)PDC_free(bulk_args->remote_reg_info);
    HG_Free_input(bulk_args->handle, &(bulk_args->in));
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct buf_map_release_bulk_args *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}
#endif

static hg_return_t
region_release_update_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                          hg_ret = HG_SUCCESS;
    region_lock_out_t                    out;
    hg_handle_t                          handle;
    struct region_lock_update_bulk_args *bulk_args = NULL;

    bulk_args = (struct region_lock_update_bulk_args *)hg_cb_info->arg;
    handle    = bulk_args->handle;

    // Perform lock releae function
    PDC_Data_Server_region_release(&(bulk_args->in), &out);
    HG_Respond(handle, NULL, NULL, &out);

    // Send notification to mapped regions, when data transfer is done
    PDC_Server_notify_region_update_to_client(bulk_args->remote_obj_id, bulk_args->remote_reg_id,
                                              bulk_args->remote_client_id);

    bulk_args->server_region->size   = (uint64_t *)PDC_free(bulk_args->server_region->size);
    bulk_args->server_region->offset = (uint64_t *)PDC_free(bulk_args->server_region->offset);
    bulk_args->server_region         = (struct pdc_region_info *)PDC_free(bulk_args->server_region);
    bulk_args->data_buf              = (void *)PDC_free(bulk_args->data_buf);

    HG_Free_input(handle, &(bulk_args->in));
    HG_Destroy(handle);
    bulk_args = (struct region_lock_update_bulk_args *)PDC_free(bulk_args);

    FUNC_LEAVE(hg_ret);
}

// region_release_cb()
HG_TEST_RPC_CB(region_release, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                          ret_value = HG_SUCCESS;
    hg_return_t                          hg_ret;
    region_lock_in_t                     in;
    region_lock_out_t                    out;
    const struct hg_info *               hg_info    = NULL;
    data_server_region_t *               target_obj = NULL;
    int                                  error      = 0;
    int                                  dirty_reg  = 0;
    hg_size_t                            size, size2;
    void *                               data_buf;
    struct pdc_region_info *             server_region;
    region_list_t *                      elt = NULL, *request_region = NULL, *tmp = NULL, *elt_tmp = NULL;
    struct region_lock_update_bulk_args *lock_update_bulk_args = NULL;
    struct buf_map_release_bulk_args *   buf_map_bulk_args = NULL, *obj_map_bulk_args = NULL;
    hg_bulk_t                            lock_local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t                            remote_bulk_handle     = HG_BULK_NULL;
    struct pdc_region_info *             remote_reg_info        = NULL;
    region_buf_map_t *                   eltt = NULL, *eltt2 = NULL, *eltt_tmp = NULL;
    hg_uint32_t                          remote_count;
    void **                              data_ptrs_to = NULL;
    size_t *                             data_size_to = NULL;
#ifdef PDC_TIMING
    double start, end;
#endif

#ifdef PDC_TIMING
    start = MPI_Wtime();
#endif
    // Decode input
    HG_Get_input(handle, &in);
    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    if (in.access_type == PDC_READ) {
        // check region is dirty or not, if dirty transfer data
        request_region = (region_list_t *)PDC_malloc(sizeof(region_list_t));
        PDC_region_transfer_t_to_list_t(&in.region, request_region);
        target_obj = PDC_Server_get_obj_region(in.obj_id);
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
        DL_FOREACH_SAFE(target_obj->region_lock_head, elt, elt_tmp)
        {
            if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty_from_buf == 1 &&
                hg_atomic_get32(&(elt->buf_map_refcount)) == 0) {
                dirty_reg = 1;
                size      = HG_Bulk_get_size(elt->bulk_handle);
                data_buf  = (void *)PDC_calloc(1, size);
                // data transfer
                server_region         = (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));
                server_region->ndim   = 1;
                server_region->size   = (uint64_t *)PDC_malloc(sizeof(uint64_t));
                server_region->offset = (uint64_t *)PDC_malloc(sizeof(uint64_t));
                (server_region->size)[0]   = size;
                (server_region->offset)[0] = in.region.start[0];

                ret_value = PDC_Server_data_read_direct(elt->from_obj_id, server_region, data_buf);
                if (ret_value != SUCCEED)
                    PGOTO_ERROR(HG_OTHER_ERROR, "PDC_Server_data_read_direct failed");
                hg_ret = HG_Bulk_create(hg_info->hg_class, 1, &data_buf, &size, HG_BULK_READWRITE,
                                        &lock_local_bulk_handle);
                if (hg_ret != HG_SUCCESS)
                    PGOTO_ERROR(hg_ret, "Could not create bulk data handle");

                lock_update_bulk_args = (struct region_lock_update_bulk_args *)PDC_malloc(
                    sizeof(struct region_lock_update_bulk_args));
                lock_update_bulk_args->handle           = handle;
                lock_update_bulk_args->in               = in;
                lock_update_bulk_args->remote_obj_id    = elt->obj_id;
                lock_update_bulk_args->remote_reg_id    = elt->reg_id;
                lock_update_bulk_args->remote_client_id = elt->client_id;
                lock_update_bulk_args->data_buf         = data_buf;
                lock_update_bulk_args->server_region    = server_region;

                hg_ret = HG_Bulk_transfer(hg_info->context, region_release_update_bulk_transfer_cb,
                                          lock_update_bulk_args, HG_BULK_PUSH, elt->addr, elt->bulk_handle, 0,
                                          lock_local_bulk_handle, 0, size, HG_OP_ID_IGNORE);
                if (hg_ret != HG_SUCCESS)
                    PGOTO_ERROR(hg_ret, "Could not write bulk data");
            }

            if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty_from_buf == 1 &&
                hg_atomic_get32(&(elt->buf_map_refcount)) > 0) {
                dirty_reg = 1;
                tmp       = (region_list_t *)PDC_malloc(sizeof(region_list_t));
                DL_FOREACH_SAFE(target_obj->region_buf_map_head, eltt2, eltt_tmp)
                {
                    PDC_region_transfer_t_to_list_t(&(eltt2->remote_region_unit), tmp);
                    if (PDC_is_same_region_list(tmp, request_region) == 1) {
                        // get remote object memory addr
                        data_buf = PDC_Server_get_region_buf_ptr(in.obj_id, in.region);
                        if (in.region.ndim > 0) {
                            remote_count  = 1;
                            data_ptrs_to  = (void **)PDC_malloc(sizeof(void *));
                            data_size_to  = (size_t *)PDC_malloc(sizeof(size_t));
                            *data_ptrs_to = data_buf;
                            PDC_copy_region_desc_bytes_to_elements((eltt2->remote_region_unit).count,
                                                                   data_size_to, in.region.ndim,
                                                                   in.data_unit);
                        }

                        hg_ret =
                            HG_Bulk_create(hg_info->hg_class, remote_count, data_ptrs_to,
                                           (hg_size_t *)data_size_to, HG_BULK_READWRITE, &remote_bulk_handle);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            PGOTO_ERROR(hg_ret, "obj map Could not create bulk data handle");
                        }
                        data_ptrs_to = (void **)PDC_free(data_ptrs_to);
                        data_size_to = (size_t *)PDC_free(data_size_to);

                        remote_reg_info =
                            (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));
                        if (remote_reg_info == NULL) {
                            error = 1;
                            PGOTO_ERROR(HG_OTHER_ERROR, "remote_reg_info memory allocation failed");
                        }

                        obj_map_bulk_args = (struct buf_map_release_bulk_args *)PDC_malloc(
                            sizeof(struct buf_map_release_bulk_args));
                        memset(obj_map_bulk_args, 0, sizeof(struct buf_map_release_bulk_args));
                        obj_map_bulk_args->handle = handle;

                        obj_map_bulk_args->data_buf             = data_buf;
                        obj_map_bulk_args->in                   = in;
                        obj_map_bulk_args->remote_obj_id        = eltt2->remote_obj_id;
                        obj_map_bulk_args->remote_reg_id        = eltt2->remote_reg_id;
                        obj_map_bulk_args->remote_reg_info      = remote_reg_info;
                        obj_map_bulk_args->remote_region_unit   = eltt2->remote_region_unit;
                        obj_map_bulk_args->remote_region_nounit = eltt2->remote_region_nounit;
                        obj_map_bulk_args->remote_client_id     = eltt2->remote_client_id;
                        obj_map_bulk_args->remote_bulk_handle   = remote_bulk_handle;
                        obj_map_bulk_args->local_bulk_handle    = eltt2->local_bulk_handle;
                        obj_map_bulk_args->local_addr           = eltt2->local_addr;
#ifdef PDC_TIMING
                        obj_map_bulk_args->start_time = MPI_Wtime();
#endif
                        remote_reg_info->ndim = (obj_map_bulk_args->remote_region_unit).ndim;
                        remote_reg_info->offset =
                            (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));
                        remote_reg_info->size =
                            (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));

                        PDC_copy_region_desc(obj_map_bulk_args->remote_region_nounit.start,
                                             remote_reg_info->offset, remote_reg_info->ndim,
                                             remote_reg_info->ndim);
                        PDC_copy_region_desc(obj_map_bulk_args->remote_region_nounit.count,
                                             remote_reg_info->size, remote_reg_info->ndim,
                                             remote_reg_info->ndim);
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
#ifdef PDC_SERVER_CACHE
                        PDC_transfer_request_data_read_from(obj_map_bulk_args->remote_obj_id, 0, NULL,
                                                            remote_reg_info, data_buf, in.data_unit);
#else
                        PDC_Server_transfer_request_io(obj_map_bulk_args->remote_obj_id, 0, NULL,
                                                       remote_reg_info, data_buf, in.data_unit, 0);
#endif
                        size = HG_Bulk_get_size(eltt2->local_bulk_handle);
                        size2 = HG_Bulk_get_size(remote_bulk_handle);
                        if (size != size2) {
                            error = 1;
                            LOG_INFO("Local size %llu, remote %llu\n", size, size2);
                            PGOTO_ERROR(HG_OTHER_ERROR, "Local and remote bulk size does not match");
                        }

                        hg_ret = HG_Bulk_transfer(hg_info->context, obj_map_region_release_bulk_transfer_cb,
                                                  obj_map_bulk_args, HG_BULK_PUSH, eltt2->local_addr,
                                                  eltt2->local_bulk_handle, 0, remote_bulk_handle, 0, size,
                                                  HG_OP_ID_IGNORE);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            PGOTO_ERROR(hg_ret, "Map Could not write bulk data");
                        }
#endif
                        break;
                    }
                }
                tmp = (region_list_t *)PDC_free(tmp);
            }
        }
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif
        request_region = (region_list_t *)PDC_free(request_region);

        if (dirty_reg == 0) {
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
        request_region = (region_list_t *)PDC_malloc(sizeof(region_list_t));
        PDC_region_transfer_t_to_list_t(&in.region, request_region);
        target_obj = PDC_Server_get_obj_region(in.obj_id);
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
        DL_FOREACH(target_obj->region_lock_head, elt)
        {
            if (PDC_is_same_region_list(request_region, elt) == 1 && elt->reg_dirty_from_buf == 1 &&
                hg_atomic_get32(&(elt->buf_map_refcount)) > 0) {
                dirty_reg = 1;
                tmp       = (region_list_t *)PDC_malloc(sizeof(region_list_t));
                DL_FOREACH(target_obj->region_buf_map_head, eltt)
                {
                    PDC_region_transfer_t_to_list_t(&(eltt->remote_region_unit), tmp);
                    if (PDC_is_same_region_list(tmp, request_region) == 1) {
                        // type_size = eltt->remote_unit;
                        // get remote object memory addr
                        data_buf = PDC_Server_get_region_buf_ptr(in.obj_id, in.region);
                        if (in.region.ndim > 0) {
                            remote_count  = 1;
                            data_ptrs_to  = (void **)PDC_malloc(sizeof(void *));
                            data_size_to  = (size_t *)PDC_malloc(sizeof(size_t));
                            *data_ptrs_to = data_buf;
                            PDC_copy_region_desc_bytes_to_elements((eltt2->remote_region_unit).count,
                                                                   data_size_to, in.region.ndim,
                                                                   in.data_unit);
                        }
                        /* Create a new block handle to read the data */
                        hg_ret =
                            HG_Bulk_create(hg_info->hg_class, remote_count, data_ptrs_to,
                                           (hg_size_t *)data_size_to, HG_BULK_READWRITE, &remote_bulk_handle);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            PGOTO_ERROR(hg_ret, "Could not create bulk data handle");
                        }
                        data_ptrs_to = (void **)PDC_free(data_ptrs_to);
                        data_size_to = (size_t *)PDC_free(data_size_to);

                        buf_map_bulk_args = (struct buf_map_release_bulk_args *)PDC_malloc(
                            sizeof(struct buf_map_release_bulk_args));
                        if (buf_map_bulk_args == NULL) {
                            error = 1;
                            PGOTO_ERROR(HG_OTHER_ERROR, "HG_TEST_RPC_CB(region_release, handle): "
                                                        "buf_map_bulk_args memory allocation failed");
                        }
                        memset(buf_map_bulk_args, 0, sizeof(struct buf_map_release_bulk_args));
                        buf_map_bulk_args->handle               = handle;
                        buf_map_bulk_args->data_buf             = data_buf;
                        buf_map_bulk_args->in                   = in;
                        buf_map_bulk_args->remote_obj_id        = eltt->remote_obj_id;
                        buf_map_bulk_args->remote_reg_id        = eltt->remote_reg_id;
                        buf_map_bulk_args->remote_region_unit   = eltt->remote_region_unit;
                        buf_map_bulk_args->remote_region_nounit = eltt->remote_region_nounit;
                        buf_map_bulk_args->remote_client_id     = eltt->remote_client_id;
                        buf_map_bulk_args->remote_bulk_handle   = remote_bulk_handle;
#ifdef PDC_TIMING
                        buf_map_bulk_args->start_time = MPI_Wtime();
#endif
#ifdef ENABLE_MULTITHREAD
                        hg_thread_mutex_init(&(buf_map_bulk_args->work_mutex));
                        hg_thread_cond_init(&(buf_map_bulk_args->work_cond));
#endif
                        /* Pull bulk data */
                        size  = HG_Bulk_get_size(eltt->local_bulk_handle);
                        size2 = HG_Bulk_get_size(remote_bulk_handle);
                        if (size != size2) {
                            error = 1;
                            LOG_INFO("Local size %llu, remote %llu\n", size, size2);
                        }
                        hg_ret = HG_Bulk_transfer(hg_info->context, buf_map_region_release_bulk_transfer_cb,
                                                  buf_map_bulk_args, HG_BULK_PULL, eltt->local_addr,
                                                  eltt->local_bulk_handle, 0, remote_bulk_handle, 0, size,
                                                  HG_OP_ID_IGNORE);
                        if (hg_ret != HG_SUCCESS) {
                            error = 1;
                            PGOTO_ERROR(hg_ret, "buf map could not read bulk data");
                        }
                        break;
                    }
                }
                tmp = (region_list_t *)PDC_free(tmp);
                break;
            }
        }
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif
        request_region = (region_list_t *)PDC_free(request_region);

        if (dirty_reg == 0) {
            // Perform lock release function
            PDC_Data_Server_region_release(&in, &out);
            HG_Respond(handle, NULL, NULL, &out);
            HG_Free_input(handle, &in);
            HG_Destroy(handle);
        }
    }
done:
    if (error == 1) {
        out.ret = 0;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }
#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (in.access_type == PDC_READ) {
        pdc_server_timings->PDCreg_release_lock_read_rpc += end - start;
        pdc_timestamp_register(pdc_release_lock_read_timestamps, start, end);
    }
    else {
        pdc_server_timings->PDCreg_release_lock_write_rpc += end - start;
        pdc_timestamp_register(pdc_release_lock_write_timestamps, start, end);
    }

#endif
    FUNC_LEAVE(ret_value);
}

// region_lock_cb
HG_TEST_RPC_CB(region_lock, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t       ret_value = HG_SUCCESS;
    perr_t            ret       = SUCCEED;
    region_lock_in_t  in;
    region_lock_out_t out;
#ifdef PDC_TIMING
    double start, end;
#endif

#ifdef PDC_TIMING
    start = MPI_Wtime();
#endif
    HG_Get_input(handle, &in);

    // Perform lock function
    ret = PDC_Data_Server_region_lock(&in, &out, &handle);

    HG_Free_input(handle, &in);
    if (ret == SUCCEED) {
        HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
    }
#ifdef PDC_TIMING
    end = MPI_Wtime();

    if (in.access_type == PDC_READ) {
        pdc_server_timings->PDCreg_obtain_lock_read_rpc += end - start;
        pdc_timestamp_register(pdc_obtain_lock_read_timestamps, start, end);
    }
    else {
        pdc_server_timings->PDCreg_obtain_lock_write_rpc += end - start;
        pdc_timestamp_register(pdc_obtain_lock_write_timestamps, start, end);
    }
#endif
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// buf_unmap_cb(hg_handle_t handle)
HG_TEST_RPC_CB(buf_unmap, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value = HG_SUCCESS;
    perr_t                ret;
    buf_unmap_in_t        in;
    buf_unmap_out_t       out;
    const struct hg_info *info;
#ifdef PDC_TIMING
    double start, end;
#endif

#ifdef PDC_TIMING
    start = MPI_Wtime();
#endif
    // Decode input
    HG_Get_input(handle, &in);
    info    = HG_Get_info(handle);
    out.ret = 1;

    ret = PDC_Data_Server_buf_unmap(info, &in);
    if (ret != SUCCEED) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "Error with PDC_Data_Server_buf_unmap");
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }
    else {
        out.ret = 1;
        HG_Respond(handle, NULL, NULL, &out);
    }
    ret = PDC_Meta_Server_buf_unmap(&in, &handle);
    if (ret != SUCCEED)
        PGOTO_ERROR(HG_OTHER_ERROR, "Error with PDC_Meta_Server_buf_unmap");

done:
#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_server_timings->PDCbuf_obj_unmap_rpc += end - start;
    pdc_timestamp_register(pdc_buf_obj_unmap_timestamps, start, end);
#endif
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// get_remote_metadata_cb(hg_handle_t handle)
HG_TEST_RPC_CB(get_remote_metadata, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t               ret_value = HG_SUCCESS;
    get_remote_metadata_in_t  in;
    get_remote_metadata_out_t out;
    pdc_metadata_t *          meta;

    // Decode input
    HG_Get_input(handle, &in);

    meta = PDC_Server_get_obj_metadata(in.obj_id);
    if (meta != NULL)
        PDC_metadata_t_to_transfer_t(meta, &out.ret);
    else {
        out.ret.user_id       = -1;
        out.ret.data_type     = -1;
        out.ret.obj_id        = 0;
        out.ret.time_step     = -1;
        out.ret.obj_name      = "N/A";
        out.ret.app_name      = "N/A";
        out.ret.tags          = "N/A";
        out.ret.data_location = "N/A";
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
    FUNC_ENTER(NULL);

    hg_return_t       ret_value = HG_SUCCESS;
    buf_unmap_in_t    in;
    buf_unmap_out_t   out;
    pdc_metadata_t *  target_obj;
    region_buf_map_t *tmp, *elt;

    // Decode input
    HG_Get_input(handle, &in);

    target_obj = PDC_Server_get_obj_metadata(in.remote_obj_id);
    if (target_obj == NULL) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "Requested object does not exist");
    }
    out.ret = 1;
#ifdef ENABLE_MULTITHREAD

    hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
    DL_FOREACH_SAFE(target_obj->region_buf_map_head, elt, tmp)
    {
        if (in.remote_obj_id == elt->remote_obj_id &&
            PDC_region_info_transfer_t_is_equal(&(in.remote_region), &(elt->remote_region_unit))) {
            DL_DELETE(target_obj->region_buf_map_head, elt);
            elt     = (region_buf_map_t *)PDC_free(elt);
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
    FUNC_ENTER(NULL);

    hg_return_t       ret_value = HG_SUCCESS;
    buf_map_in_t      in;
    buf_map_out_t     out;
    pdc_metadata_t *  target_obj;
    region_list_t *   elt, *request_region;
    region_buf_map_t *buf_map_ptr;

    // Decode input
    HG_Get_input(handle, &in);

    target_obj = PDC_Server_get_obj_metadata(in.remote_obj_id);
    if (target_obj == NULL) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "Requested object (id=%" PRIu64 ") does not exist", in.remote_obj_id);
    }
    request_region = (region_list_t *)PDC_malloc(sizeof(region_list_t));
    PDC_region_transfer_t_to_list_t(&in.remote_region_unit, request_region);
    DL_FOREACH(target_obj->region_lock_head, elt)
    {
        if (PDC_is_same_region_list(elt, request_region) == 1) {
            hg_atomic_incr32(&(elt->buf_map_refcount));
        }
    }
    buf_map_ptr                  = (region_buf_map_t *)PDC_malloc(sizeof(region_buf_map_t));
    buf_map_ptr->local_reg_id    = in.local_reg_id;
    buf_map_ptr->local_region    = in.local_region;
    buf_map_ptr->local_ndim      = in.ndim;
    buf_map_ptr->local_data_type = in.local_type;

    buf_map_ptr->remote_obj_id        = in.remote_obj_id;
    buf_map_ptr->remote_ndim          = in.ndim;
    buf_map_ptr->remote_unit          = in.remote_unit;
    buf_map_ptr->remote_region_unit   = in.remote_region_unit;
    buf_map_ptr->remote_region_nounit = in.remote_region_nounit;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
    DL_APPEND(target_obj->region_buf_map_head, buf_map_ptr);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&meta_buf_map_mutex_g);
#endif

    out.ret        = 1;
    request_region = (region_list_t *)PDC_free(request_region);

done:
    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

// buf_map_cb(hg_handle_t handle)
HG_TEST_RPC_CB(buf_map, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value = HG_SUCCESS;
    perr_t                ret;
    buf_map_in_t          in;
    buf_map_out_t         out;
    const struct hg_info *info;
    region_list_t *       request_region;
    region_buf_map_t *    new_buf_map_ptr = NULL;
    void *                data_ptr;
    size_t                ndim;
#ifdef PDC_TIMING
    double start, end;
#endif

#ifdef PDC_TIMING
    start = MPI_Wtime();
#endif
    // Decode input
    HG_Get_input(handle, &in);

    // Use region dimension to allocate memory, rather than object dimension (different from client side)
    ndim = in.remote_region_unit.ndim;
    // allocate memory for the object by region size
    data_ptr = (void *)PDC_malloc(
        PDC_get_region_desc_size_bytes(in.remote_region_nounit.count, in.remote_unit, ndim));

    if (data_ptr == NULL) {
        out.ret = 0;
        PGOTO_ERROR(HG_OTHER_ERROR, "Object memory allocation failed");
    }

    request_region = (region_list_t *)PDC_malloc(sizeof(region_list_t));
    PDC_region_transfer_t_to_list_t(&in.remote_region_unit, request_region);

    info            = HG_Get_info(handle);
    new_buf_map_ptr = PDC_Data_Server_buf_map(info, &in, request_region, data_ptr);

    if (new_buf_map_ptr == NULL) {
        out.ret  = 1;
        data_ptr = (void *)PDC_free(data_ptr);
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }
    else {
        out.ret = 1;
        HG_Respond(handle, NULL, NULL, &out);
        ret = PDC_Meta_Server_buf_map(&in, new_buf_map_ptr, &handle);
        if (ret != SUCCEED)
            PGOTO_ERROR(HG_OTHER_ERROR, "Error with PDC_Meta_Server_buf_map");
    }
#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_server_timings->PDCbuf_obj_map_rpc += end - start;
    pdc_timestamp_register(pdc_buf_obj_map_timestamps, start, end);
#endif
done:
    FUNC_LEAVE(ret_value);
}

// Bulk
/* static hg_return_t */
/* query_partial_cb(hg_handle_t handle) */
// Server execute
HG_TEST_RPC_CB(query_partial, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value = HG_SUCCESS;
    hg_return_t                   hg_ret;
    hg_bulk_t                     bulk_handle = HG_BULK_NULL;
    uint32_t                      i;
    void **                       buf_ptrs;
    size_t *                      buf_sizes;
    uint32_t *                    n_meta_ptr, n_buf;
    metadata_query_transfer_in_t  in;
    metadata_query_transfer_out_t out;

    // Decode input
    HG_Get_input(handle, &in);

    out.ret = -1;

    n_meta_ptr = (uint32_t *)PDC_malloc(sizeof(uint32_t));

    stopwatch_t server_timer;
    timer_start(&server_timer);

    PDC_Server_get_partial_query_result(&in, n_meta_ptr, &buf_ptrs);

    timer_pause(&server_timer);
    out.server_time_elapsed       = (int64_t)timer_delta_us(&server_timer);
    out.server_memory_consumption = (int64_t)PDC_get_global_mem_usage();

    // No result found
    if (*n_meta_ptr == 0) {
        out.bulk_handle = HG_BULK_NULL;
        out.ret         = 0;
        ret_value       = HG_Respond(handle, NULL, NULL, &out);
        PGOTO_DONE(ret_value);
    }

    n_buf = *n_meta_ptr;

    buf_sizes = (size_t *)PDC_malloc((n_buf + 1) * sizeof(size_t));
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
    /*     padding = PDC_malloc(padding_size); */
    /*     memcpy(padding, buf_ptrs[0], sizeof(pdc_metadata_t)); */
    /*     buf_ptrs[*n_meta_ptr] = padding; */
    /*     buf_sizes[*n_meta_ptr] = padding_size; */
    /*     n_buf++; */
    /* } */

    // Fix when Mercury output in HG_Respond gets too large and cannot be transfered
    // hg_set_output(): Output size exceeds NA expected message size
    pdc_metadata_t *large_serial_meta_buf;
    if (*n_meta_ptr > 80) {
        large_serial_meta_buf = (pdc_metadata_t *)PDC_malloc(sizeof(pdc_metadata_t) * (*n_meta_ptr));
        for (i = 0; i < *n_meta_ptr; i++) {
            memcpy(&large_serial_meta_buf[i], buf_ptrs[i], sizeof(pdc_metadata_t));
        }
        buf_ptrs[0]  = large_serial_meta_buf;
        buf_sizes[0] = sizeof(pdc_metadata_t) * (*n_meta_ptr);
        n_buf        = 1;
    }

    // Create bulk handle
    hg_ret =
        HG_Bulk_create(hg_class_g, n_buf, buf_ptrs, (hg_size_t *)buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(HG_OTHER_ERROR, "Could not create bulk data handle");

    // Fill bulk handle and return number of metadata that satisfy the query
    out.bulk_handle = bulk_handle;
    out.ret         = *n_meta_ptr;

    // Send bulk handle to client
    ret_value = HG_Respond(handle, NULL, NULL, &out);

done:
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* query_kvtag_cb(hg_handle_t handle) */
// Server execute
HG_TEST_RPC_CB(query_kvtag, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value;
    hg_return_t                   hg_ret;
    hg_bulk_t                     bulk_handle = HG_BULK_NULL;
    uint64_t *                    buf_ptr;
    size_t                        buf_size[1];
    uint32_t                      nmeta;
    pdc_kvtag_t                   in;
    metadata_query_transfer_out_t out;

    // Decode input
    HG_Get_input(handle, &in);

    stopwatch_t server_timer;
    timer_start(&server_timer);

    ret_value = PDC_Server_get_kvtag_query_result(&in, &nmeta, &buf_ptr);

    timer_pause(&server_timer);
    out.server_time_elapsed       = (int64_t)timer_delta_us(&server_timer);
    out.server_memory_consumption = (int64_t)PDC_get_global_mem_usage();

    if (ret_value != SUCCEED || nmeta == 0) {
        out.bulk_handle = HG_BULK_NULL;
        out.ret         = 0;
        ret_value       = HG_Respond(handle, NULL, NULL, &out);
        PGOTO_DONE(ret_value);
    }

    // Create bulk handle
    buf_size[0] = nmeta * sizeof(uint64_t);
    hg_ret = HG_Bulk_create(hg_class_g, 1, (void **)&buf_ptr, (const hg_size_t *)&buf_size, HG_BULK_READ_ONLY,
                            &bulk_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(hg_ret, "Could not create bulk data handle");

    // Fill bulk handle and return number of metadata that satisfy the query
    out.bulk_handle = bulk_handle;
    out.ret         = nmeta;

    // Send bulk handle to client
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
    FUNC_ENTER(NULL);

    hg_return_t         ret_value         = HG_SUCCESS;
    struct bulk_args_t *bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t           local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    int                 cnt, i;
    uint64_t *          obj_id_ptr;
    pdc_int_ret_t       out_struct;
    void **             buf;
    void *              buf_1d;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS)
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    else {
        cnt = bulk_args->cnt;
        buf = (void **)PDC_calloc(sizeof(void *), cnt);

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf_1d, NULL, NULL);

        // Mercury combines the buffer into 1, so need to make it 2d ptr again
        buf[0] = buf_1d;
        buf[1] = buf_1d + sizeof(uint64_t);
        for (i = 2; i < cnt; i++) {
            buf[i] = buf[i - 1] + sizeof(update_region_storage_meta_bulk_t);
        }

        // Now we have the storage info in buf
        // First elem is the obj id, following by cnt region infos
        obj_id_ptr = (uint64_t *)buf[0];
        if (*obj_id_ptr <= 0)
            PGOTO_ERROR(HG_OTHER_ERROR, "Error with bulk access, obj id invalid");

        if (PDC_Server_update_region_storage_meta_bulk_local((update_region_storage_meta_bulk_t **)buf,
                                                             cnt) == SUCCEED) {

            buf            = (void **)PDC_free(buf);
            out_struct.ret = 9999;
        }
    } // end of else

done:
    HG_Bulk_free(local_bulk_handle);
    HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

/* bulk_rpc_cb(hg_handle_t handle) */
// Server execute after receives the bulk_rpc from another server
HG_TEST_RPC_CB(bulk_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value          = HG_SUCCESS;
    const struct hg_info *hg_info            = NULL;
    hg_bulk_t             origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t             local_bulk_handle  = HG_BULK_NULL;
    struct bulk_args_t *  bulk_args          = NULL;
    bulk_rpc_in_t         in_struct;
    int                   cnt;

    bulk_args = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    bulk_args->origin = in_struct.origin;

    /* Get parameters */
    cnt                = in_struct.cnt;
    origin_bulk_handle = in_struct.bulk_handle;

    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt    = cnt;

    LOG_INFO("bulk_rpc_cb, nbytes %lu\n", bulk_args->nbytes);

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value = HG_Bulk_transfer(hg_info->context, update_storage_meta_bulk_cb, bulk_args, HG_BULK_PULL,
                                 hg_info->addr, origin_bulk_handle, 0, local_bulk_handle, 0,
                                 bulk_args->nbytes, HG_OP_ID_IGNORE);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret_value);
}

// READ
/* static hg_return_t */
// data_server_read_cb(hg_handle_t handle)
HG_TEST_RPC_CB(data_server_read, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t            ret_value;
    data_server_read_in_t  in;
    data_server_read_out_t out;

    // Decode input
    HG_Get_input(handle, &in);

    data_server_io_info_t *io_info = (data_server_io_info_t *)PDC_malloc(sizeof(data_server_io_info_t));

    io_info->io_type          = PDC_READ;
    io_info->client_id        = in.client_id;
    io_info->nclient          = in.nclient;
    io_info->nbuffer_request  = in.nupdate;
    io_info->cache_percentage = in.cache_percentage;

    PDC_metadata_init(&io_info->meta);
    PDC_transfer_t_to_metadata_t(&(in.meta), &(io_info->meta));

    PDC_init_region_list(&(io_info->region));
    PDC_region_transfer_t_to_list_t(&(in.region), &(io_info->region));

    io_info->region.access_type   = io_info->io_type;
    io_info->region.meta          = &(io_info->meta);
    io_info->region.client_ids[0] = in.client_id;

    out.ret = 1;
    HG_Respond(handle, PDC_Server_data_io_via_shm, io_info, &out);

    ret_value = HG_SUCCESS;

    HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Destroy");

done:
    FUNC_LEAVE(ret_value);
}

// WRITE
// data_server_write_cb(hg_handle_t handle)
HG_TEST_RPC_CB(data_server_write, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t             ret_value;
    data_server_write_in_t  in;
    data_server_write_out_t out;

    HG_Get_input(handle, &in);

    data_server_io_info_t *io_info = (data_server_io_info_t *)PDC_malloc(sizeof(data_server_io_info_t));

    io_info->io_type         = PDC_WRITE;
    io_info->client_id       = in.client_id;
    io_info->nclient         = in.nclient;
    io_info->nbuffer_request = in.nupdate;

    PDC_metadata_init(&io_info->meta);
    PDC_transfer_t_to_metadata_t(&(in.meta), &(io_info->meta));

    PDC_init_region_list(&(io_info->region));
    PDC_region_transfer_t_to_list_t(&(in.region), &(io_info->region));

    strcpy(io_info->region.shm_addr, in.shm_addr);
    io_info->region.access_type   = io_info->io_type;
    io_info->region.meta          = &(io_info->meta);
    io_info->region.client_ids[0] = in.client_id;

    out.ret = 1;
    HG_Respond(handle, PDC_Server_data_io_via_shm, io_info, &out);

    ret_value = HG_SUCCESS;

    HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Destroy");

done:
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
    server_read_check_out_t *    read_out = PDC_calloc(1, sizeof(server_read_check_out_t));

    ret_value = HG_Get_input(handle, &in);

    PDC_Server_read_check(&in, read_out);

    out.ret      = read_out->ret;
    out.shm_addr = read_out->shm_addr;
    if (out.ret == 1 && read_out->is_cache_to_bb == 1) {
        // cache to bb with callback
        out.ret = 111; // tell client to close the shm region, as we will write it to BB

        ret_value = HG_Respond(handle, PDC_cache_region_to_bb_cb, read_out, &out);
    }
    else {
        ret_value = HG_Respond(handle, NULL, NULL, &out);
    }

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

// data_server_write_check_cb(hg_handle_t handle)
HG_TEST_RPC_CB(data_server_write_check, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    // Decode input
    data_server_write_check_in_t   in;
    data_server_write_check_out_t *out =
        (data_server_write_check_out_t *)PDC_calloc(sizeof(data_server_write_check_out_t), 1);

    ret_value = HG_Get_input(handle, &in);

    PDC_Server_write_check(&in, out);

    ret_value = HG_Respond(handle, NULL, NULL, out);
    // After returning the last write check finish to client, start the storage meta bulk update process

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* update_region_loc_cb */
HG_TEST_RPC_CB(update_region_loc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t             ret_value = HG_SUCCESS;
    update_region_loc_in_t  in;
    update_region_loc_out_t out;

    // Decode input
    HG_Get_input(handle, &in);

    region_list_t *input_region = (region_list_t *)PDC_malloc(sizeof(region_list_t));
    PDC_region_transfer_t_to_list_t(&in.region, input_region);
    strcpy(input_region->storage_location, in.storage_location);
    input_region->offset = in.offset;

    if (in.has_hist == 1) {

        input_region->region_hist = (pdc_histogram_t *)PDC_calloc(1, sizeof(pdc_histogram_t));
        PDC_copy_hist(input_region->region_hist, &in.hist);
    }

    out.ret = 20171031;

    ret_value = PDC_Server_update_local_region_storage_loc(input_region, in.obj_id, in.type);
    if (ret_value != SUCCEED) {
        out.ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "Failed to update region location: obj_id=%" PRIu64 "", in.obj_id);
    }

    /* HG_Respond(handle, NULL, NULL, &out); */
    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    input_region = (region_list_t *)PDC_free(input_region);

done:
    FUNC_LEAVE(ret_value);
}

/* get_metadata_by_id_cb */
HG_TEST_RPC_CB(get_metadata_by_id, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t              ret_value = HG_SUCCESS;
    get_metadata_by_id_in_t  in;
    get_metadata_by_id_out_t out;
    pdc_metadata_t *         target = NULL;

    // Decode input
    HG_Get_input(handle, &in);

    PDC_Server_get_local_metadata_by_id(in.obj_id, &target);

    if (target != NULL)
        PDC_metadata_t_to_transfer_t(target, &out.res_meta);
    else {
        LOG_INFO("No matching metadata of obj_id=%" PRIu64 "\n", in.obj_id);
        out.res_meta.user_id       = -1;
        out.res_meta.obj_id        = 0;
        out.res_meta.cont_id       = 0;
        out.res_meta.data_type     = -1;
        out.res_meta.time_step     = -1;
        out.res_meta.obj_name      = "N/A";
        out.res_meta.app_name      = "N/A";
        out.res_meta.tags          = "N/A";
        out.res_meta.data_location = "N/A";
    }

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* aggregate_write_cb */
HG_TEST_RPC_CB(aggregate_write, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value = HG_SUCCESS;
    pdc_aggregated_io_to_server_t in;
    pdc_int_ret_t                 out;
    region_list_t                 request_region;

    // Decode input
    HG_Get_input(handle, &in);
    PDC_init_region_list(&request_region);

    // Need to un-serialize regions from each client of the same node one by one
    out.ret = 1;

    HG_Respond(handle, NULL, NULL, &out);

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
    FUNC_ENTER(NULL);

    char *ret_value = NULL;
    int   k, levels_up = 0;
    char *appName = application;
    char *dotdot;

    while ((dotdot = strstr(appName, "../")) != NULL) {
        levels_up++;
        appName = dotdot + 3;
    }
    for (k = 0; k < levels_up; k++) {
        char *slash = strrchr(workingDir, '/');
        if (slash)
            *slash = 0;
    }
    k = strlen(workingDir);
    if (k >= PATH_MAX) {
        PGOTO_ERROR(NULL, "workingDir length exceeds PATH_MAX");
    }
    if ((appName[0] == '.') && (appName[1] == '/'))
        appName += 2;
    snprintf(&workingDir[k], PATH_MAX - k, "/%s", appName);

    ret_value = strdup(workingDir);

done:
    FUNC_LEAVE(ret_value);
}

char *
PDC_find_in_path(char *workingDir, char *application)
{
    FUNC_ENTER(NULL);

    struct stat fileStat;
    char *      ret_value = NULL;
    char *      pathVar   = getenv("PATH");
    char        colon     = ':';

    char  checkPath[PATH_MAX];
    char *next = strchr(pathVar, colon);
    int   offset;

    while (next) {
        *next++ = 0;
        snprintf(checkPath, sizeof(checkPath), "%s/%s", pathVar, application);
        if (stat(checkPath, &fileStat) == 0) {
            PGOTO_DONE(strdup(checkPath));
        }
        pathVar = next;
        next    = strchr(pathVar, colon);
    }
    if (application[0] == '.') {
        snprintf(checkPath, sizeof(checkPath), "%s/%s", workingDir, application);
        if (stat(checkPath, &fileStat) == 0) {
            char *foundPath = strrchr(checkPath, '/');
            char *appName   = foundPath + 1;
            if (foundPath == NULL) {
                PGOTO_DONE(remove_relative_dirs(workingDir, application));
            }
            *foundPath = 0;
            // Change directory (pushd) to the where we find the application
            if (chdir(checkPath) == 0) {
                if (getcwd(checkPath, sizeof(checkPath)) == NULL) {
                    LOG_ERROR("Path is too large\n");
                }

                offset = strlen(checkPath);
                // Change back (popd) to where we started
                if (chdir(workingDir) != 0) {
                    LOG_ERROR("Check dir failed\n");
                }
                snprintf(&checkPath[offset], sizeof(checkPath) - offset, "/%s", appName);
                PGOTO_DONE(strdup(checkPath));
            }
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

// Update container with objects
static hg_return_t
cont_add_del_objs_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    // Server executes after received request from client
    struct bulk_args_t *        bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t                   local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    int                         cnt, op;
    cont_add_del_objs_rpc_out_t out_struct;
    uint64_t *                  obj_ids, cont_id;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS)
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    else {
        cnt = bulk_args->cnt;

        op      = bulk_args->op;
        cont_id = bulk_args->cont_id;
        obj_ids = (uint64_t *)PDC_calloc(sizeof(uint64_t), cnt);

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, (void **)&obj_ids, NULL,
                       NULL);

        if (op == ADD_OBJ) {
            if (PDC_Server_container_add_objs(cnt, obj_ids, cont_id) == SUCCEED)
                out_struct.ret = 1;
            else
                PGOTO_ERROR(HG_OTHER_ERROR, "Error updating objects to container");
        }
        else if (op == DEL_OBJ) {
            if (PDC_Server_container_del_objs(cnt, obj_ids, cont_id) == SUCCEED)
                out_struct.ret = 1;
            else
                PGOTO_ERROR(HG_OTHER_ERROR, "Error updating objects to container");
        }
        else {
            out_struct.ret = 0;
            PGOTO_ERROR(HG_OTHER_ERROR, "Unsupported container operation type");
        }
    } // end of else

done:
    /* Free block handle */
    HG_Bulk_free(local_bulk_handle);
    HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

/* cont_add_del_objs_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(cont_add_del_objs_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                ret_value          = HG_SUCCESS;
    const struct hg_info *     hg_info            = NULL;
    struct bulk_args_t *       bulk_args          = NULL;
    hg_bulk_t                  origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t                  local_bulk_handle  = HG_BULK_NULL;
    cont_add_del_objs_rpc_in_t in_struct;

    bulk_args = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    bulk_args->origin = in_struct.origin;

    /* Get parameters */
    origin_bulk_handle = in_struct.bulk_handle;

    bulk_args->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt     = in_struct.cnt;
    bulk_args->op      = in_struct.op;
    bulk_args->cont_id = in_struct.cont_id;

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value =
        HG_Bulk_transfer(hg_info->context, cont_add_del_objs_bulk_cb, bulk_args, HG_BULK_PULL, hg_info->addr,
                         origin_bulk_handle, 0, local_bulk_handle, 0, bulk_args->nbytes, HG_OP_ID_IGNORE);

    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret_value);
}

/* cont_add_tags_rpc_cb*/
HG_TEST_RPC_CB(cont_add_tags_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t            ret_value = HG_SUCCESS;
    cont_add_tags_rpc_in_t in;
    pdc_int_ret_t          out;

    // Decode input
    HG_Get_input(handle, &in);
    if (PDC_Server_container_add_tags(in.cont_id, in.tags) != SUCCEED)
        out.ret = -1;
    else
        out.ret = 1;

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

// Update container with objects
static hg_return_t
query_read_obj_name_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    hg_return_t ret       = HG_SUCCESS;
    // Server executes after received request from client
    struct bulk_args_t *      bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t                 local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    int                       iter;
    size_t                    i;
    char *                    tmp_buf;
    query_read_obj_name_out_t out_struct;
    query_read_names_args_t * query_read_names_args;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    }
    else {
        query_read_names_args = (query_read_names_args_t *)PDC_calloc(1, sizeof(query_read_names_args_t));
        query_read_names_args->cnt           = bulk_args->cnt;
        query_read_names_args->client_seq_id = bulk_args->client_seq_id;
        query_read_names_args->client_id     = bulk_args->origin;
        query_read_names_args->is_select_all = 1;
        query_read_names_args->obj_names     = (char **)PDC_calloc(sizeof(char *), bulk_args->cnt);
        query_read_names_args->obj_names_1d  = (char *)PDC_calloc(sizeof(char), bulk_args->nbytes);

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, (void **)&tmp_buf, NULL,
                       NULL);
        memcpy(query_read_names_args->obj_names_1d, tmp_buf, bulk_args->nbytes);

        // Parse the obj_names to the 2d obj_names
        iter                                     = 0;
        query_read_names_args->obj_names[iter++] = query_read_names_args->obj_names_1d;
        for (i = 1; i < bulk_args->nbytes; i++) {
            if (query_read_names_args->obj_names_1d[i - 1] == '\0')
                query_read_names_args->obj_names[iter++] = &query_read_names_args->obj_names_1d[i];
        }
    }

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret = HG_Respond(bulk_args->handle, PDC_Server_query_read_names_cb, query_read_names_args, &out_struct);
    if (ret != HG_SUCCESS)
        PGOTO_ERROR(ret, "Could not respond");

    /* Free block handle */
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS)
        PGOTO_ERROR(ret, "Could not free HG bulk handle");

done:
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

/* query_read_obj_name_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(query_read_obj_name_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t              ret_value          = HG_SUCCESS;
    hg_return_t              ret                = HG_SUCCESS;
    const struct hg_info *   hg_info            = NULL;
    struct bulk_args_t *     bulk_args          = NULL;
    hg_bulk_t                origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t                local_bulk_handle  = HG_BULK_NULL;
    query_read_obj_name_in_t in_struct;

    bulk_args = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS)
        PGOTO_ERROR(ret, "Could not get input");

    origin_bulk_handle       = in_struct.bulk_handle;
    bulk_args->client_seq_id = in_struct.client_seq_id;
    bulk_args->nbytes        = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt           = in_struct.cnt;
    bulk_args->origin        = in_struct.origin;

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret = HG_Bulk_transfer(hg_info->context, query_read_obj_name_bulk_cb, bulk_args, HG_BULK_PULL,
                           hg_info->addr, origin_bulk_handle, 0, local_bulk_handle, 0, bulk_args->nbytes,

                           HG_OP_ID_IGNORE);
    if (ret != HG_SUCCESS)
        PGOTO_ERROR(ret, "Could not read bulk data");

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret_value);
}

// Receives the query with one name, return all storage metadata of the corresponding object with bulk
// transfer
/* storage_meta_name_query_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(storage_meta_name_query_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t ret = HG_SUCCESS;

    pdc_int_ret_t                 out;
    storage_meta_name_query_in_t  in;
    storage_meta_name_query_in_t *args;

    HG_Get_input(handle, &in);
    // Duplicate the structure so we can continue to use it after leaving this function
    args            = (storage_meta_name_query_in_t *)PDC_calloc(1, sizeof(storage_meta_name_query_in_t));
    args->obj_name  = strdup(in.obj_name);
    args->task_id   = in.task_id;
    args->origin_id = in.origin_id;

    out.ret = 1;
    HG_Respond(handle, PDC_Server_storage_meta_name_query_bulk_respond, args, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret);
}

// Generic function to check the return value (RPC receipt) is 1
hg_return_t
PDC_check_int_ret_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t   ret_value = HG_SUCCESS;
    pdc_int_ret_t output;

    hg_handle_t handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");

    if (output.ret != 1)
        PGOTO_ERROR(ret_value, "Return value [%d] is NOT expected", output.ret);

done:
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
get_storage_meta_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    // Server executes after received request from client
    struct bulk_args_t *bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t           local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    int                 i, task_id, n_regions;
    int *               int_ptr;
    char *              char_ptr, *file_path;
    uint64_t *          uint64_ptr, offset;

    void *                  buf;
    region_info_transfer_t *region_info_ptr;
    region_list_t *         region_list, *region_list_head = NULL;
    pdc_int_ret_t           out_struct;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    }
    else {
        n_regions = bulk_args->cnt;
        buf       = (void *)PDC_calloc(1, bulk_args->nbytes);

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, NULL, NULL);

        // buf_ptrs[0]: task_id

        // then for each next ptr, path, offset, region info (region_info_transfer_t)
        int_ptr = (int *)buf;
        task_id = *int_ptr;
        int_ptr++;

        for (i = 0; i < n_regions; i++) {
            char_ptr   = (char *)int_ptr;
            file_path  = char_ptr;
            uint64_ptr = (uint64_t *)(char_ptr + strlen(char_ptr) + 1);
            offset     = *uint64_ptr;
            uint64_ptr++;
            region_info_ptr = (region_info_transfer_t *)uint64_ptr;
            region_list     = (region_list_t *)PDC_calloc(1, sizeof(region_list_t));
            PDC_init_region_list(region_list);
            PDC_region_transfer_t_to_list_t(region_info_ptr, region_list);
            strcpy(region_list->storage_location, file_path);
            region_list->offset = offset;

            DL_APPEND(region_list_head, region_list);

            region_info_ptr++;
            int_ptr = (int *)region_info_ptr;
        }
    }

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret_value = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

    PDC_Server_proc_storage_meta_bulk(task_id, n_regions, region_list_head);

    /* Free block handle */
    ret_value = HG_Bulk_free(local_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

done:
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

/* get_storage_meta_name_query_bulk_result_rpc_cb */
HG_TEST_RPC_CB(get_storage_meta_name_query_bulk_result_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value          = HG_SUCCESS;
    const struct hg_info *hg_info            = NULL;
    struct bulk_args_t *  bulk_args          = NULL;
    hg_bulk_t             origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t             local_bulk_handle  = HG_BULK_NULL;
    bulk_rpc_in_t         in_struct;

    bulk_args = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */

    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    bulk_args->origin = in_struct.origin;

    origin_bulk_handle = in_struct.bulk_handle;

    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt    = in_struct.cnt;

    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value =
        HG_Bulk_transfer(hg_info->context, get_storage_meta_bulk_cb, bulk_args, HG_BULK_PULL, hg_info->addr,
                         origin_bulk_handle, 0, local_bulk_handle, 0, bulk_args->nbytes, HG_OP_ID_IGNORE);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
notify_client_multi_io_complete_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    // Client executes after received request from server
    struct bulk_args_t *bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t           local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    int                 n_shm;
    void *              buf;
    char *              buf_cp;
    pdc_int_ret_t       out_struct;

    out_struct.ret = 0;
    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    }
    else {
        n_shm = bulk_args->cnt;
        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, NULL, NULL);
        buf_cp = (char *)PDC_malloc(bulk_args->nbytes);
        memcpy(buf_cp, buf, bulk_args->nbytes);
    }

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret_value = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

    PDC_Client_query_read_complete(buf_cp, bulk_args->nbytes, n_shm, bulk_args->client_seq_id);

    /* Free block handle */
    ret_value = HG_Bulk_free(local_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

done:
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

/* notify_client_multi_io_complete_rpc_cb*/
HG_TEST_RPC_CB(notify_client_multi_io_complete_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value          = HG_SUCCESS;
    const struct hg_info *hg_info            = NULL;
    struct bulk_args_t *  bulk_args          = NULL;
    hg_bulk_t             origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t             local_bulk_handle  = HG_BULK_NULL;
    bulk_rpc_in_t         in_struct;

    bulk_args         = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));
    bulk_args->handle = handle;

    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    bulk_args->origin        = in_struct.origin;
    origin_bulk_handle       = in_struct.bulk_handle;
    bulk_args->nbytes        = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt           = in_struct.cnt;
    bulk_args->client_seq_id = in_struct.seq_id;

    hg_info = HG_Get_info(handle);
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value = HG_Bulk_transfer(hg_info->context, notify_client_multi_io_complete_bulk_cb, bulk_args,
                                 HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0, local_bulk_handle, 0,
                                 bulk_args->nbytes, HG_OP_ID_IGNORE);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

done:
    HG_Free_input(handle, &in_struct);

    FUNC_LEAVE(ret_value);
}

int
PDC_add_task_to_list(pdc_task_list_t **target_list, perr_t (*cb)(), void *cb_args, int *curr_task_id,
#ifdef ENABLE_MULTITHREAD
                     void *_mutex)
#else
                     void *_mutex ATTRIBUTE(unused))
#endif
{
    FUNC_ENTER(NULL);

    int              ret_value = 0;
    pdc_task_list_t *new_task;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_t *mutex = _mutex;

#endif

    if (target_list == NULL)
        PGOTO_ERROR(FAIL, "NULL input");

    new_task          = (pdc_task_list_t *)PDC_calloc(1, sizeof(pdc_task_list_t));
    new_task->cb      = cb;
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

    ret_value = new_task->task_id;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_del_task_from_list(pdc_task_list_t **target_list, pdc_task_list_t *del,
#ifdef ENABLE_MULTITHREAD
                       void *_mutex)
#else
                       void *_mutex ATTRIBUTE(unused))
#endif
{
    FUNC_ENTER(NULL);

    perr_t           ret_value = SUCCEED;
    pdc_task_list_t *tmp;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_t *mutex = _mutex;
#endif

    if (target_list == NULL || del == NULL)
        PGOTO_ERROR(FAIL, "NULL input");

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(mutex);
#endif

    tmp = del;
    DL_DELETE(*target_list, del);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(mutex);
#endif
    tmp = (pdc_task_list_t *)PDC_free(tmp);

done:
    FUNC_LEAVE(ret_value);
}

int
PDC_is_valid_task_id(int id)
{
    FUNC_ENTER(NULL);

    int ret_value = SUCCEED;
    if (id < PDC_SERVER_TASK_INIT_VALUE || id > 10000)
        PGOTO_ERROR(FAIL, "id %d is invalid", id);

done:
    FUNC_LEAVE(ret_value);
}

pdc_task_list_t *
PDC_find_task_from_list(pdc_task_list_t **target_list, int id,
#ifdef ENABLE_MULTITHREAD
                        void *_mutex)
#else
                        void *_mutex ATTRIBUTE(unused))
#endif
{
    FUNC_ENTER(NULL);

    pdc_task_list_t *ret_value = NULL;
    pdc_task_list_t *tmp;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_t *mutex = _mutex;
#endif

    if (PDC_is_valid_task_id(id) != 1)
        PGOTO_ERROR(NULL, "NULL input");

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(mutex);
#endif

    DL_FOREACH(*target_list, tmp)
    {
        if (tmp->task_id == id) {
            PGOTO_DONE(tmp);
        }
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(mutex);
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_del_task_from_list_id(pdc_task_list_t **target_list, int id, hg_thread_mutex_t *mutex)
{
    FUNC_ENTER(NULL);

    perr_t           ret_value = SUCCEED;
    pdc_task_list_t *tmp;

    if (target_list == NULL || PDC_is_valid_task_id(id) != 1)
        PGOTO_ERROR(FAIL, "NULL input");

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(mutex);
#endif

    tmp = PDC_find_task_from_list(target_list, id, mutex);
    DL_DELETE(*target_list, tmp);

#ifdef ENABLE_MULTITHREAD

    hg_thread_mutex_unlock(mutex);
#endif
    tmp = (pdc_task_list_t *)PDC_free(tmp);

done:
    FUNC_LEAVE(ret_value);
}

int
PDC_is_valid_obj_id(uint64_t id)
{
    FUNC_ENTER(NULL);

    int ret_value = SUCCEED;
    if (id < PDC_SERVER_ID_INTERVEL)
        PGOTO_ERROR(FAIL, "id %" PRIu64 " is invalid", id);

done:
    FUNC_LEAVE(ret_value);
}

/* an_attach_region_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(an_attach_region_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value = HG_SUCCESS;
    an_attach_region_in_t in;
    pdc_int_ret_t         out;

    HG_Get_input(handle, &in);

    perr_t an_ret =
        PDCan_store_attach_mapping(in.json_filepath, in.state_name, (pdcid_t)in.obj_id, in.offset, in.size,
                                   in.ndim, in.obj_ndim, in.obj_dims, (pdc_var_type_t)in.pdc_var_type);

    /* The object/region being attached may also have a PDC TF graph
     * attached client-side. That registration would otherwise never reach
     * the server for an analysis output -- it's normally a side effect of
     * a client write RPC's pdc_tf_pkg piggyback, and an analysis output is
     * only ever written by PDCan_exec_graph running server-side, never by
     * a client write RPC. Register it here instead, exactly as
     * transfer_request_cb would have. */
    perr_t tf_ret = SUCCEED;
    if (an_ret == SUCCEED && in.tf_json_filepath != NULL && strlen(in.tf_json_filepath) > 0) {
        tf_ret = PDCtf_store_json_mapping(in.obj_id, in.tf_json_filepath, in.tf_client_state,
                                          in.tf_client_state, in.tf_store_state, in.offset, in.size, in.ndim,
                                          (pdc_var_type_t)in.pdc_var_type);
        if (tf_ret != SUCCEED)
            LOG_ERROR("an_attach_region_rpc: failed to register PDC TF mapping \"%s\" for obj_id %" PRIu64
                      "\n",
                      in.tf_json_filepath, in.obj_id);
    }

    out.ret = (an_ret == SUCCEED && tf_ret == SUCCEED) ? 1 : -1;

    ret_value = HG_Respond(handle, NULL, NULL, &out);

    ret_value = HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* server_checkpoint_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(server_checkpoint_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t    ret_value = HG_SUCCESS;
    pdc_int_send_t in;
    pdc_int_ret_t  out;

    HG_Get_input(handle, &in);

    out.ret   = 1;
    ret_value = HG_Respond(handle, PDC_Server_checkpoint_cb, &in, &out);

    ret_value = HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* send_shm_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_shm, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t     ret_value = HG_SUCCESS;
    send_shm_in_t   in;
    pdc_int_ret_t   out;
    pdc_shm_info_t *shm_info;

    HG_Get_input(handle, &in);

    shm_info            = (pdc_shm_info_t *)PDC_calloc(sizeof(pdc_shm_info_t), 1);
    shm_info->client_id = in.client_id;
    shm_info->size      = in.size;
    strcpy(shm_info->shm_addr, in.shm_addr);

    out.ret   = 1;
    ret_value = HG_Respond(handle, PDC_Server_recv_shm_cb, shm_info, &out);

    ret_value = HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
query_read_obj_name_client_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    // Server executes after received request from client
    struct bulk_args_t *      bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t                 local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    int                       iter;
    size_t                    i;
    char *                    tmp_buf;
    query_read_obj_name_out_t out_struct;
    query_read_names_args_t * query_read_names_args;

    out_struct.ret = 0;

    if (hg_cb_info->ret != HG_SUCCESS) {
        HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    }
    else {
        query_read_names_args = (query_read_names_args_t *)PDC_calloc(1, sizeof(query_read_names_args_t));
        query_read_names_args->cnt           = bulk_args->cnt;
        query_read_names_args->client_seq_id = bulk_args->client_seq_id;
        query_read_names_args->client_id     = bulk_args->origin;
        query_read_names_args->is_select_all = 1;
        query_read_names_args->obj_names     = (char **)PDC_calloc(sizeof(char *), bulk_args->cnt);
        query_read_names_args->obj_names_1d  = (char *)PDC_calloc(sizeof(char), bulk_args->nbytes);

        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, (void **)&tmp_buf, NULL,
                       NULL);
        memcpy(query_read_names_args->obj_names_1d, tmp_buf, bulk_args->nbytes);

        // Parse the obj_names to the 2d obj_names
        iter                                     = 0;
        query_read_names_args->obj_names[iter++] = query_read_names_args->obj_names_1d;
        for (i = 1; i < bulk_args->nbytes; i++) {
            if (query_read_names_args->obj_names_1d[i - 1] == '\0')
                query_read_names_args->obj_names[iter++] = &query_read_names_args->obj_names_1d[i];
        }
    }

    out_struct.ret = 1;
    // Data server retrieve storage metadata and then read data to shared memory
    ret_value = HG_Respond(bulk_args->handle, PDC_Server_query_read_names_clinet_cb, query_read_names_args,
                           &out_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

    /* Free block handle */
    ret_value = HG_Bulk_free(local_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

done:
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

/* query_read_obj_name_client_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(query_read_obj_name_client_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t              ret_value          = HG_SUCCESS;
    const struct hg_info *   hg_info            = NULL;
    struct bulk_args_t *     bulk_args          = NULL;
    hg_bulk_t                origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t                local_bulk_handle  = HG_BULK_NULL;
    query_read_obj_name_in_t in_struct;

    bulk_args = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    origin_bulk_handle       = in_struct.bulk_handle;
    bulk_args->client_seq_id = in_struct.client_seq_id;
    bulk_args->nbytes        = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt           = in_struct.cnt;
    bulk_args->origin        = in_struct.origin;

    /* Create a new block handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value = HG_Bulk_transfer(hg_info->context, query_read_obj_name_client_bulk_cb, bulk_args,
                                 HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0, local_bulk_handle, 0,
                                 bulk_args->nbytes, HG_OP_ID_IGNORE);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

    HG_Free_input(handle, &in_struct);

done:
    FUNC_LEAVE(ret_value);
}

// Client receives bulk transfer rpc request, start transfer, then
// process the bulk data of all storage meta for a previous request
static hg_return_t
send_client_storage_meta_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                       ret_value         = HG_SUCCESS;
    struct bulk_args_t *              bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t                         local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    void *                            buf = NULL, *buf_cp = NULL;
    process_bulk_storage_meta_args_t *process_args = NULL;

    if (hg_cb_info->ret != HG_SUCCESS)
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    else {
        ret_value =
            HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READ_ONLY, 1, &buf, NULL, NULL);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value, "Error with bulk access");

        buf_cp = PDC_malloc(bulk_args->nbytes);
        memcpy(buf_cp, buf, bulk_args->nbytes);

        process_args =
            (process_bulk_storage_meta_args_t *)PDC_calloc(sizeof(process_bulk_storage_meta_args_t), 1);
        process_args->origin_id        = bulk_args->origin;
        process_args->n_storage_meta   = bulk_args->cnt;
        process_args->seq_id           = *((int *)buf_cp);
        process_args->all_storage_meta = (region_storage_meta_t *)(buf_cp + sizeof(int));
    } // end of else

    // Need to free buf_cp later
    PDC_Client_recv_bulk_storage_meta(process_args);

done:
    /* Free bulk handle */
    HG_Bulk_free(local_bulk_handle);
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

// Client receives bulk transfer request
/* send_client_storage_meta_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_client_storage_meta_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value          = HG_SUCCESS;
    const struct hg_info *hg_info            = NULL;
    hg_bulk_t             origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t             local_bulk_handle  = HG_BULK_NULL;
    struct bulk_args_t *  bulk_args          = NULL;
    bulk_rpc_in_t         in_struct;
    pdc_int_ret_t         out_struct;
    int                   cnt;

    bulk_args         = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    bulk_args->origin = in_struct.origin;

    /* Get parameters */
    cnt                = in_struct.cnt;
    origin_bulk_handle = in_struct.bulk_handle;

    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt    = cnt;

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value = HG_Bulk_transfer(hg_info->context, send_client_storage_meta_bulk_cb, bulk_args, HG_BULK_PULL,
                                 hg_info->addr, origin_bulk_handle, 0, local_bulk_handle, 0,
                                 bulk_args->nbytes, HG_OP_ID_IGNORE);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

    HG_Free_input(handle, &in_struct);

    /* Send response back */
    out_struct.ret = 1;
    ret_value      = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
server_recv_shm_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t         ret_value         = HG_SUCCESS;
    struct bulk_args_t *bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    hg_bulk_t           local_bulk_handle = hg_cb_info->info.bulk.local_handle;
    void *              buf = NULL, *buf_cp = NULL;

    if (hg_cb_info->ret != HG_SUCCESS)
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    else {
        ret_value =
            HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READ_ONLY, 1, &buf, NULL, NULL);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value, "Error with bulk access");

        buf_cp = PDC_malloc(bulk_args->nbytes);
        memcpy(buf_cp, buf, bulk_args->nbytes);

        // TODO now we have all storage info (region, shm_addr, offset, etc.) of data read by client
        // Insert them to the request list, and mark io_done
        PDC_Server_add_client_shm_to_cache(bulk_args->cnt, buf_cp);
    } // end else

done:
    /* Free bulk handle */
    HG_Bulk_free(local_bulk_handle);
    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

/* send_shm_bulk_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_shm_bulk_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value          = HG_SUCCESS;
    const struct hg_info *hg_info            = NULL;
    hg_bulk_t             origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t             local_bulk_handle  = HG_BULK_NULL;
    struct bulk_args_t *  bulk_args          = NULL;
    bulk_rpc_in_t         in_struct;
    pdc_int_ret_t         out_struct;
    int                   cnt;

    bulk_args = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));

    /* Keep handle to pass to callback */
    bulk_args->handle = handle;

    /* Get info from handle */
    hg_info = HG_Get_info(handle);

    /* Get input parameters and data */
    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    bulk_args->origin = in_struct.origin;

    /* Get parameters */
    cnt                = in_struct.cnt;
    origin_bulk_handle = in_struct.bulk_handle;
    bulk_args->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->cnt     = cnt;

    LOG_ERROR("send_bulk_rpc_cb, nbytes %lu\n", bulk_args->nbytes);

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_args->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value =
        HG_Bulk_transfer(hg_info->context, server_recv_shm_bulk_cb, bulk_args, HG_BULK_PULL, hg_info->addr,
                         origin_bulk_handle, 0, local_bulk_handle, 0, bulk_args->nbytes, HG_OP_ID_IGNORE);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

    HG_Free_input(handle, &in_struct);

    /* Send response back */
    out_struct.ret = 1;
    ret_value      = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

done:
    FUNC_LEAVE(ret_value);
}

/* send_data_query_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_data_query_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t      ret_value = HG_SUCCESS;
    pdc_query_xfer_t in, *query_xfer;
    pdc_int_ret_t    out;
    size_t           size;

    HG_Get_input(handle, &in);

    // Copy the received data
    size       = sizeof(pdc_query_xfer_t);
    query_xfer = (pdc_query_xfer_t *)PDC_malloc(size);
    memcpy(query_xfer, &in, size);

    size                    = sizeof(int) * query_xfer->n_combine_ops;
    query_xfer->combine_ops = (int *)PDC_malloc(size);
    memcpy(query_xfer->combine_ops, in.combine_ops, size);

    size                    = sizeof(pdc_query_constraint_t) * query_xfer->n_constraints;
    query_xfer->constraints = (pdc_query_constraint_t *)PDC_malloc(size);
    memcpy(query_xfer->constraints, in.constraints, size);

    out.ret   = 1;
    ret_value = HG_Respond(handle, PDC_Server_recv_data_query, query_xfer, &out);

    ret_value = HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* send_nhits_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_nhits, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t   ret_value = HG_SUCCESS;
    send_nhits_t  in, *in_cp;
    pdc_int_ret_t out;

    in_cp = (send_nhits_t *)PDC_malloc(sizeof(send_nhits_t));

    HG_Get_input(handle, &in);

    in_cp->nhits    = in.nhits;
    in_cp->query_id = in.query_id;

    out.ret   = 1;
    ret_value = HG_Respond(handle, PDC_recv_nhits, in_cp, &out);

    ret_value = HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* send_read_sel_obj_id_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_read_sel_obj_id_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value = HG_SUCCESS;
    get_sel_data_rpc_in_t in, *in_cp;
    pdc_int_ret_t         out;

    HG_Get_input(handle, &in);

    in_cp = (get_sel_data_rpc_in_t *)PDC_malloc(sizeof(get_sel_data_rpc_in_t));
    memcpy(in_cp, &in, sizeof(get_sel_data_rpc_in_t));

    out.ret   = 1;
    ret_value = HG_Respond(handle, PDC_Server_recv_read_sel_obj_data, in_cp, &out);

    ret_value = HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* get_sel_data_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(get_sel_data_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value = HG_SUCCESS;
    get_sel_data_rpc_in_t in, *in_cp;
    pdc_int_ret_t         out;

    HG_Get_input(handle, &in);

    in_cp = (get_sel_data_rpc_in_t *)PDC_malloc(sizeof(get_sel_data_rpc_in_t));
    memcpy(in_cp, &in, sizeof(get_sel_data_rpc_in_t));

    out.ret   = 1;
    ret_value = HG_Respond(handle, PDC_Server_recv_get_sel_data, in_cp, &out);

    ret_value = HG_Free_input(handle, &in);
    ret_value = HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

// Generic bulk transfer
/* send_bulk_rpc_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(send_bulk_rpc, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t           ret_value          = HG_SUCCESS;
    const struct hg_info *hg_info            = NULL;
    hg_bulk_t             origin_bulk_handle = HG_BULK_NULL;
    hg_bulk_t             local_bulk_handle  = HG_BULK_NULL;
    struct bulk_args_t *  bulk_arg           = NULL;
    bulk_rpc_in_t         in_struct;
    hg_return_t (*func_ptr)(const struct hg_cb_info *hg_cb_info);
    struct hg_cb_info callback_info;

    ret_value = HG_Get_input(handle, &in_struct);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get input");

    bulk_arg                = (struct bulk_args_t *)PDC_calloc(1, sizeof(struct bulk_args_t));
    bulk_arg->cnt           = in_struct.cnt;
    bulk_arg->total         = in_struct.total;
    bulk_arg->origin        = in_struct.origin;
    bulk_arg->query_id      = in_struct.seq_id;
    bulk_arg->client_seq_id = in_struct.seq_id2;
    bulk_arg->op            = in_struct.op_id;
    bulk_arg->ndim          = in_struct.ndim;
    bulk_arg->obj_id        = in_struct.obj_id;
    bulk_arg->data_type     = in_struct.data_type;
    origin_bulk_handle      = in_struct.bulk_handle;

    bulk_arg->handle = handle;

    if (in_struct.op_id == PDC_BULK_QUERY_COORDS) {
        func_ptr = &PDC_recv_coords;
    }
    else if (in_struct.op_id == PDC_BULK_READ_COORDS) {
        func_ptr = &PDC_recv_read_coords;
    }
    else if (in_struct.op_id == PDC_BULK_SEND_QUERY_DATA) {
        func_ptr = &PDC_recv_read_coords_data;
    }
    else if (in_struct.op_id == PDC_BULK_QUERY_METADATA) {
        func_ptr = &PDC_recv_query_metadata_bulk;
    }
    else
        PGOTO_ERROR(HG_OTHER_ERROR, "Invalid bulk op ID");

    if (in_struct.cnt > 0) {
        bulk_arg->nbytes = HG_Bulk_get_size(origin_bulk_handle);
        hg_info          = HG_Get_info(handle);
        HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_arg->nbytes, HG_BULK_READWRITE,
                       &local_bulk_handle);

        ret_value =
            HG_Bulk_transfer(hg_info->context, func_ptr, bulk_arg, HG_BULK_PULL, hg_info->addr,
                             origin_bulk_handle, 0, local_bulk_handle, 0, bulk_arg->nbytes, HG_OP_ID_IGNORE);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value, "Could not read bulk data");
    }
    else {
        callback_info.arg = bulk_arg;
        callback_info.ret = HG_SUCCESS;
        func_ptr(&callback_info);
    }

done:
    HG_Free_input(handle, &in_struct);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
/* dart_get_server_info_cb(hg_handle_t handle) */
HG_TEST_RPC_CB(dart_get_server_info, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                ret = HG_SUCCESS;
    dart_get_server_info_in_t  in;
    dart_get_server_info_out_t out;

    // Extract input from handle
    HG_Get_input(handle, &in);
    // retrieve server info from desigated server
    PDC_Server_dart_get_server_info(&in, &out);

    // Send response to client
    HG_Respond(handle, NULL, NULL, &out);
    // Free input
    HG_Free_input(handle, &in);
    // Free handle
    HG_Destroy(handle);

    return ret;
}

/* static hg_return_t */
// dart_perform_one_server_cb(hg_handle_t handle)
HG_TEST_RPC_CB(dart_perform_one_server, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value = HG_SUCCESS;
    hg_return_t                   hg_ret    = HG_SUCCESS;
    dart_perform_one_server_in_t  in;
    dart_perform_one_server_out_t out;

    hg_bulk_t  bulk_handle = HG_BULK_NULL;
    uint64_t * n_obj_ids_ptr;
    uint64_t   n_buf;
    uint64_t **buf_ptrs;
    size_t *   buf_sizes;
    uint32_t   i;

    // Extract input from handle
    HG_Get_input(handle, &in);

    n_obj_ids_ptr = (uint64_t *)PDC_calloc(1, sizeof(uint64_t));
    buf_ptrs      = (uint64_t **)PDC_calloc(1, sizeof(uint64_t *));

    stopwatch_t server_timer;
    timer_start(&server_timer);

    PDC_Server_dart_perform_one_server(&in, &out, n_obj_ids_ptr, buf_ptrs);

    timer_pause(&server_timer);
    out.server_time_elapsed       = (int64_t)timer_delta_us(&server_timer);
    out.server_memory_consumption = (int64_t)PDC_get_global_mem_usage();

    out.op_type = in.op_type;
    // No result found
    if (*n_obj_ids_ptr == 0) {
        out.bulk_handle = HG_BULK_NULL;
        out.ret         = 0;
        PGOTO_DONE(HG_Respond(handle, NULL, NULL, &out));
    }

    n_buf        = 1;
    buf_sizes    = (size_t *)PDC_calloc(n_buf, sizeof(size_t));
    buf_sizes[0] = sizeof(uint64_t) * (*n_obj_ids_ptr);

    // Create bulk handle
    hg_ret = HG_Bulk_create(hg_class_g, n_buf, (void **)buf_ptrs, (const hg_size_t *)buf_sizes,
                            HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        LOG_ERROR("Could not create bulk data handle\n");
        return EXIT_FAILURE;
    }

    // Fill bulk handle and return number of metadata that satisfy the query
    out.bulk_handle = bulk_handle;
    out.ret         = *n_obj_ids_ptr;

    // FIXME: Memory leak? buf_ptrs is not freed
    // TODO: To confirm how we can know the bulk data has been sent to client completely

    // Send bulk handle to client
    ret_value = HG_Respond(handle, NULL, NULL, &out);

done:
    // Free input
    HG_Free_input(handle, &in);
    // Free handle
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

HG_TEST_THREAD_CB(server_lookup_client)
HG_TEST_THREAD_CB(gen_obj_id)
HG_TEST_THREAD_CB(gen_cont_id)
HG_TEST_THREAD_CB(cont_add_del_objs_rpc)
HG_TEST_THREAD_CB(cont_add_tags_rpc)
HG_TEST_THREAD_CB(query_read_obj_name_rpc)
HG_TEST_THREAD_CB(storage_meta_name_query_rpc)
HG_TEST_THREAD_CB(get_storage_meta_name_query_bulk_result_rpc)
HG_TEST_THREAD_CB(notify_client_multi_io_complete_rpc)
HG_TEST_THREAD_CB(an_attach_region_rpc)
HG_TEST_THREAD_CB(server_checkpoint_rpc)
HG_TEST_THREAD_CB(send_shm)
HG_TEST_THREAD_CB(client_test_connect)
HG_TEST_THREAD_CB(metadata_query)
HG_TEST_THREAD_CB(container_query)
HG_TEST_THREAD_CB(metadata_delete)
HG_TEST_THREAD_CB(metadata_delete_by_id)
HG_TEST_THREAD_CB(metadata_update)
HG_TEST_THREAD_CB(notify_io_complete)
HG_TEST_THREAD_CB(notify_region_update)
HG_TEST_THREAD_CB(close_server)
HG_TEST_THREAD_CB(flush_obj)
HG_TEST_THREAD_CB(flush_obj_all)
HG_TEST_THREAD_CB(obj_reset_dims)
HG_TEST_THREAD_CB(region_lock)
HG_TEST_THREAD_CB(query_partial)
HG_TEST_THREAD_CB(query_kvtag)
HG_TEST_THREAD_CB(data_server_read)
HG_TEST_THREAD_CB(data_server_write)
HG_TEST_THREAD_CB(data_server_read_check)
HG_TEST_THREAD_CB(data_server_write_check)
HG_TEST_THREAD_CB(update_region_loc)
HG_TEST_THREAD_CB(get_metadata_by_id)
HG_TEST_THREAD_CB(aggregate_write)
HG_TEST_THREAD_CB(region_release)
HG_TEST_THREAD_CB(metadata_add_tag)
HG_TEST_THREAD_CB(metadata_add_kvtag)
HG_TEST_THREAD_CB(metadata_get_kvtag)
HG_TEST_THREAD_CB(metadata_del_kvtag)
HG_TEST_THREAD_CB(server_lookup_remote_server)
HG_TEST_THREAD_CB(bulk_rpc)
HG_TEST_THREAD_CB(buf_map)
HG_TEST_THREAD_CB(transfer_request)
HG_TEST_THREAD_CB(transfer_request_all)
HG_TEST_THREAD_CB(transfer_request_metadata_query)
HG_TEST_THREAD_CB(transfer_request_metadata_query2)
HG_TEST_THREAD_CB(transfer_request_status)
HG_TEST_THREAD_CB(transfer_request_wait_all)
HG_TEST_THREAD_CB(transfer_request_wait)
HG_TEST_THREAD_CB(get_remote_metadata)
HG_TEST_THREAD_CB(buf_map_server)
HG_TEST_THREAD_CB(buf_unmap_server)
HG_TEST_THREAD_CB(buf_unmap)
HG_TEST_THREAD_CB(query_read_obj_name_client_rpc)
HG_TEST_THREAD_CB(send_client_storage_meta_rpc)
HG_TEST_THREAD_CB(send_shm_bulk_rpc)
HG_TEST_THREAD_CB(send_data_query_rpc)
HG_TEST_THREAD_CB(send_rpc)

HG_TEST_THREAD_CB(send_nhits)
HG_TEST_THREAD_CB(send_bulk_rpc)
HG_TEST_THREAD_CB(get_sel_data_rpc)
HG_TEST_THREAD_CB(send_read_sel_obj_id_rpc)
HG_TEST_THREAD_CB(dart_get_server_info)
HG_TEST_THREAD_CB(dart_perform_one_server)

PDC_FUNC_DECLARE_REGISTER(gen_obj_id)
PDC_FUNC_DECLARE_REGISTER(gen_cont_id)
PDC_FUNC_DECLARE_REGISTER(server_lookup_client)
PDC_FUNC_DECLARE_REGISTER(server_lookup_remote_server)
PDC_FUNC_DECLARE_REGISTER(client_test_connect)
PDC_FUNC_DECLARE_REGISTER(notify_io_complete)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(send_shm_bulk_rpc, bulk_rpc_in_t, bulk_rpc_out_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(query_read_obj_name_client_rpc, query_read_obj_name_in_t,
                                 query_read_obj_name_out_t)
PDC_FUNC_DECLARE_REGISTER(notify_region_update)
PDC_FUNC_DECLARE_REGISTER(metadata_query)
PDC_FUNC_DECLARE_REGISTER(container_query)
PDC_FUNC_DECLARE_REGISTER(metadata_add_tag)
PDC_FUNC_DECLARE_REGISTER(send_rpc)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(metadata_del_kvtag, metadata_get_kvtag_in_t, metadata_add_tag_out_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(metadata_add_kvtag, metadata_add_kvtag_in_t, metadata_add_tag_out_t)
PDC_FUNC_DECLARE_REGISTER(metadata_get_kvtag)
PDC_FUNC_DECLARE_REGISTER(metadata_update)
PDC_FUNC_DECLARE_REGISTER(metadata_delete_by_id)
PDC_FUNC_DECLARE_REGISTER(metadata_delete)
PDC_FUNC_DECLARE_REGISTER(close_server)
PDC_FUNC_DECLARE_REGISTER(flush_obj)
PDC_FUNC_DECLARE_REGISTER(flush_obj_all)
PDC_FUNC_DECLARE_REGISTER(obj_reset_dims)
PDC_FUNC_DECLARE_REGISTER(transfer_request)
PDC_FUNC_DECLARE_REGISTER(transfer_request_all)
PDC_FUNC_DECLARE_REGISTER(transfer_request_metadata_query)
PDC_FUNC_DECLARE_REGISTER(transfer_request_metadata_query2)
PDC_FUNC_DECLARE_REGISTER(transfer_request_wait)
PDC_FUNC_DECLARE_REGISTER(transfer_request_wait_all)
PDC_FUNC_DECLARE_REGISTER(transfer_request_status)
PDC_FUNC_DECLARE_REGISTER(buf_map)
PDC_FUNC_DECLARE_REGISTER(get_remote_metadata)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(buf_map_server, buf_map_in_t, buf_map_out_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(buf_unmap_server, buf_unmap_in_t, buf_unmap_out_t)
PDC_FUNC_DECLARE_REGISTER(buf_unmap)
PDC_FUNC_DECLARE_REGISTER(region_lock)

PDC_FUNC_DECLARE_REGISTER_IN_OUT(region_release, region_lock_in_t, region_lock_out_t)

PDC_FUNC_DECLARE_REGISTER_IN_OUT(query_partial, metadata_query_transfer_in_t, metadata_query_transfer_out_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(query_kvtag, pdc_kvtag_t, metadata_query_transfer_out_t)
PDC_FUNC_DECLARE_REGISTER(bulk_rpc)
PDC_FUNC_DECLARE_REGISTER(data_server_read)
PDC_FUNC_DECLARE_REGISTER(data_server_write)
PDC_FUNC_DECLARE_REGISTER(data_server_read_check)

PDC_FUNC_DECLARE_REGISTER(data_server_write_check)
PDC_FUNC_DECLARE_REGISTER(update_region_loc)
PDC_FUNC_DECLARE_REGISTER(get_metadata_by_id)
PDC_FUNC_DECLARE_REGISTER(cont_add_del_objs_rpc)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(query_read_obj_name_rpc, query_read_obj_name_in_t, query_read_obj_name_out_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(storage_meta_name_query_rpc, storage_meta_name_query_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(get_storage_meta_name_query_bulk_result_rpc, bulk_rpc_in_t, pdc_int_ret_t)

PDC_FUNC_DECLARE_REGISTER_IN_OUT(an_attach_region_rpc, an_attach_region_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(server_checkpoint_rpc, pdc_int_send_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(send_shm, send_shm_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(cont_add_tags_rpc, cont_add_tags_rpc_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(notify_client_multi_io_complete_rpc, bulk_rpc_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(send_client_storage_meta_rpc, bulk_rpc_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(send_data_query_rpc, pdc_query_xfer_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(send_nhits, send_nhits_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(send_bulk_rpc, bulk_rpc_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(get_sel_data_rpc, get_sel_data_rpc_in_t, pdc_int_ret_t)
PDC_FUNC_DECLARE_REGISTER_IN_OUT(send_read_sel_obj_id_rpc, get_sel_data_rpc_in_t, pdc_int_ret_t)
// DART Index
PDC_FUNC_DECLARE_REGISTER(dart_get_server_info)
PDC_FUNC_DECLARE_REGISTER(dart_perform_one_server)

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
static int
is_overlap_1D(uint64_t xmin1, uint64_t xmax1, uint64_t xmin2, uint64_t xmax2)
{
    FUNC_ENTER(NULL);

    int ret_value = -1;

    if (xmax1 >= xmin2 && xmax2 >= xmin1) {
        ret_value = 1;
    }

    FUNC_LEAVE(ret_value);
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
static int
is_overlap_2D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t xmin2, uint64_t xmax2,
              uint64_t ymin2, uint64_t ymax2)
{
    FUNC_ENTER(NULL);

    int ret_value = -1;

    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1) {
        ret_value = 1;
    }

    FUNC_LEAVE(ret_value);
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
static int
is_overlap_3D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t zmin1, uint64_t zmax1,
              uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2, uint64_t zmin2, uint64_t zmax2)
{
    FUNC_ENTER(NULL);

    int ret_value = -1;
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1 &&
        is_overlap_1D(zmin1, zmax1, zmin2, zmax2) == 1) {
        ret_value = 1;
    }

    FUNC_LEAVE(ret_value);
}

int
PDC_is_contiguous_region_overlap(region_list_t *a, region_list_t *b)
{
    FUNC_ENTER(NULL);

    int      ret_value = 1;
    uint64_t xmin1 = 0, xmin2 = 0, xmax1 = 0, xmax2 = 0;
    uint64_t ymin1 = 0, ymin2 = 0, ymax1 = 0, ymax2 = 0;
    uint64_t zmin1 = 0, zmin2 = 0, zmax1 = 0, zmax2 = 0;

    if (a == NULL || b == NULL)
        PGOTO_ERROR(-1, "Passed NULL value");

    if (a->ndim != b->ndim || a->ndim <= 0 || b->ndim <= 0)
        PGOTO_ERROR(-1, "Dimension does not match");

    if (a->ndim >= 1) {
        xmin1 = a->start[0];
        xmax1 = a->start[0] + a->count[0] - 1;
        xmin2 = b->start[0];
        xmax2 = b->start[0] + b->count[0] - 1;
    }
    if (a->ndim >= 2) {
        ymin1 = a->start[1];
        ymax1 = a->start[1] + a->count[1] - 1;
        ymin2 = b->start[1];
        ymax2 = b->start[1] + b->count[1] - 1;
    }
    if (a->ndim >= 3) {
        zmin1 = a->start[2];
        zmax1 = a->start[2] + a->count[2] - 1;
        zmin2 = b->start[2];
        zmax2 = b->start[2] + b->count[2] - 1;
    }

    if (a->ndim == 1)
        ret_value = is_overlap_1D(xmin1, xmax1, xmin2, xmax2);
    else if (a->ndim == 2)
        ret_value = is_overlap_2D(xmin1, xmax1, ymin1, ymax1, xmin2, xmax2, ymin2, ymax2);
    else if (a->ndim == 3)
        ret_value =
            is_overlap_3D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2);

done:
    FUNC_LEAVE(ret_value);
}

int
PDC_is_contiguous_start_count_overlap(uint32_t ndim, uint64_t *a_start, uint64_t *a_count, uint64_t *b_start,
                                      uint64_t *b_count)
{
    FUNC_ENTER(NULL);

    int ret_value = 1;

    if (ndim > DIM_MAX || NULL == a_start || NULL == a_count || NULL == b_start || NULL == b_count)
        PGOTO_ERROR(-1, "Invalid input");

    uint64_t xmin1 = 0, xmin2 = 0, xmax1 = 0, xmax2 = 0;
    uint64_t ymin1 = 0, ymin2 = 0, ymax1 = 0, ymax2 = 0;
    uint64_t zmin1 = 0, zmin2 = 0, zmax1 = 0, zmax2 = 0;

    if (ndim >= 1) {
        xmin1 = a_start[0];
        xmax1 = a_start[0] + a_count[0] - 1;
        xmin2 = b_start[0];
        xmax2 = b_start[0] + b_count[0] - 1;
    }
    if (ndim >= 2) {
        ymin1 = a_start[1];
        ymax1 = a_start[1] + a_count[1] - 1;
        ymin2 = b_start[1];
        ymax2 = b_start[1] + b_count[1] - 1;
    }
    if (ndim >= 3) {
        zmin1 = a_start[2];
        zmax1 = a_start[2] + a_count[2] - 1;
        zmin2 = b_start[2];
        zmax2 = b_start[2] + b_count[2] - 1;
    }

    if (ndim == 1)
        ret_value = is_overlap_1D(xmin1, xmax1, xmin2, xmax2);
    else if (ndim == 2)
        ret_value = is_overlap_2D(xmin1, xmax1, ymin1, ymax1, xmin2, xmax2, ymin2, ymax2);
    else if (ndim == 3)

        ret_value =
            is_overlap_3D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2);

done:
    FUNC_LEAVE(ret_value);
}

perr_t

PDC_get_overlap_start_count(uint32_t ndim, uint64_t *start_a, uint64_t *count_a,

                            uint64_t *start_b, uint64_t *count_b, uint64_t *overlap_start,
                            uint64_t *overlap_count)
{
    FUNC_ENTER(NULL);

    perr_t   ret_value = SUCCEED;
    uint64_t i;

    if (NULL == start_a || NULL == count_a || NULL == start_b || NULL == count_b || NULL == overlap_start ||
        NULL == overlap_count)
        PGOTO_ERROR(FAIL, "get_overlap NULL input");

    // Check if they are truly overlapping regions
    if (PDC_is_contiguous_start_count_overlap(ndim, start_a, count_a, start_b, count_b) != 1) {
        LOG_INFO("non-overlap regions\n");
        for (i = 0; i < ndim; i++) {
            LOG_INFO("\t\tdim%" PRIu64 " - start_a: %" PRIu64 " count_a: %" PRIu64 ", "
                     "\t\tstart_b:%" PRIu64 " count_b:%" PRIu64 "\n",
                     i, start_a[i], count_a[i], start_b[i], count_b[i]);
        }
        PGOTO_DONE(FAIL);
    }

    for (i = 0; i < ndim; i++) {
        overlap_start[i] = PDC_MAX(start_a[i], start_b[i]);
        overlap_count[i] = PDC_MIN(start_a[i] + count_a[i], start_b[i] + count_b[i]) - overlap_start[i];
    }

done:
    if (ret_value == FAIL) {
        for (i = 0; i < ndim; i++) {
            overlap_start[i] = 0;
            overlap_count[i] = 0;
        }
    }
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_create_shm_segment_ind(uint64_t size, char *shm_addr, void **buf)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    retry;
    int    shm_fd = -1;

    if (shm_addr == NULL)
        PGOTO_ERROR(FAIL, "Shared memory addr is NULL");

    /* create the shared memory segment as if it was a file */
    retry = 0;
    srand(time(0));
    while (retry < PDC_MAX_TRIAL_NUM) {
        snprintf(shm_addr, ADDR_MAX, "/PDCshm%d", rand());
        shm_fd = shm_open(shm_addr, O_CREAT | O_RDWR, 0644);
        if (shm_fd != -1)
            break;
        retry++;
    }

    if (shm_fd == -1)
        PGOTO_ERROR(FAIL, "Shared memory create failed");

    /* configure the size of the shared memory segment */
    if (ftruncate(shm_fd, size) != 0) {
        PGOTO_ERROR(FAIL, "Truncate memory failed");
    }

    /* map the shared memory segment to the address space of the process */
    *buf = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (*buf == MAP_FAILED)
        PGOTO_ERROR(FAIL, "Shared memory mmap failed [%s]", shm_addr);
    // close and shm_unlink?

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_create_shm_segment(region_list_t *region)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    size_t i         = 0;
    int    retry;

    if (region->shm_addr[0] == 0)
        PGOTO_ERROR(FAIL, "Shared memory addr is NULL");

    /* create the shared memory segment as if it was a file */
    retry = 0;
    while (retry < PDC_MAX_TRIAL_NUM) {
        region->shm_fd = shm_open(region->shm_addr, O_CREAT | O_RDWR, 0644);
        if (region->shm_fd != -1)
            break;
        retry++;
    }

    if (region->shm_fd == -1)
        PGOTO_ERROR(FAIL, "Shared memory create failed");

    // Calculate the actual size for reading the data if needed
    if (region->data_size == 0) {
        region->data_size = region->count[0];
        for (i = 1; i < region->ndim; i++)
            region->data_size *= region->count[i];
    }

    /* configure the size of the shared memory segment */
    if (ftruncate(region->shm_fd, region->data_size) != 0) {
        PGOTO_ERROR(FAIL, "Truncate memory failed");
    }

    /* map the shared memory segment to the address space of the process */
    region->buf = mmap(0, region->data_size, PROT_READ | PROT_WRITE, MAP_SHARED, region->shm_fd, 0);
    if (region->buf == MAP_FAILED)
        PGOTO_ERROR(FAIL, "Shared memory mmap failed");
    // close and shm_unlink?

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_kvtag_dup(pdc_kvtag_t *from, pdc_kvtag_t **to)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (from == NULL || to == NULL)
        PGOTO_DONE(FAIL);

    (*to)        = (pdc_kvtag_t *)PDC_calloc(1, sizeof(pdc_kvtag_t));
    (*to)->name  = (char *)PDC_malloc(strlen(from->name) + 1);
    (*to)->size  = from->size;
    (*to)->type  = from->type;
    (*to)->value = (void *)PDC_malloc(from->size);
    memcpy((void *)(*to)->name, (void *)from->name, strlen(from->name) + 1);
    memcpy((void *)(*to)->value, (void *)from->value, from->size);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_free_kvtag(pdc_kvtag_t **kvtag)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    (*kvtag)->name  = (char *)PDC_free((*kvtag)->name);
    (*kvtag)->value = (void *)PDC_free((*kvtag)->value);
    *kvtag          = (pdc_kvtag_t *)PDC_free(*kvtag);
    *kvtag          = NULL;

    FUNC_LEAVE(ret_value);
}

// Query related

int
PDC_query_get_nnode(pdc_query_t *query)
{
    FUNC_ENTER(NULL);

    int ret_value = 0;

    if (NULL == query)
        ret_value = 0;
    else
        ret_value = 1 + PDC_query_get_nnode(query->left) + PDC_query_get_nnode(query->right);

    FUNC_LEAVE(ret_value);
}

void
PDC_query_get_nleaf(pdc_query_t *query, int *n)
{
    FUNC_ENTER(NULL);

    if (NULL == query)
        PGOTO_DONE_VOID;

    if (NULL == query->left && NULL == query->right) {
        (*n)++;
        PGOTO_DONE_VOID;
    }
    PDC_query_get_nleaf(query->left, n);
    PDC_query_get_nleaf(query->right, n);

done:
    FUNC_LEAVE_VOID();
}

void
serialize(pdc_query_t *root, int *combine_ops, int *cnt, pdc_query_constraint_t *constraints,
          int *constraint_cnt)
{
    FUNC_ENTER(NULL);

    if (root == NULL) {
        combine_ops[*cnt] = -1;
        (*cnt)++;
        PGOTO_DONE_VOID;
    }

    if (root->left == NULL && root->right == NULL) {
        memcpy(&constraints[*constraint_cnt], root->constraint, sizeof(pdc_query_constraint_t));
        (*constraint_cnt)++;
    }

    combine_ops[*cnt] = root->combine_op;
    (*cnt)++;
    serialize(root->left, combine_ops, cnt, constraints, constraint_cnt);
    serialize(root->right, combine_ops, cnt, constraints, constraint_cnt);

done:
    FUNC_LEAVE_VOID();
}

void
deSerialize(pdc_query_t **root, pdc_query_constraint_t *constraints, int *constraint_idx, int *combine_ops,
            int *order_idx)
{
    FUNC_ENTER(NULL);

    if (combine_ops[*order_idx] == -1) {
        (*order_idx)++;
        PGOTO_DONE_VOID;
    }

    *root               = (pdc_query_t *)PDC_calloc(1, sizeof(pdc_query_t));
    (*root)->combine_op = combine_ops[*order_idx];

    if (combine_ops[*order_idx] == 0) {
        // Current node is leaf
        (*root)->constraint = (pdc_query_constraint_t *)PDC_calloc(1, sizeof(pdc_query_constraint_t));
        memcpy((*root)->constraint, &constraints[*constraint_idx], sizeof(pdc_query_constraint_t));
        (*constraint_idx)++;
    }

    (*order_idx)++;

    deSerialize(&(*root)->left, constraints, constraint_idx, combine_ops, order_idx);
    deSerialize(&(*root)->right, constraints, constraint_idx, combine_ops, order_idx);

done:
    FUNC_LEAVE_VOID();
}

char pdcquery_combine_op_char_g[3][5] = {"NONE", "AND", "OR"};
char pdcquery_op_char_g[6][5]         = {"NONE", ">", "<", ">=", "<=", "=="};

void
print_query(pdc_query_t *query)
{
    FUNC_ENTER(NULL);

    if (query == NULL)
        PGOTO_DONE_VOID;

    if (query->left == NULL && query->right == NULL) {

        LOG_JUST_PRINT(" (%" PRIu64 " %s", query->constraint->obj_id,
                       pdcquery_op_char_g[query->constraint->op]);
        if (query->constraint->is_range == 1) {
            if (query->constraint->type == PDC_FLOAT)
                LOG_JUST_PRINT(" %.6f %s %.6f) ", (float)query->constraint->value,
                               pdcquery_op_char_g[query->constraint->op2], (float)query->constraint->value2);
            else if (query->constraint->type == PDC_DOUBLE)
                LOG_JUST_PRINT(" %.6f %s %.6f) ", (double)query->constraint->value,
                               pdcquery_op_char_g[query->constraint->op2], (double)query->constraint->value2);
            else if (query->constraint->type == PDC_INT)
                LOG_JUST_PRINT(" %d %s %d) ", (int)query->constraint->value,
                               pdcquery_op_char_g[query->constraint->op2], (int)query->constraint->value2);
            else if (query->constraint->type == PDC_UINT)
                LOG_JUST_PRINT(" %u %s %u) ", (unsigned)query->constraint->value,
                               pdcquery_op_char_g[query->constraint->op2],
                               (unsigned)query->constraint->value2);
            else if (query->constraint->type == PDC_INT64)
                LOG_JUST_PRINT(" %" PRId64 " %s %" PRId64 ")", (int64_t)query->constraint->value,
                               pdcquery_op_char_g[query->constraint->op2],
                               (int64_t)query->constraint->value2);
            else if (query->constraint->type == PDC_UINT64)
                LOG_JUST_PRINT(" %" PRId64 " %s %" PRId64 ") ", (uint64_t)query->constraint->value,
                               pdcquery_op_char_g[query->constraint->op2],
                               (uint64_t)query->constraint->value2);
        }
        else {
            if (query->constraint->type == PDC_FLOAT)
                LOG_JUST_PRINT(" %.6f) ", (float)query->constraint->value);
            else if (query->constraint->type == PDC_DOUBLE)
                LOG_JUST_PRINT(" %.6f) ", (double)query->constraint->value);
            else if (query->constraint->type == PDC_INT)
                LOG_JUST_PRINT(" %d) ", (int)query->constraint->value);
            else if (query->constraint->type == PDC_UINT)
                LOG_JUST_PRINT(" %u) ", (unsigned)query->constraint->value);
            else if (query->constraint->type == PDC_INT64)
                LOG_JUST_PRINT(" %" PRId64 ")", (int64_t)query->constraint->value);
            else if (query->constraint->type == PDC_UINT64)
                LOG_JUST_PRINT(" %" PRIu64 ") ", (uint64_t)query->constraint->value);
        }
        PGOTO_DONE_VOID;
    }

    LOG_JUST_PRINT("(");
    print_query(query->left);

    LOG_JUST_PRINT(" %s ", pdcquery_combine_op_char_g[query->combine_op]);

    print_query(query->right);
    LOG_JUST_PRINT(")");

done:
    FUNC_LEAVE_VOID();
}

void
PDCquery_print(pdc_query_t *query)
{
    FUNC_ENTER(NULL);

    uint32_t i;

    LOG_JUST_PRINT("Value selection: \n");
    print_query(query);
    LOG_JUST_PRINT("\n");
    if (query->region) {
        LOG_JUST_PRINT("Spatial selection: \n");
        LOG_JUST_PRINT("  ndim      = %lu\n", query->region->ndim);
        LOG_JUST_PRINT("  start    count\n");
        for (i = 0; i < query->region->ndim; i++) {
            LOG_JUST_PRINT("  %5" PRIu64 "    %5" PRIu64 "\n", query->region->offset[i],
                           query->region->size[i]);
        }
    }
    LOG_JUST_PRINT("\n");

    FUNC_LEAVE_VOID();
}

pdc_query_xfer_t *
PDC_serialize_query(pdc_query_t *query)
{
    FUNC_ENTER(NULL);

    pdc_query_xfer_t *ret_value = NULL;
    int               nnode, nleaf, ops_cnt, constraint_cnt;
    pdc_query_xfer_t *query_xfer;

    if (NULL == query)
        PGOTO_ERROR(NULL, "NULL input");

    query_xfer = (pdc_query_xfer_t *)PDC_calloc(1, sizeof(pdc_query_xfer_t));
    nnode      = PDC_query_get_nnode(query);

    nleaf = 0;
    PDC_query_get_nleaf(query, &nleaf);

    query_xfer->n_constraints = nleaf;
    query_xfer->constraints   = (pdc_query_constraint_t *)PDC_calloc(nleaf, sizeof(pdc_query_constraint_t));

    query_xfer->n_combine_ops = nnode * 2 + 1;
    query_xfer->combine_ops   = (int *)PDC_calloc(nnode * 2 + 1, sizeof(int));

    ops_cnt = constraint_cnt = 0;
    serialize(query, query_xfer->combine_ops, &ops_cnt, query_xfer->constraints, &constraint_cnt);

    if (NULL != query->region)
        PDC_region_info_t_to_transfer(query->region, &query_xfer->region);

    ret_value = query_xfer;
done:
    FUNC_LEAVE(ret_value);
}

pdc_query_t *
PDC_deserialize_query(pdc_query_xfer_t *query_xfer)
{
    FUNC_ENTER(NULL);

    pdc_query_t *ret_value      = NULL;
    int          constraint_idx = 0, order_idx = 0;
    pdc_query_t *new_root;

    if (NULL == query_xfer)
        PGOTO_ERROR(NULL, "NULL input");

    deSerialize(&new_root, query_xfer->constraints, &constraint_idx, query_xfer->combine_ops, &order_idx);
    new_root->region_constraint = (region_list_t *)PDC_malloc(sizeof(region_list_t));
    PDC_region_transfer_t_to_list_t(&query_xfer->region, new_root->region_constraint);

    ret_value = new_root;

done:
    FUNC_LEAVE(ret_value);
}

void
PDCquery_free(pdc_query_t *query)
{
    FUNC_ENTER(NULL);

    if (NULL == query)
        PGOTO_DONE_VOID;
    if (query->constraint)
        query->constraint = (pdc_query_constraint_t *)PDC_free(query->constraint);
    query = (pdc_query_t *)PDC_free(query);

done:
    FUNC_LEAVE_VOID();
}

void
PDCquery_free_all(pdc_query_t *root)
{
    FUNC_ENTER(NULL);

    if (NULL == root)
        PGOTO_DONE_VOID;

    if (root->sel && root->sel->coords_alloc > 0 && root->sel->coords != NULL) {
        root->sel->coords       = (uint64_t *)PDC_free(root->sel->coords);
        root->sel->coords_alloc = 0;
        root->sel->coords       = NULL;
    }

    if (root->left == NULL && root->right == NULL) {
        if (root->constraint) {
            root->constraint = (pdc_query_constraint_t *)PDC_free(root->constraint);
            root->constraint = NULL;
        }
    }

    PDCquery_free_all(root->left);
    PDCquery_free_all(root->right);

    root = (pdc_query_t *)PDC_free(root);

done:
    FUNC_LEAVE_VOID();
}

void
PDC_query_xfer_free(pdc_query_xfer_t *query_xfer)
{
    FUNC_ENTER(NULL);

    if (NULL != query_xfer) {
        query_xfer->combine_ops = (int *)PDC_free(query_xfer->combine_ops);
        query_xfer->constraints = (pdc_query_constraint_t *)PDC_free(query_xfer->constraints);
        query_xfer              = (pdc_query_xfer_t *)PDC_free(query_xfer);
    }

    FUNC_LEAVE_VOID();
}

void
PDCregion_free(struct pdc_region_info *region)
{
    FUNC_ENTER(NULL);

    if (region) {
        if (region->offset)
            region->offset = (uint64_t *)PDC_free(region->offset);
        if (region->size)
            region->size = (uint64_t *)PDC_free(region->size);
    }

    FUNC_LEAVE_VOID();
}

void
PDCselection_print(pdc_selection_t *sel)
{
    FUNC_ENTER(NULL);

    uint64_t i;

    LOG_JUST_PRINT("%" PRIu64 " hits, allocated %" PRIu64 " coordinates\n", sel->nhits, sel->coords_alloc);
    LOG_JUST_PRINT("Coordinates:\n");

    if (sel->nhits > 10) {
        for (i = 0; i < 10; i++)
            LOG_JUST_PRINT(" ,%" PRIu64 "", sel->coords[i]);
        LOG_JUST_PRINT(" , ...");
        for (i = sel->nhits - 10; i < sel->nhits; i++)
            LOG_JUST_PRINT(" ,%" PRIu64 "", sel->coords[i]);
    }
    else {
        for (i = 0; i < sel->nhits; i++)
            LOG_JUST_PRINT(" ,%" PRIu64 "", sel->coords[i]);
    }
    LOG_JUST_PRINT("\n\n");

    FUNC_LEAVE_VOID();
}

void
PDCselection_print_all(pdc_selection_t *sel)
{
    FUNC_ENTER(NULL);

    uint64_t i;

    LOG_JUST_PRINT("%" PRIu64 " hits, allocated %" PRIu64 " coordinates\n", sel->nhits, sel->coords_alloc);
    LOG_JUST_PRINT("Coordinates:\n");

    for (i = 0; i < sel->nhits; i++)
        LOG_JUST_PRINT(" ,%" PRIu64 "", sel->coords[i]);

    LOG_JUST_PRINT("\n\n");

    FUNC_LEAVE_VOID();
}
