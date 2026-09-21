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
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <inttypes.h>

#include "pdc_config.h"
#include "pdc_interface.h"
#include "pdc_utlist.h"
#include "pdc_malloc.h"
#include "pdc_server_rescale.h"
#include "pdc_server_metadata.h"
#include "pdc_client_server_common.h"
#include "pdc_logger.h"
#include "pdc_hash_table.h"
#include "pdc_region.h"
#include "pdc_server_region_transfer_metadata_query.h"
#include "bulki.h"
#include "bulki_serde.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

/* Keep in sync with pdc_server.c checkpoint bounds / magic */
#define PDC_CHECKPOINT_MAGIC_CURRENT      "PDC26.03"
#define PDC_CHECKPOINT_MAX_METADATA_COUNT 10000000
#define PDC_CHECKPOINT_MAX_KVTAG_KEY_LEN  65536
#define PDC_CHECKPOINT_MAX_KVTAG_SIZE     (1u << 24)
#define PDC_CHECKPOINT_MAX_REGION_COUNT   1000000
#define PDC_CHECKPOINT_MAX_HIST_NBIN      65536

typedef struct pdc_rescale_tmp_cont {
    uint32_t                     hash_key;
    int                          source_shard;
    pdc_cont_hash_table_entry_t *entry;
    struct pdc_rescale_tmp_cont *next;
} pdc_rescale_tmp_cont_t;

typedef struct pdc_rescale_tmp_tq_region {
    uint32_t                          data_server_id;
    uint64_t *                        reg_offset_size; /* ndim offsets then ndim sizes */
    struct pdc_rescale_tmp_tq_region *next;
} pdc_rescale_tmp_tq_region_t;

typedef struct pdc_rescale_tmp_tq_obj {
    uint64_t                       obj_id;
    int                            ndim;
    int                            source_shard;
    pdc_rescale_tmp_tq_region_t *  regions;
    pdc_rescale_tmp_tq_region_t *  regions_end;
    struct pdc_rescale_tmp_tq_obj *next;
} pdc_rescale_tmp_tq_obj_t;

/* Cached after successful T6 validation for T7 load planning */
static int rescale_validated_n_old_g = -1;
static int rescale_validated_n_new_g = -1;

/* Temporary holders (T7) — not published to hash tables until migrate (T8/T9) */
static pdc_metadata_t *          rescale_tmp_objs_g     = NULL;
static int                       rescale_tmp_n_obj_g    = 0;
static int                       rescale_tmp_n_region_g = 0;
static pdc_rescale_tmp_cont_t *  rescale_tmp_conts_g    = NULL;
static int                       rescale_tmp_n_cont_g   = 0;
static pdc_rescale_tmp_tq_obj_t *rescale_tmp_tq_g       = NULL;
static int                       rescale_tmp_n_tq_g     = 0;

/* Session-lifetime obj_id/cont_id → home_rank after elastic migrate (NULL = inactive). */
static HashTable *obj_id_home_map_g = NULL;

static unsigned int
rescale_obj_id_hash(void *vlocation)
{
    FUNC_ENTER(NULL);

    uint64_t v = *((uint64_t *)vlocation);

    FUNC_LEAVE((unsigned int)(v ^ (v >> 32)));
}

static int
rescale_obj_id_equal(void *vlocation1, void *vlocation2)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(*((uint64_t *)vlocation1) == *((uint64_t *)vlocation2));
}

static void
rescale_obj_id_key_free(void *key)
{
    FUNC_ENTER(NULL);
    key = (void *)PDC_free((uint64_t *)key);
    FUNC_LEAVE_VOID();
}

static void
rescale_obj_id_value_free(void *value)
{
    FUNC_ENTER(NULL);
    value = (void *)PDC_free((uint32_t *)value);
    FUNC_LEAVE_VOID();
}

static void
PDC_Server_rescale_free_obj_id_home_map(void)
{
    FUNC_ENTER(NULL);

    if (obj_id_home_map_g != NULL) {
        hash_table_free(obj_id_home_map_g);
        obj_id_home_map_g = NULL;
    }

    FUNC_LEAVE_VOID();
}

static perr_t
rescale_map_insert(uint64_t obj_id, uint32_t home)
{
    FUNC_ENTER(NULL);

    perr_t    ret_value = SUCCEED;
    uint64_t *key       = NULL;
    uint32_t *val       = NULL;

    if (obj_id_home_map_g == NULL)
        PGOTO_ERROR(FAIL, "obj_id home map not initialized");

    if (hash_table_lookup(obj_id_home_map_g, &obj_id) != NULL)
        PGOTO_DONE(SUCCEED);

    key = (uint64_t *)PDC_malloc(sizeof(uint64_t));
    val = (uint32_t *)PDC_malloc(sizeof(uint32_t));
    if (key == NULL || val == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate obj_id home map entry");
    *key = obj_id;
    *val = home;

    if (hash_table_insert(obj_id_home_map_g, key, val) != 1)
        PGOTO_ERROR(FAIL, "Failed to insert obj_id %" PRIu64 " -> home %u into map", obj_id, home);
    key = NULL;
    val = NULL;

done:
    if (ret_value != SUCCEED) {
        if (key != NULL)
            key = (uint64_t *)PDC_free(key);
        if (val != NULL)
            val = (uint32_t *)PDC_free(val);
    }
    FUNC_LEAVE(ret_value);
}

/*
 * After migrate, each rank owns exactly the metadata for home == my_rank.
 * Gather all local IDs cluster-wide so every rank can resolve any migrated ID.
 * Includes container IDs (same PDC_get_server_by_obj_id routing).
 */
static perr_t
PDC_Server_rescale_build_obj_id_home_map(int n_new)
{
    FUNC_ENTER(NULL);

    perr_t            ret_value   = SUCCEED;
    uint64_t *        local_ids   = NULL;
    uint64_t *        all_ids     = NULL;
    int *             recvcounts  = NULL;
    int *             displs      = NULL;
    int               local_count = 0;
    int               total_count = 0;
    int               i, r;
    HashTableIterator iter;
    HashTablePair     pair;

    PDC_Server_rescale_free_obj_id_home_map();

    obj_id_home_map_g = hash_table_new(rescale_obj_id_hash, rescale_obj_id_equal);
    if (obj_id_home_map_g == NULL)
        PGOTO_ERROR(FAIL, "Cannot create obj_id home map");
    hash_table_register_free_functions(obj_id_home_map_g, rescale_obj_id_key_free, rescale_obj_id_value_free);

    /* Count local object + container IDs. */
    if (metadata_hash_table_g != NULL) {
        hash_table_iterate(metadata_hash_table_g, &iter);
        while (hash_table_iter_has_more(&iter)) {
            pdc_hash_table_entry_head *head;
            pdc_metadata_t *           elt;

            pair = hash_table_iter_next(&iter);
            head = pair.value;
            DL_FOREACH(head->metadata, elt)
            {
                local_count++;
            }
        }
    }
    if (container_hash_table_g != NULL)
        local_count += (int)hash_table_num_entries(container_hash_table_g);

    if (local_count > 0) {
        local_ids = (uint64_t *)PDC_malloc((size_t)local_count * sizeof(uint64_t));
        if (local_ids == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate local obj_id list for home map");
    }

    i = 0;
    if (metadata_hash_table_g != NULL) {
        hash_table_iterate(metadata_hash_table_g, &iter);
        while (hash_table_iter_has_more(&iter)) {
            pdc_hash_table_entry_head *head;
            pdc_metadata_t *           elt;

            pair = hash_table_iter_next(&iter);
            head = pair.value;
            DL_FOREACH(head->metadata, elt)
            {
                if (i >= local_count)
                    PGOTO_ERROR(FAIL, "Local obj_id count mismatch while building home map");
                local_ids[i++] = elt->obj_id;
            }
        }
    }
    if (container_hash_table_g != NULL) {
        hash_table_iterate(container_hash_table_g, &iter);
        while (hash_table_iter_has_more(&iter)) {
            pdc_cont_hash_table_entry_t *cent;

            pair = hash_table_iter_next(&iter);
            cent = pair.value;
            if (i >= local_count)
                PGOTO_ERROR(FAIL, "Local cont_id count mismatch while building home map");
            local_ids[i++] = cent->cont_id;
        }
    }
    if (i != local_count)
        PGOTO_ERROR(FAIL, "Collected %d IDs but expected %d for home map", i, local_count);

#ifdef ENABLE_MPI
    recvcounts = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    displs     = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    if (recvcounts == NULL || displs == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate MPI state for obj_id home map");

    MPI_Allgather(&local_count, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    total_count = 0;
    for (r = 0; r < n_new; r++) {
        displs[r] = total_count;
        total_count += recvcounts[r];
    }

    if (total_count > 0) {
        all_ids = (uint64_t *)PDC_malloc((size_t)total_count * sizeof(uint64_t));
        if (all_ids == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate gathered obj_id list for home map");
    }

    {
        uint64_t dummy = 0;
        MPI_Allgatherv(local_count > 0 ? local_ids : &dummy, local_count, MPI_UINT64_T,
                       total_count > 0 ? all_ids : &dummy, recvcounts, displs, MPI_UINT64_T, MPI_COMM_WORLD);
    }

    for (r = 0; r < n_new; r++) {
        for (i = 0; i < recvcounts[r]; i++) {
            if (rescale_map_insert(all_ids[displs[r] + i], (uint32_t)r) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to insert gathered ID into home map");
        }
    }
#else
    (void)n_new;
    total_count = local_count;
    for (i = 0; i < local_count; i++) {
        if (rescale_map_insert(local_ids[i], 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to insert local ID into home map");
    }
#endif

    if (pdc_server_rank_g == 0)
        LOG_INFO("Built elastic obj_id home map with %d entries across %d ranks\n", total_count, n_new);

done:
    if (ret_value != SUCCEED)
        PDC_Server_rescale_free_obj_id_home_map();
    if (local_ids != NULL)
        local_ids = (uint64_t *)PDC_free(local_ids);
    if (all_ids != NULL)
        all_ids = (uint64_t *)PDC_free(all_ids);
    if (recvcounts != NULL)
        recvcounts = (int *)PDC_free(recvcounts);
    if (displs != NULL)
        displs = (int *)PDC_free(displs);
    FUNC_LEAVE(ret_value);
}

int
PDC_Server_lookup_obj_id_home(uint64_t obj_id, uint32_t *home_out)
{
    FUNC_ENTER(NULL);

    uint32_t *home;

    if (obj_id_home_map_g == NULL || home_out == NULL)
        FUNC_LEAVE(0);

    home = (uint32_t *)hash_table_lookup(obj_id_home_map_g, &obj_id);
    if (home == NULL)
        FUNC_LEAVE(0);

    *home_out = *home;
    FUNC_LEAVE(1);
}

perr_t
PDC_Server_unpublish_obj_id_home_map(void)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    char   map_fname[ADDR_MAX];

    if (pdc_server_rank_g != 0)
        PGOTO_DONE(SUCCEED);

    if (strpbrk(pdc_server_tmp_dir_g, ";&|`$<>") != NULL)
        PGOTO_ERROR(FAIL, "Invalid characters in server tmp dir path");

    snprintf(map_fname, ADDR_MAX, "%s%s", pdc_server_tmp_dir_g, pdc_obj_id_home_map_name_g);
    if (remove(map_fname) != 0 && errno != ENOENT)
        PGOTO_ERROR(FAIL, "Unable to remove stale obj_id home map [%s]: %s", map_fname, strerror(errno));

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_publish_obj_id_home_map(int n_new)
{
    FUNC_ENTER(NULL);

    perr_t                ret_value = SUCCEED;
    char                  map_fname[ADDR_MAX];
    FILE *                fp = NULL;
    pdc_obj_id_home_hdr_t hdr;
    pdc_obj_id_home_rec_t rec;
    HashTableIterator     iter;
    HashTablePair         pair;
    unsigned int          n_entries = 0;
    unsigned int          written   = 0;

    if (n_new <= 0)
        PGOTO_ERROR(FAIL, "Invalid n_new=%d for publishing obj_id home map", n_new);

    if (pdc_server_rank_g != 0)
        PGOTO_DONE(SUCCEED);

    if (obj_id_home_map_g == NULL)
        PGOTO_ERROR(FAIL, "Cannot publish obj_id home map: map not built");

    if (strpbrk(pdc_server_tmp_dir_g, ";&|`$<>") != NULL)
        PGOTO_ERROR(FAIL, "Invalid characters in server tmp dir path");

    n_entries = hash_table_num_entries(obj_id_home_map_g);

    snprintf(map_fname, ADDR_MAX, "%s%s", pdc_server_tmp_dir_g, pdc_obj_id_home_map_name_g);
    fp = fopen(map_fname, "wb");
    if (fp == NULL)
        PGOTO_ERROR(FAIL, "Cannot open obj_id home map for write [%s]: %s", map_fname, strerror(errno));

    memset(&hdr, 0, sizeof(hdr));
    hdr.magic     = PDC_OBJ_ID_HOME_MAP_MAGIC;
    hdr.version   = PDC_OBJ_ID_HOME_MAP_VERSION;
    hdr.n_server  = (uint32_t)n_new;
    hdr.n_entries = (uint64_t)n_entries;

    if (fwrite(&hdr, sizeof(hdr), 1, fp) != 1)
        PGOTO_ERROR(FAIL, "Failed writing obj_id home map header to [%s]", map_fname);

    hash_table_iterate(obj_id_home_map_g, &iter);
    while (hash_table_iter_has_more(&iter)) {
        pair = hash_table_iter_next(&iter);
        memset(&rec, 0, sizeof(rec));
        rec.id   = *((uint64_t *)pair.key);
        rec.home = *((uint32_t *)pair.value);
        if (fwrite(&rec, sizeof(rec), 1, fp) != 1)
            PGOTO_ERROR(FAIL, "Failed writing obj_id home map record to [%s]", map_fname);
        written++;
    }

    if (written != n_entries)
        PGOTO_ERROR(FAIL, "obj_id home map write count mismatch (%u vs %u)", written, n_entries);

    if (fflush(fp) != 0)
        PGOTO_ERROR(FAIL, "fflush failed for obj_id home map [%s]", map_fname);

    LOG_INFO("Published %s with %u entries (n_server=%d)\n", pdc_obj_id_home_map_name_g, n_entries, n_new);

done:
    if (fp != NULL)
        fclose(fp);
    if (ret_value != SUCCEED && pdc_server_rank_g == 0)
        (void)PDC_Server_unpublish_obj_id_home_map();
    FUNC_LEAVE(ret_value);
}

/*
 * Advance pdc_id_seq_g past any restored IDs that belong to this rank's
 * creation-rank interval: [(rank+1)*INTERVAL, (rank+2)*INTERVAL).
 * Foreign IDs homed here after elastic migrate do not move the sequence.
 */
static perr_t
PDC_Server_rescale_recompute_id_seq(void)
{
    FUNC_ENTER(NULL);

    perr_t            ret_value = SUCCEED;
    uint64_t          interval  = PDC_SERVER_ID_INTERVEL;
    uint64_t          base      = interval * (uint64_t)(pdc_server_rank_g + 1);
    uint64_t          limit     = base + interval;
    uint64_t          next_id   = base;
    HashTableIterator iter;
    HashTablePair     pair;
    int               n_in_interval = 0;

    if (metadata_hash_table_g != NULL) {
        hash_table_iterate(metadata_hash_table_g, &iter);
        while (hash_table_iter_has_more(&iter)) {
            pdc_hash_table_entry_head *head;
            pdc_metadata_t *           elt;

            pair = hash_table_iter_next(&iter);
            head = pair.value;
            DL_FOREACH(head->metadata, elt)
            {
                if (elt->obj_id >= base && elt->obj_id < limit) {
                    n_in_interval++;
                    if (elt->obj_id + 1 > next_id)
                        next_id = elt->obj_id + 1;
                }
            }
        }
    }

    if (container_hash_table_g != NULL) {
        hash_table_iterate(container_hash_table_g, &iter);
        while (hash_table_iter_has_more(&iter)) {
            pdc_cont_hash_table_entry_t *cent;

            pair = hash_table_iter_next(&iter);
            cent = pair.value;
            if (cent->cont_id >= base && cent->cont_id < limit) {
                n_in_interval++;
                if (cent->cont_id + 1 > next_id)
                    next_id = cent->cont_id + 1;
            }
        }
    }

    if (next_id >= limit)
        PGOTO_ERROR(
            FAIL, "Rank %d ID interval exhausted after elastic restore (next=%" PRIu64 ", limit=%" PRIu64 ")",
            pdc_server_rank_g, next_id, limit);

    pdc_id_seq_g = next_id;

    LOG_INFO("Rank %d recomputed pdc_id_seq_g=%" PRIu64 " after elastic restore "
             "(%d IDs in local creation interval [%" PRIu64 ", %" PRIu64 "))\n",
             pdc_server_rank_g, pdc_id_seq_g, n_in_interval, base, limit);

done:
    FUNC_LEAVE(ret_value);
}

static void
checkpoint_shard_path(char *buf, size_t buflen, int shard_rank)
{
    FUNC_ENTER(NULL);

    snprintf(buf, buflen, "%s/%d/metadata_checkpoint.%d", pdc_server_tmp_dir_g, shard_rank, shard_rank);

    FUNC_LEAVE_VOID();
}

static int
checkpoint_shard_exists(int shard_rank)
{
    FUNC_ENTER(NULL);

    char path[ADDR_MAX];

    checkpoint_shard_path(path, sizeof(path), shard_rank);

    FUNC_LEAVE(access(path, F_OK) == 0);
}

static int
rescale_loader_rank_for_shard(int shard, int n_old, int n_new)
{
    FUNC_ENTER(NULL);

    if (shard < 0 || shard >= n_old || n_new <= 0)
        FUNC_LEAVE(-1);

    if (shard < n_new)
        FUNC_LEAVE(shard);

    FUNC_LEAVE(shard % n_new);
}

static int
rescale_rank_should_load_shard(int shard, int n_old, int n_new, int my_rank)
{
    FUNC_ENTER(NULL);

    FUNC_LEAVE(rescale_loader_rank_for_shard(shard, n_old, n_new) == my_rank);
}

static int
rescale_region_cmp(region_list_t *a, region_list_t *b)
{
    FUNC_ENTER(NULL);

    size_t unit_size = sizeof(uint64_t) * a->ndim;

    FUNC_LEAVE(memcmp(a->start, b->start, unit_size));
}

static void
rescale_free_kvtag_list(pdc_kvtag_list_t *head)
{
    FUNC_ENTER(NULL);

    pdc_kvtag_list_t *elt, *tmp;

    DL_FOREACH_SAFE(head, elt, tmp)
    {
        DL_DELETE(head, elt);
        if (elt->kvtag) {
            elt->kvtag->name  = (char *)PDC_free(elt->kvtag->name);
            elt->kvtag->value = PDC_free(elt->kvtag->value);
            elt->kvtag        = (pdc_kvtag_t *)PDC_free(elt->kvtag);
        }
        elt = (pdc_kvtag_list_t *)PDC_free(elt);
    }

    FUNC_LEAVE_VOID();
}

static void
rescale_free_region_list(region_list_t *head)
{
    FUNC_ENTER(NULL);

    region_list_t *elt, *tmp;

    DL_FOREACH_SAFE(head, elt, tmp)
    {
        DL_DELETE(head, elt);
        if (elt->region_hist) {
            elt->region_hist->range = (double *)PDC_free(elt->region_hist->range);
            elt->region_hist->bin   = (uint64_t *)PDC_free(elt->region_hist->bin);
            elt->region_hist        = (pdc_histogram_t *)PDC_free(elt->region_hist);
        }
        elt = (region_list_t *)PDC_free(elt);
    }

    FUNC_LEAVE_VOID();
}

static void
rescale_free_one_metadata(pdc_metadata_t *obj)
{
    FUNC_ENTER(NULL);

    if (obj == NULL)
        FUNC_LEAVE_VOID();

    rescale_free_kvtag_list(obj->kvtag_list_head);
    rescale_free_region_list(obj->storage_region_list_head);
    obj = (pdc_metadata_t *)PDC_free(obj);

    FUNC_LEAVE_VOID();
}

static void
PDC_Server_rescale_free_tmp_conts(void)
{
    FUNC_ENTER(NULL);

    pdc_rescale_tmp_cont_t *cont, *cont_tmp;

    cont = rescale_tmp_conts_g;
    while (cont) {
        cont_tmp = cont->next;
        if (cont->entry) {
            cont->entry->obj_ids = (uint64_t *)PDC_free(cont->entry->obj_ids);
            cont->entry          = (pdc_cont_hash_table_entry_t *)PDC_free(cont->entry);
        }
        cont = (pdc_rescale_tmp_cont_t *)PDC_free(cont);
        cont = cont_tmp;
    }
    rescale_tmp_conts_g  = NULL;
    rescale_tmp_n_cont_g = 0;

    FUNC_LEAVE_VOID();
}

static void
PDC_Server_rescale_free_tmp_objs(void)
{
    FUNC_ENTER(NULL);

    pdc_metadata_t *obj, *obj_tmp;

    DL_FOREACH_SAFE(rescale_tmp_objs_g, obj, obj_tmp)
    {
        DL_DELETE(rescale_tmp_objs_g, obj);
        rescale_free_one_metadata(obj);
    }
    rescale_tmp_objs_g     = NULL;
    rescale_tmp_n_obj_g    = 0;
    rescale_tmp_n_region_g = 0;

    FUNC_LEAVE_VOID();
}

static void
rescale_free_one_tmp_tq(pdc_rescale_tmp_tq_obj_t *obj)
{
    FUNC_ENTER(NULL);

    pdc_rescale_tmp_tq_region_t *reg, *reg_tmp;

    if (obj == NULL)
        FUNC_LEAVE_VOID();

    reg = obj->regions;
    while (reg) {
        reg_tmp              = reg->next;
        reg->reg_offset_size = (uint64_t *)PDC_free(reg->reg_offset_size);
        reg                  = (pdc_rescale_tmp_tq_region_t *)PDC_free(reg);
        reg                  = reg_tmp;
    }
    obj = (pdc_rescale_tmp_tq_obj_t *)PDC_free(obj);

    FUNC_LEAVE_VOID();
}

static void
PDC_Server_rescale_free_tmp_tq(void)
{
    FUNC_ENTER(NULL);

    pdc_rescale_tmp_tq_obj_t *obj, *obj_tmp;

    obj = rescale_tmp_tq_g;
    while (obj) {
        obj_tmp = obj->next;
        rescale_free_one_tmp_tq(obj);
        obj = obj_tmp;
    }
    rescale_tmp_tq_g   = NULL;
    rescale_tmp_n_tq_g = 0;

    FUNC_LEAVE_VOID();
}

static void
PDC_Server_rescale_free_tmp_holders(void)
{
    FUNC_ENTER(NULL);

    PDC_Server_rescale_free_tmp_objs();
    PDC_Server_rescale_free_tmp_conts();
    PDC_Server_rescale_free_tmp_tq();

    FUNC_LEAVE_VOID();
}

static perr_t
rescale_append_region_from_bulki(BULKI *region_entry, pdc_metadata_t *metadata, int *n_region_out)
{
    FUNC_ENTER(NULL);

    perr_t         ret_value   = SUCCEED;
    region_list_t *region_list = NULL;
    unsigned       idx;
    int            has_hist;

    region_list = (region_list_t *)PDC_malloc(sizeof(region_list_t));
    if (region_list == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate region_list");

    BULKI_Entity *region_ent = BULKI_get(region_entry, BULKI_singleton_ENTITY("region", PDC_STRING));
    if (region_ent == NULL || region_ent->data == NULL)
        PGOTO_ERROR(FAIL, "Missing region in checkpoint");
    memcpy(region_list, region_ent->data, sizeof(region_list_t));
    region_list->region_hist = NULL;

    BULKI_Entity *has_hist_ent = BULKI_get(region_entry, BULKI_singleton_ENTITY("has_hist", PDC_STRING));
    if (has_hist_ent == NULL)
        PGOTO_ERROR(FAIL, "Missing has_hist in checkpoint");
    memcpy(&has_hist, has_hist_ent->data, sizeof(int));

    if (has_hist == 1) {
        BULKI_Entity *histogram_ent =
            BULKI_get(region_entry, BULKI_singleton_ENTITY("histogram", PDC_STRING));
        if (histogram_ent != NULL && histogram_ent->pdc_type == PDC_BULKI) {
            BULKI *histogram = (BULKI *)histogram_ent->data;
            int    nbin;

            region_list->region_hist = (pdc_histogram_t *)PDC_malloc(sizeof(pdc_histogram_t));
            if (region_list->region_hist == NULL)
                PGOTO_ERROR(FAIL, "Cannot allocate histogram");

            BULKI_Entity *dtype_ent = BULKI_get(histogram, BULKI_singleton_ENTITY("dtype", PDC_STRING));
            if (dtype_ent == NULL)
                PGOTO_ERROR(FAIL, "Missing histogram dtype");
            memcpy(&region_list->region_hist->dtype, dtype_ent->data, sizeof(int));

            BULKI_Entity *nbin_ent = BULKI_get(histogram, BULKI_singleton_ENTITY("nbin", PDC_STRING));
            if (nbin_ent == NULL)
                PGOTO_ERROR(FAIL, "Missing histogram nbin");
            memcpy(&nbin, nbin_ent->data, sizeof(int));
            if (nbin <= 0 || nbin > PDC_CHECKPOINT_MAX_HIST_NBIN)
                PGOTO_ERROR(FAIL, "Invalid histogram nbin %d", nbin);
            region_list->region_hist->nbin = nbin;

            BULKI_Entity *range_ent = BULKI_get(histogram, BULKI_singleton_ENTITY("range", PDC_STRING));
            if (range_ent == NULL || range_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing histogram range");
            region_list->region_hist->range = (double *)PDC_malloc(sizeof(double) * (size_t)nbin * 2);
            memcpy(region_list->region_hist->range, range_ent->data, sizeof(double) * (size_t)nbin * 2);

            BULKI_Entity *bin_ent = BULKI_get(histogram, BULKI_singleton_ENTITY("bin", PDC_STRING));
            if (bin_ent == NULL || bin_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing histogram bin");
            region_list->region_hist->bin = (uint64_t *)PDC_malloc(sizeof(uint64_t) * (size_t)nbin);
            memcpy(region_list->region_hist->bin, bin_ent->data, sizeof(uint64_t) * (size_t)nbin);

            BULKI_Entity *incr_ent = BULKI_get(histogram, BULKI_singleton_ENTITY("incr", PDC_STRING));
            if (incr_ent == NULL)
                PGOTO_ERROR(FAIL, "Missing histogram incr");
            memcpy(&region_list->region_hist->incr, incr_ent->data, sizeof(double));
        }
    }

    region_list->buf       = NULL;
    region_list->data_size = 1;
    for (idx = 0; idx < region_list->ndim; idx++)
        region_list->data_size *= region_list->count[idx];
    region_list->is_data_ready            = 0;
    region_list->shm_fd                   = 0;
    region_list->meta                     = metadata;
    region_list->prev                     = NULL;
    region_list->next                     = NULL;
    region_list->overlap_storage_regions  = NULL;
    region_list->n_overlap_storage_region = 0;
    hg_atomic_init32(&(region_list->buf_map_refcount), 0);
    region_list->reg_dirty_from_buf = 0;
    region_list->access_type        = PDC_NA;
    region_list->bulk_handle        = NULL;
    region_list->lock_handle        = NULL;
    region_list->addr               = NULL;
    region_list->obj_id             = metadata->obj_id;
    region_list->reg_id             = 0;
    region_list->from_obj_id        = 0;
    region_list->client_id          = 0;
    region_list->is_io_done         = 0;
    region_list->is_shm_closed      = 0;
    region_list->seq_id             = 0;
    region_list->sent_to_server     = 0;
    region_list->io_cache_region    = NULL;
    memset(region_list->shm_addr, 0, ADDR_MAX);
    memset(region_list->client_ids, 0, PDC_SERVER_MAX_PROC_PER_NODE * sizeof(uint32_t));

    if (strstr(region_list->storage_location, "/global/cscratch") != NULL)
        region_list->data_loc_type = PDC_LUSTRE;

    DL_APPEND(metadata->storage_region_list_head, region_list);
    if (n_region_out)
        (*n_region_out)++;
    region_list = NULL;

done:
    if (ret_value != SUCCEED && region_list != NULL) {
        if (region_list->region_hist) {
            region_list->region_hist->range = (double *)PDC_free(region_list->region_hist->range);
            region_list->region_hist->bin   = (uint64_t *)PDC_free(region_list->region_hist->bin);
            region_list->region_hist        = (pdc_histogram_t *)PDC_free(region_list->region_hist);
        }
        region_list = (region_list_t *)PDC_free(region_list);
    }
    FUNC_LEAVE(ret_value);
}

static perr_t
rescale_parse_obj_from_bulki(BULKI *metadata_obj, pdc_metadata_t **out_meta, int *n_region_out)
{
    FUNC_ENTER(NULL);

    perr_t          ret_value = SUCCEED;
    pdc_metadata_t *metadata  = NULL;

    if (out_meta == NULL)
        PGOTO_ERROR(FAIL, "out_meta is NULL");
    *out_meta = NULL;

    metadata = (pdc_metadata_t *)PDC_calloc(1, sizeof(pdc_metadata_t));
    if (metadata == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate metadata");

    BULKI_Entity *metadata_ent = BULKI_get(metadata_obj, BULKI_singleton_ENTITY("metadata", PDC_STRING));
    if (metadata_ent == NULL || metadata_ent->data == NULL)
        PGOTO_ERROR(FAIL, "Missing metadata in checkpoint");
    memcpy(metadata, metadata_ent->data, sizeof(pdc_metadata_t));

    metadata->storage_region_list_head       = NULL;
    metadata->region_lock_head               = NULL;
    metadata->region_map_head                = NULL;
    metadata->region_buf_map_head            = NULL;
    metadata->bloom                          = NULL;
    metadata->prev                           = NULL;
    metadata->next                           = NULL;
    metadata->kvtag_list_head                = NULL;
    metadata->all_storage_region_distributed = 0;

    BULKI_Entity *kvtags_array = BULKI_get(metadata_obj, BULKI_singleton_ENTITY("kvtags", PDC_STRING));
    if (kvtags_array != NULL && kvtags_array->pdc_type == PDC_BULKI) {
        BULKI_Entity_Iterator *kvtag_iter = Bent_iterator_init(kvtags_array, NULL, PDC_BULKI);
        while (Bent_iterator_has_next_BULKI(kvtag_iter)) {
            BULKI *           kvtag_entry = Bent_iterator_next_BULKI(kvtag_iter);
            pdc_kvtag_list_t *kvtag_list  = (pdc_kvtag_list_t *)PDC_calloc(1, sizeof(pdc_kvtag_list_t));
            uint32_t          kv_size;

            kvtag_list->kvtag = (pdc_kvtag_t *)PDC_malloc(sizeof(pdc_kvtag_t));

            BULKI_Entity *key_ent = BULKI_get(kvtag_entry, BULKI_singleton_ENTITY("key", PDC_STRING));
            if (key_ent == NULL || key_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Invalid kvtag key in checkpoint");
            {
                int key_len = (int)strlen((char *)key_ent->data) + 1;
                if (key_len <= 0 || key_len > PDC_CHECKPOINT_MAX_KVTAG_KEY_LEN)
                    PGOTO_ERROR(FAIL, "Invalid key_len %d in checkpoint", key_len);
                kvtag_list->kvtag->name = PDC_malloc((size_t)key_len);
                memcpy(kvtag_list->kvtag->name, key_ent->data, (size_t)key_len);
            }

            BULKI_Entity *size_ent = BULKI_get(kvtag_entry, BULKI_singleton_ENTITY("size", PDC_STRING));
            if (size_ent == NULL)
                PGOTO_ERROR(FAIL, "Missing kvtag size");
            memcpy(&kv_size, size_ent->data, sizeof(uint32_t));
            if (kv_size == 0 || kv_size > PDC_CHECKPOINT_MAX_KVTAG_SIZE)
                PGOTO_ERROR(FAIL, "Invalid kvtag size");
            kvtag_list->kvtag->size = kv_size;

            BULKI_Entity *type_ent = BULKI_get(kvtag_entry, BULKI_singleton_ENTITY("type", PDC_STRING));
            if (type_ent == NULL)
                PGOTO_ERROR(FAIL, "Missing kvtag type");
            memcpy(&kvtag_list->kvtag->type, type_ent->data, sizeof(int8_t));

            BULKI_Entity *value_ent = BULKI_get(kvtag_entry, BULKI_singleton_ENTITY("value", PDC_STRING));
            if (value_ent == NULL || value_ent->count != kv_size)
                PGOTO_ERROR(FAIL, "Invalid kvtag value size");
            kvtag_list->kvtag->value = PDC_malloc((size_t)kv_size);
            memcpy(kvtag_list->kvtag->value, value_ent->data, (size_t)kv_size);

            DL_APPEND(metadata->kvtag_list_head, kvtag_list);
        }
    }

    BULKI_Entity *regions_array = BULKI_get(metadata_obj, BULKI_singleton_ENTITY("regions", PDC_STRING));
    if (regions_array != NULL && regions_array->pdc_type == PDC_BULKI) {
        int n_region = regions_array->count;
        if (n_region < 0 || n_region > PDC_CHECKPOINT_MAX_REGION_COUNT)
            PGOTO_ERROR(FAIL, "Suspicious n_region value %d", n_region);

        BULKI_Entity_Iterator *region_iter = Bent_iterator_init(regions_array, NULL, PDC_BULKI);
        while (Bent_iterator_has_next_BULKI(region_iter)) {
            BULKI *region_entry = Bent_iterator_next_BULKI(region_iter);
            if (rescale_append_region_from_bulki(region_entry, metadata, n_region_out) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to restore region into temp holder");
        }
        DL_SORT(metadata->storage_region_list_head, rescale_region_cmp);
    }

    *out_meta = metadata;
    metadata  = NULL;

done:
    if (ret_value != SUCCEED && metadata != NULL)
        rescale_free_one_metadata(metadata);
    FUNC_LEAVE(ret_value);
}

static perr_t
rescale_append_obj_from_bulki(BULKI *metadata_obj, int source_shard, int *n_region_out)
{
    FUNC_ENTER(NULL);

    perr_t          ret_value = SUCCEED;
    pdc_metadata_t *metadata  = NULL;

    (void)source_shard;

    if (rescale_parse_obj_from_bulki(metadata_obj, &metadata, n_region_out) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to parse metadata object");

    DL_APPEND(rescale_tmp_objs_g, metadata);
    rescale_tmp_n_obj_g++;
    metadata = NULL;

done:
    if (ret_value != SUCCEED && metadata != NULL)
        rescale_free_one_metadata(metadata);
    FUNC_LEAVE(ret_value);
}

/* Pack one object into checkpoint metadata_obj shape (metadata + kvtags + regions). */
static BULKI *
rescale_pack_metadata_obj(pdc_metadata_t *elt)
{
    FUNC_ENTER(NULL);

    BULKI *           metadata_obj = NULL;
    pdc_kvtag_list_t *kvlist_elt;
    region_list_t *   region_elt;
    int               n_kvtag  = 0;
    int               n_region = 0;

    if (elt == NULL)
        FUNC_LEAVE(NULL);

    metadata_obj = BULKI_init(3);

    BULKI_put_incremental(metadata_obj, BULKI_singleton_ENTITY("metadata", PDC_STRING),
                          BULKI_ENTITY(elt, sizeof(pdc_metadata_t), PDC_UINT8, PDC_CLS_ARRAY));

    DL_COUNT(elt->kvtag_list_head, kvlist_elt, n_kvtag);
    {
        BULKI_Entity *kvtags_array = empty_BULKI_Array_Entity_with_capacity(n_kvtag > 0 ? n_kvtag : 1);
        DL_FOREACH(elt->kvtag_list_head, kvlist_elt)
        {
            BULKI *kvtag_entry = BULKI_init(4);

            BULKI_put_incremental(kvtag_entry, BULKI_singleton_ENTITY("key", PDC_STRING),
                                  BULKI_singleton_ENTITY(kvlist_elt->kvtag->name, PDC_STRING));
            BULKI_put_incremental(kvtag_entry, BULKI_singleton_ENTITY("size", PDC_STRING),
                                  BULKI_ENTITY(&kvlist_elt->kvtag->size, 1, PDC_UINT32, PDC_CLS_ITEM));
            BULKI_put_incremental(kvtag_entry, BULKI_singleton_ENTITY("type", PDC_STRING),
                                  BULKI_ENTITY(&kvlist_elt->kvtag->type, 1, PDC_INT8, PDC_CLS_ITEM));
            BULKI_put_incremental(
                kvtag_entry, BULKI_singleton_ENTITY("value", PDC_STRING),
                BULKI_ENTITY(kvlist_elt->kvtag->value, kvlist_elt->kvtag->size, PDC_UINT8, PDC_CLS_ARRAY));
            BULKI_ENTITY_append_BULKI_incremental(kvtags_array, kvtag_entry);
        }
        BULKI_put_incremental(metadata_obj, BULKI_singleton_ENTITY("kvtags", PDC_STRING), kvtags_array);
    }

    DL_COUNT(elt->storage_region_list_head, region_elt, n_region);
    {
        BULKI_Entity *regions_array = empty_BULKI_Array_Entity_with_capacity(n_region > 0 ? n_region : 1);
        DL_FOREACH(elt->storage_region_list_head, region_elt)
        {
            BULKI *region_entry = BULKI_init(3);
            int    has_hist     = (region_elt->region_hist != NULL) ? 1 : 0;

            BULKI_put_incremental(region_entry, BULKI_singleton_ENTITY("region", PDC_STRING),
                                  BULKI_ENTITY(region_elt, sizeof(region_list_t), PDC_UINT8, PDC_CLS_ARRAY));
            BULKI_put_incremental(region_entry, BULKI_singleton_ENTITY("has_hist", PDC_STRING),
                                  BULKI_ENTITY(&has_hist, 1, PDC_INT, PDC_CLS_ITEM));

            if (has_hist == 1) {
                BULKI *histogram = BULKI_init(5);

                BULKI_put_incremental(
                    histogram, BULKI_singleton_ENTITY("dtype", PDC_STRING),
                    BULKI_ENTITY(&region_elt->region_hist->dtype, 1, PDC_INT, PDC_CLS_ITEM));
                BULKI_put_incremental(histogram, BULKI_singleton_ENTITY("nbin", PDC_STRING),
                                      BULKI_ENTITY(&region_elt->region_hist->nbin, 1, PDC_INT, PDC_CLS_ITEM));
                BULKI_put_incremental(histogram, BULKI_singleton_ENTITY("range", PDC_STRING),
                                      BULKI_ENTITY(region_elt->region_hist->range,
                                                   region_elt->region_hist->nbin * 2, PDC_DOUBLE,
                                                   PDC_CLS_ARRAY));
                BULKI_put_incremental(histogram, BULKI_singleton_ENTITY("bin", PDC_STRING),
                                      BULKI_ENTITY(region_elt->region_hist->bin,
                                                   region_elt->region_hist->nbin, PDC_UINT64, PDC_CLS_ARRAY));
                BULKI_put_incremental(
                    histogram, BULKI_singleton_ENTITY("incr", PDC_STRING),
                    BULKI_ENTITY(&region_elt->region_hist->incr, 1, PDC_DOUBLE, PDC_CLS_ITEM));
                BULKI_put_incremental(region_entry, BULKI_singleton_ENTITY("histogram", PDC_STRING),
                                      BULKI_ENTITY(histogram, 1, PDC_BULKI, PDC_CLS_ITEM));
            }

            BULKI_ENTITY_append_BULKI_incremental(regions_array, region_entry);
        }
        BULKI_put_incremental(metadata_obj, BULKI_singleton_ENTITY("regions", PDC_STRING), regions_array);
    }

    FUNC_LEAVE(metadata_obj);
}

static perr_t
rescale_insert_metadata(pdc_metadata_t *metadata)
{
    FUNC_ENTER(NULL);

    perr_t                     ret_value    = SUCCEED;
    uint32_t *                 hash_key     = NULL;
    pdc_hash_table_entry_head *lookup_value = NULL;
    pdc_hash_table_entry_head *entry        = NULL;
    uint32_t                   hash_value;

    if (metadata == NULL)
        PGOTO_ERROR(FAIL, "metadata is NULL");
    if (metadata_hash_table_g == NULL)
        PGOTO_ERROR(FAIL, "metadata_hash_table_g not initialized");

    hash_value   = PDC_get_hash_by_name(metadata->obj_name);
    lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_value);

    if (lookup_value != NULL) {
        if (PDC_Server_hash_table_list_insert(lookup_value, metadata) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error inserting metadata into existing hash entry");
    }
    else {
        hash_key = (uint32_t *)PDC_malloc(sizeof(uint32_t));
        if (hash_key == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate hash_key");
        *hash_key = hash_value;

        entry = (pdc_hash_table_entry_head *)PDC_malloc(sizeof(pdc_hash_table_entry_head));
        if (entry == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate hash table entry");
        entry->bloom    = NULL;
        entry->metadata = NULL;
        entry->n_obj    = 0;

        if (PDC_Server_hash_table_list_init(entry, hash_key) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Server_hash_table_list_init");
        hash_key = NULL;

        if (PDC_Server_hash_table_list_insert(entry, metadata) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error inserting metadata into new hash entry");
        entry = NULL;
    }

    n_metadata_g++;

done:
    if (ret_value != SUCCEED) {
        if (hash_key != NULL)
            hash_key = (uint32_t *)PDC_free(hash_key);
        if (entry != NULL)
            entry = (pdc_hash_table_entry_head *)PDC_free(entry);
    }
    FUNC_LEAVE(ret_value);
}

static perr_t
rescale_ingest_obj_bundle(void *buf, int buf_size, int *n_inserted)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    BULKI *bundle    = NULL;

    if (buf_size == 0)
        PGOTO_DONE(SUCCEED);
    if (buf == NULL || buf_size < 0)
        PGOTO_ERROR(FAIL, "Invalid object migration bundle");

    bundle = BULKI_deserialize(buf);
    if (bundle == NULL)
        PGOTO_ERROR(FAIL, "Failed to deserialize object migration bundle");

    BULKI_Entity *objs_array = BULKI_get(bundle, BULKI_singleton_ENTITY("metadata_objects", PDC_STRING));
    if (objs_array == NULL || objs_array->pdc_type != PDC_BULKI)
        PGOTO_ERROR(FAIL, "Missing metadata_objects in migration bundle");

    {
        BULKI_Entity_Iterator *obj_iter = Bent_iterator_init(objs_array, NULL, PDC_BULKI);
        while (Bent_iterator_has_next_BULKI(obj_iter)) {
            BULKI *         metadata_obj = Bent_iterator_next_BULKI(obj_iter);
            pdc_metadata_t *metadata     = NULL;

            if (rescale_parse_obj_from_bulki(metadata_obj, &metadata, NULL) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to parse migrated metadata object");
            if (rescale_insert_metadata(metadata) != SUCCEED) {
                rescale_free_one_metadata(metadata);
                PGOTO_ERROR(FAIL, "Failed to insert migrated metadata object");
            }
            if (n_inserted)
                (*n_inserted)++;
        }
    }

done:
    if (bundle != NULL)
        BULKI_free(bundle, 1);
    FUNC_LEAVE(ret_value);
}

/*
 * Migrate temp objects to N_new home ranks; insert into metadata_hash_table_g.
 * Containers remain in temp holders until PDC_Server_rescale_migrate_containers.
 */
static perr_t
PDC_Server_rescale_migrate_objects(int n_new)
{
    FUNC_ENTER(NULL);

    perr_t          ret_value     = SUCCEED;
    pdc_metadata_t *obj           = NULL;
    pdc_metadata_t *obj_tmp       = NULL;
    int             before_local  = 0;
    int             before_global = 0;
    int             after_local   = 0;
    int             after_global  = 0;
    int             n_inserted    = 0;
#ifdef ENABLE_MPI
    int *          sendcounts   = NULL;
    int *          recvcounts   = NULL;
    int *          sdispls      = NULL;
    int *          rdispls      = NULL;
    int *          dest_nobj    = NULL;
    void **        dest_bufs    = NULL;
    BULKI **       dest_bundles = NULL;
    BULKI_Entity **dest_arrays  = NULL;
    char *         sendbuf      = NULL;
    char *         recvbuf      = NULL;
    int            total_send   = 0;
    int            total_recv   = 0;
    int            r;
#endif

    before_local = rescale_tmp_n_obj_g;
#ifdef ENABLE_MPI
    MPI_Allreduce(&before_local, &before_global, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#else
    before_global = before_local;
#endif

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic object migrate: %d objects before exchange (N_new=%d)\n", before_global, n_new);

    if (metadata_hash_table_g == NULL) {
        if (PDC_Server_init_hash_table() != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Server_init_hash_table during elastic migrate");
    }

#ifndef ENABLE_MPI
    /* Non-MPI: n_new must be 1; insert all local objects. */
    (void)n_new;
    DL_FOREACH_SAFE(rescale_tmp_objs_g, obj, obj_tmp)
    {
        DL_DELETE(rescale_tmp_objs_g, obj);
        if (rescale_insert_metadata(obj) != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to insert local object during non-MPI elastic migrate");
        n_inserted++;
    }
    rescale_tmp_n_obj_g    = 0;
    rescale_tmp_n_region_g = 0;
#else
    if (n_new <= 0)
        PGOTO_ERROR(FAIL, "Invalid n_new=%d for object migrate", n_new);

    sendcounts   = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    recvcounts   = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    sdispls      = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    rdispls      = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    dest_nobj    = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    dest_bufs    = (void **)PDC_calloc((size_t)n_new, sizeof(void *));
    dest_bundles = (BULKI **)PDC_calloc((size_t)n_new, sizeof(BULKI *));
    dest_arrays  = (BULKI_Entity **)PDC_calloc((size_t)n_new, sizeof(BULKI_Entity *));
    if (sendcounts == NULL || recvcounts == NULL || sdispls == NULL || rdispls == NULL || dest_nobj == NULL ||
        dest_bufs == NULL || dest_bundles == NULL || dest_arrays == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate MPI exchange state for object migrate");

    /* First pass: count per-dest objects (excluding local inserts). */
    DL_FOREACH(rescale_tmp_objs_g, obj)
    {
        int dest = (int)PDC_metadata_home_rank(obj->obj_name, obj->time_step, n_new);
        if (dest < 0 || dest >= n_new)
            PGOTO_ERROR(FAIL, "Invalid home rank %d for object %s", dest, obj->obj_name);
        if (dest != pdc_server_rank_g)
            dest_nobj[dest]++;
    }

    for (r = 0; r < n_new; r++) {
        if (dest_nobj[r] <= 0)
            continue;
        dest_bundles[r] = BULKI_init(1);
        dest_arrays[r]  = empty_BULKI_Array_Entity_with_capacity(dest_nobj[r]);
    }

    /* Second pass: local insert or pack into per-dest bundles. */
    DL_FOREACH_SAFE(rescale_tmp_objs_g, obj, obj_tmp)
    {
        int dest = (int)PDC_metadata_home_rank(obj->obj_name, obj->time_step, n_new);

        DL_DELETE(rescale_tmp_objs_g, obj);

        if (dest == pdc_server_rank_g) {
            if (rescale_insert_metadata(obj) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to insert local-home object during elastic migrate");
            n_inserted++;
            continue;
        }

        {
            BULKI *packed = rescale_pack_metadata_obj(obj);
            if (packed == NULL)
                PGOTO_ERROR(FAIL, "Failed to pack metadata object for migrate");
            BULKI_ENTITY_append_BULKI_incremental(dest_arrays[dest], packed);
            /* Shell only: header/data ownership transferred into dest_arrays via memcpy. */
            packed = (BULKI *)PDC_free(packed);
            /* Object bytes are in the bundle; free local copy. */
            rescale_free_one_metadata(obj);
        }
    }
    rescale_tmp_n_obj_g    = 0;
    rescale_tmp_n_region_g = 0;

    for (r = 0; r < n_new; r++) {
        size_t ser_size = 0;

        if (dest_bundles[r] == NULL)
            continue;

        BULKI_put_incremental(dest_bundles[r], BULKI_singleton_ENTITY("metadata_objects", PDC_STRING),
                              dest_arrays[r]);
        dest_arrays[r] = NULL;

        dest_bufs[r] = BULKI_serialize(dest_bundles[r], &ser_size);
        BULKI_free(dest_bundles[r], 1);
        dest_bundles[r] = NULL;

        if (dest_bufs[r] == NULL)
            PGOTO_ERROR(FAIL, "Failed to serialize object migrate bundle for dest %d", r);
        if (ser_size > (size_t)INT_MAX)
            PGOTO_ERROR(FAIL, "Object migrate bundle too large for MPI counts");
        sendcounts[r] = (int)ser_size;
    }

    MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    total_send = 0;
    total_recv = 0;
    for (r = 0; r < n_new; r++) {
        sdispls[r] = total_send;
        rdispls[r] = total_recv;
        total_send += sendcounts[r];
        total_recv += recvcounts[r];
    }

    if (total_send > 0) {
        sendbuf = (char *)PDC_malloc((size_t)total_send);
        if (sendbuf == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate Alltoallv send buffer");
        for (r = 0; r < n_new; r++) {
            if (sendcounts[r] > 0 && dest_bufs[r] != NULL) {
                memcpy(sendbuf + sdispls[r], dest_bufs[r], (size_t)sendcounts[r]);
                dest_bufs[r] = PDC_free(dest_bufs[r]);
            }
        }
    }
    for (r = 0; r < n_new; r++) {
        if (dest_bufs[r] != NULL)
            dest_bufs[r] = PDC_free(dest_bufs[r]);
    }

    if (total_recv > 0) {
        recvbuf = (char *)PDC_malloc((size_t)total_recv);
        if (recvbuf == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate Alltoallv recv buffer");
    }

    {
        char dummy = 0;
        MPI_Alltoallv(total_send > 0 ? sendbuf : &dummy, sendcounts, sdispls, MPI_BYTE,
                      total_recv > 0 ? recvbuf : &dummy, recvcounts, rdispls, MPI_BYTE, MPI_COMM_WORLD);
    }

    if (sendbuf != NULL)
        sendbuf = (char *)PDC_free(sendbuf);

    for (r = 0; r < n_new; r++) {
        if (recvcounts[r] <= 0)
            continue;
        if (rescale_ingest_obj_bundle(recvbuf + rdispls[r], recvcounts[r], &n_inserted) != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to ingest migrated objects from rank %d", r);
    }

    if (recvbuf != NULL)
        recvbuf = (char *)PDC_free(recvbuf);
#endif /* ENABLE_MPI */

    after_local = n_inserted;
#ifdef ENABLE_MPI
    MPI_Allreduce(&after_local, &after_global, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#else
    after_global = after_local;
#endif

    LOG_INFO("Rank %d inserted %d objects after elastic migrate\n", pdc_server_rank_g, after_local);

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic object migrate complete: %d objects before, %d after\n", before_global,
                 after_global);

    if (before_global != after_global)
        PGOTO_ERROR(FAIL, "Object count mismatch after elastic migrate (%d before, %d after)", before_global,
                    after_global);

done:
#ifdef ENABLE_MPI
    if (ret_value != SUCCEED) {
        for (r = 0; r < n_new; r++) {
            if (dest_bufs != NULL && dest_bufs[r] != NULL)
                dest_bufs[r] = PDC_free(dest_bufs[r]);
            if (dest_bundles != NULL && dest_bundles[r] != NULL) {
                BULKI_free(dest_bundles[r], 1);
                dest_bundles[r] = NULL;
            }
            if (dest_arrays != NULL && dest_arrays[r] != NULL) {
                BULKI_Entity_free(dest_arrays[r], 1);
                dest_arrays[r] = NULL;
            }
        }
        if (sendbuf != NULL)
            sendbuf = (char *)PDC_free(sendbuf);
        if (recvbuf != NULL)
            recvbuf = (char *)PDC_free(recvbuf);
        /* Leftover temp objects not yet migrated */
        PDC_Server_rescale_free_tmp_objs();
    }
    if (sendcounts != NULL)
        sendcounts = (int *)PDC_free(sendcounts);
    if (recvcounts != NULL)
        recvcounts = (int *)PDC_free(recvcounts);
    if (sdispls != NULL)
        sdispls = (int *)PDC_free(sdispls);
    if (rdispls != NULL)
        rdispls = (int *)PDC_free(rdispls);
    if (dest_nobj != NULL)
        dest_nobj = (int *)PDC_free(dest_nobj);
    if (dest_bufs != NULL)
        dest_bufs = (void **)PDC_free(dest_bufs);
    if (dest_bundles != NULL)
        dest_bundles = (BULKI **)PDC_free(dest_bundles);
    if (dest_arrays != NULL)
        dest_arrays = (BULKI_Entity **)PDC_free(dest_arrays);
#else
    if (ret_value != SUCCEED)
        PDC_Server_rescale_free_tmp_objs();
#endif
    FUNC_LEAVE(ret_value);
}

static perr_t
rescale_load_one_shard(int shard_rank, int n_old)
{
    FUNC_ENTER(NULL);

    perr_t ret_value        = SUCCEED;
    FILE * file             = NULL;
    BULKI *checkpoint_bulki = NULL;
    char   path[ADDR_MAX];
    int    n_region_shard = 0;

    if (n_old <= 0)
        PGOTO_ERROR(FAIL, "Invalid n_old=%d when loading shard %d", n_old, shard_rank);

    checkpoint_shard_path(path, sizeof(path), shard_rank);
    file = fopen(path, "rb");
    if (file == NULL)
        PGOTO_ERROR(FAIL, "Error with fopen, filename: [%s]", path);

    checkpoint_bulki = BULKI_deserialize_from_file(file);
    file             = NULL;
    if (checkpoint_bulki == NULL)
        PGOTO_ERROR(FAIL, "Failed to deserialize checkpoint [%s]", path);

    BULKI_Entity *version_entity =
        BULKI_get(checkpoint_bulki, BULKI_singleton_ENTITY("version_number", PDC_STRING));
    if (version_entity == NULL)
        PGOTO_ERROR(FAIL, "Missing version_number in [%s]", path);
    {
        BULKI_Entity *expected = BULKI_ENTITY(PDC_CHECKPOINT_MAGIC_CURRENT, 1, PDC_STRING, PDC_CLS_ITEM);
        int           equal    = BULKI_Entity_equal(version_entity, expected);
        BULKI_Entity_free(expected, 1);
        if (!equal)
            PGOTO_ERROR(FAIL, "Checkpoint version mismatch in [%s]", path);
    }

    BULKI_Entity *containers_array =
        BULKI_get(checkpoint_bulki, BULKI_singleton_ENTITY("containers", PDC_STRING));
    if (containers_array != NULL && containers_array->pdc_type == PDC_BULKI) {
        BULKI_Entity_Iterator *cont_iter = Bent_iterator_init(containers_array, NULL, PDC_BULKI);
        while (Bent_iterator_has_next_BULKI(cont_iter)) {
            BULKI *                      container_entry = Bent_iterator_next_BULKI(cont_iter);
            pdc_rescale_tmp_cont_t *     tmp_cont        = NULL;
            pdc_cont_hash_table_entry_t *cont_entry      = NULL;

            BULKI_Entity *hash_key_ent =
                BULKI_get(container_entry, BULKI_singleton_ENTITY("hash_key", PDC_STRING));
            if (hash_key_ent == NULL || hash_key_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing container hash_key in [%s]", path);

            BULKI_Entity *cont_data_ent =
                BULKI_get(container_entry, BULKI_singleton_ENTITY("cont_data", PDC_STRING));
            if (cont_data_ent == NULL || cont_data_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing container cont_data in [%s]", path);

            cont_entry = (pdc_cont_hash_table_entry_t *)PDC_malloc(sizeof(pdc_cont_hash_table_entry_t));
            memcpy(cont_entry, cont_data_ent->data, sizeof(pdc_cont_hash_table_entry_t));
            /* Pointers inside the blob are not valid across restart */
            cont_entry->obj_ids         = NULL;
            cont_entry->n_obj           = 0;
            cont_entry->n_allocated     = 0;
            cont_entry->kvtag_list_head = NULL;

            tmp_cont = (pdc_rescale_tmp_cont_t *)PDC_calloc(1, sizeof(pdc_rescale_tmp_cont_t));
            memcpy(&tmp_cont->hash_key, hash_key_ent->data, sizeof(uint32_t));
            tmp_cont->source_shard = shard_rank;
            tmp_cont->entry        = cont_entry;
            tmp_cont->next         = rescale_tmp_conts_g;
            rescale_tmp_conts_g    = tmp_cont;
            rescale_tmp_n_cont_g++;
        }
    }

    BULKI_Entity *metadata_entries_array =
        BULKI_get(checkpoint_bulki, BULKI_singleton_ENTITY("metadata_entries", PDC_STRING));
    if (metadata_entries_array != NULL && metadata_entries_array->pdc_type == PDC_BULKI) {
        BULKI_Entity_Iterator *entry_iter = Bent_iterator_init(metadata_entries_array, NULL, PDC_BULKI);
        while (Bent_iterator_has_next_BULKI(entry_iter)) {
            BULKI *hash_entry = Bent_iterator_next_BULKI(entry_iter);
            int    count;

            BULKI_Entity *n_obj_ent = BULKI_get(hash_entry, BULKI_singleton_ENTITY("n_obj", PDC_STRING));
            if (n_obj_ent == NULL)
                PGOTO_ERROR(FAIL, "Missing n_obj in [%s]", path);
            memcpy(&count, n_obj_ent->data, sizeof(int));
            if (count <= 0 || count > PDC_CHECKPOINT_MAX_METADATA_COUNT)
                PGOTO_ERROR(FAIL, "Suspicious count value %d in [%s]", count, path);

            BULKI_Entity *metadata_objs_array =
                BULKI_get(hash_entry, BULKI_singleton_ENTITY("metadata_objects", PDC_STRING));
            if (metadata_objs_array == NULL || metadata_objs_array->pdc_type != PDC_BULKI)
                PGOTO_ERROR(FAIL, "Missing metadata_objects in [%s]", path);

            BULKI_Entity_Iterator *obj_iter = Bent_iterator_init(metadata_objs_array, NULL, PDC_BULKI);
            int                    i        = 0;
            while (Bent_iterator_has_next_BULKI(obj_iter) && i < count) {
                BULKI *metadata_obj = Bent_iterator_next_BULKI(obj_iter);
                if (rescale_append_obj_from_bulki(metadata_obj, shard_rank, &n_region_shard) != SUCCEED)
                    PGOTO_ERROR(FAIL, "Failed to load object from shard %d", shard_rank);
                i++;
            }
            if (i != count)
                PGOTO_ERROR(FAIL, "metadata object count mismatch in [%s] (got %d expect %d)", path, i,
                            count);
        }
    }

    /* Transfer-query planner state (migrated later to metadata homes). */
    {
        BULKI_Entity *tq_ent =
            BULKI_get(checkpoint_bulki, BULKI_singleton_ENTITY("transfer_query", PDC_STRING));
        if (tq_ent != NULL && tq_ent->pdc_type == PDC_BULKI && tq_ent->data != NULL) {
            BULKI *       tq_bulki      = (BULKI *)tq_ent->data;
            BULKI_Entity *objects_array = BULKI_get(tq_bulki, BULKI_singleton_ENTITY("objects", PDC_STRING));

            if (objects_array != NULL && objects_array->pdc_type == PDC_BULKI) {
                BULKI_Entity_Iterator *obj_iter = Bent_iterator_init(objects_array, NULL, PDC_BULKI);
                while (Bent_iterator_has_next_BULKI(obj_iter)) {
                    BULKI *                   obj_bulki = Bent_iterator_next_BULKI(obj_iter);
                    pdc_rescale_tmp_tq_obj_t *tmp_obj   = NULL;
                    int                       ndim      = 0;
                    uint64_t                  obj_id    = 0;

                    BULKI_Entity *obj_id_ent =
                        BULKI_get(obj_bulki, BULKI_singleton_ENTITY("obj_id", PDC_STRING));
                    if (obj_id_ent == NULL || obj_id_ent->data == NULL) {
                        LOG_ERROR("Missing obj_id in transfer_query of [%s]; skipping entry\n", path);
                        continue;
                    }
                    memcpy(&obj_id, obj_id_ent->data, sizeof(uint64_t));

                    BULKI_Entity *ndim_ent = BULKI_get(obj_bulki, BULKI_singleton_ENTITY("ndim", PDC_STRING));
                    if (ndim_ent == NULL || ndim_ent->data == NULL)
                        PGOTO_ERROR(FAIL, "Missing ndim in transfer_query of [%s]", path);
                    memcpy(&ndim, ndim_ent->data, sizeof(int));
                    if (ndim <= 0 || ndim > DIM_MAX)
                        PGOTO_ERROR(FAIL, "Invalid ndim %d in transfer_query of [%s]", ndim, path);

                    tmp_obj = (pdc_rescale_tmp_tq_obj_t *)PDC_calloc(1, sizeof(pdc_rescale_tmp_tq_obj_t));
                    if (tmp_obj == NULL)
                        PGOTO_ERROR(FAIL, "Cannot allocate transfer_query temp object");
                    tmp_obj->obj_id       = obj_id;
                    tmp_obj->ndim         = ndim;
                    tmp_obj->source_shard = shard_rank;

                    BULKI_Entity *regions_array =
                        BULKI_get(obj_bulki, BULKI_singleton_ENTITY("regions", PDC_STRING));
                    if (regions_array != NULL && regions_array->pdc_type == PDC_BULKI) {
                        BULKI_Entity_Iterator *reg_iter = Bent_iterator_init(regions_array, NULL, PDC_BULKI);
                        while (Bent_iterator_has_next_BULKI(reg_iter)) {
                            BULKI *                      region_bulki = Bent_iterator_next_BULKI(reg_iter);
                            pdc_rescale_tmp_tq_region_t *reg_pkg      = NULL;
                            uint32_t                     ds_id        = 0;

                            BULKI_Entity *server_id_ent =
                                BULKI_get(region_bulki, BULKI_singleton_ENTITY("data_server_id", PDC_STRING));
                            if (server_id_ent == NULL || server_id_ent->data == NULL)
                                PGOTO_ERROR(FAIL, "Missing data_server_id in transfer_query of [%s]", path);
                            memcpy(&ds_id, server_id_ent->data, sizeof(uint32_t));
                            if (ds_id >= (uint32_t)n_old)
                                PGOTO_ERROR(FAIL,
                                            "transfer_query data_server_id %u out of range for N_old=%d "
                                            "in [%s]",
                                            ds_id, n_old, path);

                            BULKI_Entity *offset_size_ent = BULKI_get(
                                region_bulki, BULKI_singleton_ENTITY("reg_offset_size", PDC_STRING));
                            if (offset_size_ent == NULL || offset_size_ent->data == NULL)
                                PGOTO_ERROR(FAIL, "Missing reg_offset_size in transfer_query of [%s]", path);
                            if ((size_t)offset_size_ent->count != (size_t)ndim * 2)
                                PGOTO_ERROR(FAIL, "Invalid reg_offset_size count in transfer_query of [%s]",
                                            path);

                            reg_pkg = (pdc_rescale_tmp_tq_region_t *)PDC_calloc(
                                1, sizeof(pdc_rescale_tmp_tq_region_t));
                            if (reg_pkg == NULL)
                                PGOTO_ERROR(FAIL, "Cannot allocate transfer_query temp region");
                            reg_pkg->data_server_id = ds_id;
                            reg_pkg->reg_offset_size =
                                (uint64_t *)PDC_malloc(sizeof(uint64_t) * (size_t)ndim * 2);
                            if (reg_pkg->reg_offset_size == NULL)
                                PGOTO_ERROR(FAIL, "Cannot allocate transfer_query region extents");
                            memcpy(reg_pkg->reg_offset_size, offset_size_ent->data,
                                   sizeof(uint64_t) * (size_t)ndim * 2);

                            if (tmp_obj->regions == NULL) {
                                tmp_obj->regions     = reg_pkg;
                                tmp_obj->regions_end = reg_pkg;
                            }
                            else {
                                tmp_obj->regions_end->next = reg_pkg;
                                tmp_obj->regions_end       = reg_pkg;
                            }
                        }
                    }

                    tmp_obj->next    = rescale_tmp_tq_g;
                    rescale_tmp_tq_g = tmp_obj;
                    rescale_tmp_n_tq_g++;
                }
            }
        }
    }

    rescale_tmp_n_region_g += n_region_shard;
    LOG_INFO("Rank %d loaded shard %d into temp holders (%d objs, %d regions, %d conts, %d tq objs)\n",
             pdc_server_rank_g, shard_rank, rescale_tmp_n_obj_g, rescale_tmp_n_region_g, rescale_tmp_n_cont_g,
             rescale_tmp_n_tq_g);

done:
    if (checkpoint_bulki != NULL)
        BULKI_free(checkpoint_bulki, 1);
    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Server_rescale_validate_shards(int n_old, int n_new)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    local_ok  = 1;
    int    global_ok = 1;
    int    s;
    char   path[ADDR_MAX];
    int    n_load = 0;

    if (strpbrk(pdc_server_tmp_dir_g, ";&|`$<>") != NULL)
        PGOTO_ERROR(FAIL, "Invalid characters in server tmp dir path");

    for (s = 0; s < n_old; s++) {
        if (!checkpoint_shard_exists(s)) {
            local_ok = 0;
            break;
        }
    }

#ifdef ENABLE_MPI
    MPI_Allreduce(&local_ok, &global_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
#else
    global_ok     = local_ok;
#endif

    if (!global_ok) {
        if (pdc_server_rank_g == 0) {
            for (s = 0; s < n_old; s++) {
                if (!checkpoint_shard_exists(s)) {
                    checkpoint_shard_path(path, sizeof(path), s);
                    LOG_ERROR("Missing checkpoint shard for elastic restart: [%s] "
                              "(need all shards 0..%d)\n",
                              path, n_old - 1);
                    break;
                }
            }
        }
        PGOTO_ERROR(FAIL, "Elastic restart aborted: incomplete checkpoint shards for N_old=%d", n_old);
    }

    if (pdc_server_rank_g == 0) {
        LOG_INFO("Validated %d checkpoint shard(s) for elastic restart (%d -> %d)\n", n_old, n_old, n_new);
        if (n_old > n_new) {
            LOG_INFO("Scale-down orphan shard loaders (shard -> rank):\n");
            for (s = n_new; s < n_old; s++)
                LOG_INFO("  metadata_checkpoint.%d -> rank %d\n", s,
                         rescale_loader_rank_for_shard(s, n_old, n_new));
        }
        else if (n_old < n_new) {
            LOG_INFO("Scale-up: ranks %d..%d start with empty metadata holders\n", n_old, n_new - 1);
        }
    }

    for (s = 0; s < n_old; s++) {
        if (rescale_rank_should_load_shard(s, n_old, n_new, pdc_server_rank_g))
            n_load++;
    }
    LOG_INFO("Rank %d will load %d checkpoint shard(s) during elastic restart\n", pdc_server_rank_g, n_load);

    rescale_validated_n_old_g = n_old;
    rescale_validated_n_new_g = n_new;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Deserialize assigned N_old shards into temporary holders (not final hash tables).
 */
static perr_t
PDC_Server_rescale_load_shards(int n_old, int n_new)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    s;
    int    local_nobj  = 0;
    int    all_nobj    = 0;
    int    local_ncont = 0;
    int    all_ncont   = 0;

    PDC_Server_rescale_free_tmp_holders();

    for (s = 0; s < n_old; s++) {
        if (!rescale_rank_should_load_shard(s, n_old, n_new, pdc_server_rank_g))
            continue;
        if (rescale_load_one_shard(s, n_old) != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed loading checkpoint shard %d on rank %d", s, pdc_server_rank_g);
    }

    local_nobj  = rescale_tmp_n_obj_g;
    local_ncont = rescale_tmp_n_cont_g;

#ifdef ENABLE_MPI
    MPI_Allreduce(&local_nobj, &all_nobj, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&local_ncont, &all_ncont, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#else
    all_nobj      = local_nobj;
    all_ncont     = local_ncont;
#endif

    LOG_INFO("Rank %d temp holders: %d objects, %d regions, %d containers, %d transfer-query objs\n",
             pdc_server_rank_g, local_nobj, rescale_tmp_n_region_g, local_ncont, rescale_tmp_n_tq_g);

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic load complete: %d objects and %d containers across all ranks "
                 "(N_old=%d -> N_new=%d); migration not yet applied\n",
                 all_nobj, all_ncont, n_old, n_new);

    (void)rescale_validated_n_old_g;
    (void)rescale_validated_n_new_g;

done:
    if (ret_value != SUCCEED)
        PDC_Server_rescale_free_tmp_holders();
    FUNC_LEAVE(ret_value);
}

static void
rescale_free_one_tmp_cont(pdc_rescale_tmp_cont_t *cont)
{
    FUNC_ENTER(NULL);

    if (cont == NULL)
        FUNC_LEAVE_VOID();

    if (cont->entry != NULL) {
        cont->entry->obj_ids = (uint64_t *)PDC_free(cont->entry->obj_ids);
        cont->entry          = (pdc_cont_hash_table_entry_t *)PDC_free(cont->entry);
    }
    cont = (pdc_rescale_tmp_cont_t *)PDC_free(cont);

    FUNC_LEAVE_VOID();
}

/* Pack one container into checkpoint container_entry shape (hash_key + cont_data). */
static BULKI *
rescale_pack_container(pdc_rescale_tmp_cont_t *cont)
{
    FUNC_ENTER(NULL);

    BULKI *entry_bulki = NULL;

    if (cont == NULL || cont->entry == NULL)
        FUNC_LEAVE(NULL);

    entry_bulki = BULKI_init(2);
    BULKI_put_incremental(entry_bulki, BULKI_singleton_ENTITY("hash_key", PDC_STRING),
                          BULKI_ENTITY(&cont->hash_key, 1, PDC_UINT32, PDC_CLS_ITEM));
    BULKI_put_incremental(
        entry_bulki, BULKI_singleton_ENTITY("cont_data", PDC_STRING),
        BULKI_ENTITY(cont->entry, sizeof(pdc_cont_hash_table_entry_t), PDC_UINT8, PDC_CLS_ARRAY));

    FUNC_LEAVE(entry_bulki);
}

/*
 * Insert container into container_hash_table_g. Takes ownership of entry on success.
 * Uses hash of cont_name (must match client create: home_rank(name, 0, N)).
 */
static perr_t
rescale_insert_container(pdc_cont_hash_table_entry_t *entry, uint32_t hash_key_val)
{
    FUNC_ENTER(NULL);

    perr_t                       ret_value    = SUCCEED;
    uint32_t *                   hash_key     = NULL;
    pdc_cont_hash_table_entry_t *lookup_value = NULL;

    if (entry == NULL)
        PGOTO_ERROR(FAIL, "container entry is NULL");
    if (container_hash_table_g == NULL)
        PGOTO_ERROR(FAIL, "container_hash_table_g not initialized");

    /* Match checkpoint write / client create: hash key is PDC_get_hash_by_name(cont_name). */
    {
        uint32_t name_hash = PDC_get_hash_by_name(entry->cont_name);
        if (name_hash != hash_key_val && pdc_server_rank_g == 0)
            LOG_INFO("Container [%s] stored hash_key %u differs from name hash %u; using name hash\n",
                     entry->cont_name, hash_key_val, name_hash);
        hash_key_val = name_hash;
    }

    lookup_value = hash_table_lookup(container_hash_table_g, &hash_key_val);
    if (lookup_value != NULL) {
        if (lookup_value->cont_id != entry->cont_id)
            PGOTO_ERROR(FAIL, "Duplicate container name [%s] with different cont_id during elastic migrate",
                        entry->cont_name);
        /* Identical container already present — drop duplicate payload. */
        entry->obj_ids = (uint64_t *)PDC_free(entry->obj_ids);
        entry          = (pdc_cont_hash_table_entry_t *)PDC_free(entry);
        PGOTO_DONE(SUCCEED);
    }

    hash_key = (uint32_t *)PDC_malloc(sizeof(uint32_t));
    if (hash_key == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate container hash_key");
    *hash_key = hash_key_val;

    entry->obj_ids         = NULL;
    entry->n_obj           = 0;
    entry->n_allocated     = 0;
    entry->kvtag_list_head = NULL;

    if (hash_table_insert(container_hash_table_g, hash_key, entry) != 1)
        PGOTO_ERROR(FAIL, "Hash table insert failed for container [%s]", entry->cont_name);

    hash_key = NULL;
    entry    = NULL;

done:
    if (ret_value != SUCCEED) {
        if (hash_key != NULL)
            hash_key = (uint32_t *)PDC_free(hash_key);
        if (entry != NULL) {
            entry->obj_ids = (uint64_t *)PDC_free(entry->obj_ids);
            entry          = (pdc_cont_hash_table_entry_t *)PDC_free(entry);
        }
    }
    FUNC_LEAVE(ret_value);
}

static perr_t
rescale_ingest_cont_bundle(void *buf, int buf_size, int *n_inserted)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    BULKI *bundle    = NULL;

    if (buf_size == 0)
        PGOTO_DONE(SUCCEED);
    if (buf == NULL || buf_size < 0)
        PGOTO_ERROR(FAIL, "Invalid container migration bundle");

    bundle = BULKI_deserialize(buf);
    if (bundle == NULL)
        PGOTO_ERROR(FAIL, "Failed to deserialize container migration bundle");

    BULKI_Entity *conts_array = BULKI_get(bundle, BULKI_singleton_ENTITY("containers", PDC_STRING));
    if (conts_array == NULL || conts_array->pdc_type != PDC_BULKI)
        PGOTO_ERROR(FAIL, "Missing containers array in migration bundle");

    {
        BULKI_Entity_Iterator *cont_iter = Bent_iterator_init(conts_array, NULL, PDC_BULKI);
        while (Bent_iterator_has_next_BULKI(cont_iter)) {
            BULKI *                      container_entry = Bent_iterator_next_BULKI(cont_iter);
            uint32_t                     hash_key_val    = 0;
            pdc_cont_hash_table_entry_t *cont_entry      = NULL;

            BULKI_Entity *hash_key_ent =
                BULKI_get(container_entry, BULKI_singleton_ENTITY("hash_key", PDC_STRING));
            if (hash_key_ent == NULL || hash_key_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing container hash_key in migration bundle");
            memcpy(&hash_key_val, hash_key_ent->data, sizeof(uint32_t));

            BULKI_Entity *cont_data_ent =
                BULKI_get(container_entry, BULKI_singleton_ENTITY("cont_data", PDC_STRING));
            if (cont_data_ent == NULL || cont_data_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing container cont_data in migration bundle");

            cont_entry = (pdc_cont_hash_table_entry_t *)PDC_malloc(sizeof(pdc_cont_hash_table_entry_t));
            if (cont_entry == NULL)
                PGOTO_ERROR(FAIL, "Cannot allocate container entry");
            memcpy(cont_entry, cont_data_ent->data, sizeof(pdc_cont_hash_table_entry_t));
            cont_entry->obj_ids         = NULL;
            cont_entry->n_obj           = 0;
            cont_entry->n_allocated     = 0;
            cont_entry->kvtag_list_head = NULL;

            if (rescale_insert_container(cont_entry, hash_key_val) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to insert migrated container");
            if (n_inserted)
                (*n_inserted)++;
        }
    }

done:
    if (bundle != NULL)
        BULKI_free(bundle, 1);
    FUNC_LEAVE(ret_value);
}

/*
 * Migrate temp containers to N_new home ranks using
 * PDC_metadata_home_rank(cont_name, 0, N_new) — same as client create/query.
 * Keeps stable cont_id values.
 */
static perr_t
PDC_Server_rescale_migrate_containers(int n_new)
{
    FUNC_ENTER(NULL);

    perr_t                  ret_value     = SUCCEED;
    pdc_rescale_tmp_cont_t *cont          = NULL;
    pdc_rescale_tmp_cont_t *cont_tmp      = NULL;
    int                     before_local  = 0;
    int                     before_global = 0;
    int                     after_local   = 0;
    int                     after_global  = 0;
    int                     n_inserted    = 0;
#ifdef ENABLE_MPI
    int *          sendcounts   = NULL;
    int *          recvcounts   = NULL;
    int *          sdispls      = NULL;
    int *          rdispls      = NULL;
    int *          dest_ncont   = NULL;
    void **        dest_bufs    = NULL;
    BULKI **       dest_bundles = NULL;
    BULKI_Entity **dest_arrays  = NULL;
    char *         sendbuf      = NULL;
    char *         recvbuf      = NULL;
    int            total_send   = 0;
    int            total_recv   = 0;
    int            r;
#endif

    before_local = rescale_tmp_n_cont_g;
#ifdef ENABLE_MPI
    MPI_Allreduce(&before_local, &before_global, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#else
    before_global = before_local;
#endif

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic container migrate: %d containers before exchange (N_new=%d)\n", before_global,
                 n_new);

    if (container_hash_table_g == NULL) {
        if (PDC_Server_init_hash_table() != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Server_init_hash_table during container migrate");
    }

#ifndef ENABLE_MPI
    (void)n_new;
    cont = rescale_tmp_conts_g;
    while (cont) {
        cont_tmp                           = cont->next;
        pdc_cont_hash_table_entry_t *entry = cont->entry;
        uint32_t                     hk    = cont->hash_key;

        cont->entry = NULL;
        cont        = (pdc_rescale_tmp_cont_t *)PDC_free(cont);
        cont        = cont_tmp;

        if (rescale_insert_container(entry, hk) != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to insert local container during non-MPI elastic migrate");
        n_inserted++;
    }
    rescale_tmp_conts_g  = NULL;
    rescale_tmp_n_cont_g = 0;
#else
    if (n_new <= 0)
        PGOTO_ERROR(FAIL, "Invalid n_new=%d for container migrate", n_new);

    sendcounts   = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    recvcounts   = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    sdispls      = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    rdispls      = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    dest_ncont   = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    dest_bufs    = (void **)PDC_calloc((size_t)n_new, sizeof(void *));
    dest_bundles = (BULKI **)PDC_calloc((size_t)n_new, sizeof(BULKI *));
    dest_arrays  = (BULKI_Entity **)PDC_calloc((size_t)n_new, sizeof(BULKI_Entity *));
    if (sendcounts == NULL || recvcounts == NULL || sdispls == NULL || rdispls == NULL ||
        dest_ncont == NULL || dest_bufs == NULL || dest_bundles == NULL || dest_arrays == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate MPI exchange state for container migrate");

    /* Count per-dest containers (excluding local). */
    for (cont = rescale_tmp_conts_g; cont != NULL; cont = cont->next) {
        int dest = (int)PDC_metadata_home_rank(cont->entry->cont_name, 0, n_new);
        if (dest < 0 || dest >= n_new)
            PGOTO_ERROR(FAIL, "Invalid home rank %d for container %s", dest, cont->entry->cont_name);
        if (dest != pdc_server_rank_g)
            dest_ncont[dest]++;
    }

    for (r = 0; r < n_new; r++) {
        if (dest_ncont[r] <= 0)
            continue;
        dest_bundles[r] = BULKI_init(1);
        dest_arrays[r]  = empty_BULKI_Array_Entity_with_capacity(dest_ncont[r]);
    }

    /* Local insert or pack for remote. */
    cont = rescale_tmp_conts_g;
    while (cont) {
        int                          dest;
        pdc_cont_hash_table_entry_t *entry;

        cont_tmp = cont->next;
        dest     = (int)PDC_metadata_home_rank(cont->entry->cont_name, 0, n_new);
        entry    = cont->entry;

        if (dest == pdc_server_rank_g) {
            uint32_t hk = cont->hash_key;
            cont->entry = NULL;
            cont        = (pdc_rescale_tmp_cont_t *)PDC_free(cont);
            cont        = cont_tmp;
            if (rescale_insert_container(entry, hk) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to insert local-home container during elastic migrate");
            n_inserted++;
            continue;
        }

        {
            BULKI *packed = rescale_pack_container(cont);
            if (packed == NULL)
                PGOTO_ERROR(FAIL, "Failed to pack container for migrate");
            BULKI_ENTITY_append_BULKI_incremental(dest_arrays[dest], packed);
            packed = (BULKI *)PDC_free(packed);
            rescale_free_one_tmp_cont(cont);
            cont = cont_tmp;
        }
    }
    rescale_tmp_conts_g  = NULL;
    rescale_tmp_n_cont_g = 0;

    for (r = 0; r < n_new; r++) {
        size_t ser_size = 0;

        if (dest_bundles[r] == NULL)
            continue;

        BULKI_put_incremental(dest_bundles[r], BULKI_singleton_ENTITY("containers", PDC_STRING),
                              dest_arrays[r]);
        dest_arrays[r] = NULL;

        dest_bufs[r] = BULKI_serialize(dest_bundles[r], &ser_size);
        BULKI_free(dest_bundles[r], 1);
        dest_bundles[r] = NULL;

        if (dest_bufs[r] == NULL)
            PGOTO_ERROR(FAIL, "Failed to serialize container migrate bundle for dest %d", r);
        if (ser_size > (size_t)INT_MAX)
            PGOTO_ERROR(FAIL, "Container migrate bundle too large for MPI counts");
        sendcounts[r] = (int)ser_size;
    }

    MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    total_send = 0;
    total_recv = 0;
    for (r = 0; r < n_new; r++) {
        sdispls[r] = total_send;
        rdispls[r] = total_recv;
        total_send += sendcounts[r];
        total_recv += recvcounts[r];
    }

    if (total_send > 0) {
        sendbuf = (char *)PDC_malloc((size_t)total_send);
        if (sendbuf == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate container Alltoallv send buffer");
        for (r = 0; r < n_new; r++) {
            if (sendcounts[r] > 0 && dest_bufs[r] != NULL) {
                memcpy(sendbuf + sdispls[r], dest_bufs[r], (size_t)sendcounts[r]);
                dest_bufs[r] = PDC_free(dest_bufs[r]);
            }
        }
    }
    for (r = 0; r < n_new; r++) {
        if (dest_bufs[r] != NULL)
            dest_bufs[r] = PDC_free(dest_bufs[r]);
    }

    if (total_recv > 0) {
        recvbuf = (char *)PDC_malloc((size_t)total_recv);
        if (recvbuf == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate container Alltoallv recv buffer");
    }

    {
        char dummy = 0;
        MPI_Alltoallv(total_send > 0 ? sendbuf : &dummy, sendcounts, sdispls, MPI_BYTE,
                      total_recv > 0 ? recvbuf : &dummy, recvcounts, rdispls, MPI_BYTE, MPI_COMM_WORLD);
    }

    if (sendbuf != NULL)
        sendbuf = (char *)PDC_free(sendbuf);

    for (r = 0; r < n_new; r++) {
        if (recvcounts[r] <= 0)
            continue;
        if (rescale_ingest_cont_bundle(recvbuf + rdispls[r], recvcounts[r], &n_inserted) != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to ingest migrated containers from rank %d", r);
    }

    if (recvbuf != NULL)
        recvbuf = (char *)PDC_free(recvbuf);
#endif /* ENABLE_MPI */

    after_local = n_inserted;
#ifdef ENABLE_MPI
    MPI_Allreduce(&after_local, &after_global, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#else
    after_global = after_local;
#endif

    LOG_INFO("Rank %d inserted %d containers after elastic migrate\n", pdc_server_rank_g, after_local);

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic container migrate complete: %d containers before, %d after\n", before_global,
                 after_global);

    if (before_global != after_global)
        PGOTO_ERROR(FAIL, "Container count mismatch after elastic migrate (%d before, %d after)",
                    before_global, after_global);

done:
#ifdef ENABLE_MPI
    if (ret_value != SUCCEED) {
        for (r = 0; r < n_new; r++) {
            if (dest_bufs != NULL && dest_bufs[r] != NULL)
                dest_bufs[r] = PDC_free(dest_bufs[r]);
            if (dest_bundles != NULL && dest_bundles[r] != NULL) {
                BULKI_free(dest_bundles[r], 1);
                dest_bundles[r] = NULL;
            }
            if (dest_arrays != NULL && dest_arrays[r] != NULL) {
                BULKI_Entity_free(dest_arrays[r], 1);
                dest_arrays[r] = NULL;
            }
        }
        if (sendbuf != NULL)
            sendbuf = (char *)PDC_free(sendbuf);
        if (recvbuf != NULL)
            recvbuf = (char *)PDC_free(recvbuf);
        PDC_Server_rescale_free_tmp_conts();
    }
    if (sendcounts != NULL)
        sendcounts = (int *)PDC_free(sendcounts);
    if (recvcounts != NULL)
        recvcounts = (int *)PDC_free(recvcounts);
    if (sdispls != NULL)
        sdispls = (int *)PDC_free(sdispls);
    if (rdispls != NULL)
        rdispls = (int *)PDC_free(rdispls);
    if (dest_ncont != NULL)
        dest_ncont = (int *)PDC_free(dest_ncont);
    if (dest_bufs != NULL)
        dest_bufs = (void **)PDC_free(dest_bufs);
    if (dest_bundles != NULL)
        dest_bundles = (BULKI **)PDC_free(dest_bundles);
    if (dest_arrays != NULL)
        dest_arrays = (BULKI_Entity **)PDC_free(dest_arrays);
#else
    if (ret_value != SUCCEED)
        PDC_Server_rescale_free_tmp_conts();
#endif
    FUNC_LEAVE(ret_value);
}

static void
rescale_remap_tq_obj_servers(pdc_rescale_tmp_tq_obj_t *obj, int n_new)
{
    FUNC_ENTER(NULL);

    pdc_rescale_tmp_tq_region_t *reg;

    if (obj == NULL || n_new <= 0)
        FUNC_LEAVE_VOID();

    for (reg = obj->regions; reg != NULL; reg = reg->next)
        reg->data_server_id = reg->data_server_id % (uint32_t)n_new;

    FUNC_LEAVE_VOID();
}

static BULKI *
rescale_pack_tq_obj(pdc_rescale_tmp_tq_obj_t *obj)
{
    FUNC_ENTER(NULL);

    BULKI *                      obj_bulki = NULL;
    pdc_rescale_tmp_tq_region_t *reg;
    int                          reg_count = 0;

    if (obj == NULL)
        FUNC_LEAVE(NULL);

    for (reg = obj->regions; reg != NULL; reg = reg->next)
        reg_count++;

    obj_bulki = BULKI_init(3);
    BULKI_put_incremental(obj_bulki, BULKI_singleton_ENTITY("obj_id", PDC_STRING),
                          BULKI_ENTITY(&obj->obj_id, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put_incremental(obj_bulki, BULKI_singleton_ENTITY("ndim", PDC_STRING),
                          BULKI_ENTITY(&obj->ndim, 1, PDC_INT, PDC_CLS_ITEM));

    {
        BULKI_Entity *regions_array = empty_BULKI_Array_Entity_with_capacity(reg_count > 0 ? reg_count : 1);

        for (reg = obj->regions; reg != NULL; reg = reg->next) {
            BULKI *region_bulki = BULKI_init(2);

            BULKI_put_incremental(region_bulki, BULKI_singleton_ENTITY("data_server_id", PDC_STRING),
                                  BULKI_ENTITY(&reg->data_server_id, 1, PDC_UINT32, PDC_CLS_ITEM));
            BULKI_put_incremental(
                region_bulki, BULKI_singleton_ENTITY("reg_offset_size", PDC_STRING),
                BULKI_ENTITY(reg->reg_offset_size, obj->ndim * 2, PDC_UINT64, PDC_CLS_ARRAY));
            BULKI_ENTITY_append_BULKI_incremental(regions_array, region_bulki);
        }
        BULKI_put_incremental(obj_bulki, BULKI_singleton_ENTITY("regions", PDC_STRING), regions_array);
    }

    FUNC_LEAVE(obj_bulki);
}

static perr_t
rescale_tq_append_region_from_bulki(BULKI *region_bulki, pdc_rescale_tmp_tq_obj_t *obj)
{
    FUNC_ENTER(NULL);

    perr_t                       ret_value = SUCCEED;
    pdc_rescale_tmp_tq_region_t *reg_pkg   = NULL;
    uint32_t                     ds_id     = 0;

    BULKI_Entity *server_id_ent =
        BULKI_get(region_bulki, BULKI_singleton_ENTITY("data_server_id", PDC_STRING));
    if (server_id_ent == NULL || server_id_ent->data == NULL)
        PGOTO_ERROR(FAIL, "Missing data_server_id in migrated transfer_query region");
    memcpy(&ds_id, server_id_ent->data, sizeof(uint32_t));

    BULKI_Entity *offset_size_ent =
        BULKI_get(region_bulki, BULKI_singleton_ENTITY("reg_offset_size", PDC_STRING));
    if (offset_size_ent == NULL || offset_size_ent->data == NULL)
        PGOTO_ERROR(FAIL, "Missing reg_offset_size in migrated transfer_query region");
    if ((size_t)offset_size_ent->count != (size_t)obj->ndim * 2)
        PGOTO_ERROR(FAIL, "Invalid reg_offset_size count in migrated transfer_query region");

    reg_pkg = (pdc_rescale_tmp_tq_region_t *)PDC_calloc(1, sizeof(pdc_rescale_tmp_tq_region_t));
    if (reg_pkg == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate migrated transfer_query region");
    reg_pkg->data_server_id  = ds_id;
    reg_pkg->reg_offset_size = (uint64_t *)PDC_malloc(sizeof(uint64_t) * (size_t)obj->ndim * 2);
    if (reg_pkg->reg_offset_size == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate migrated transfer_query extents");
    memcpy(reg_pkg->reg_offset_size, offset_size_ent->data, sizeof(uint64_t) * (size_t)obj->ndim * 2);

    if (obj->regions == NULL) {
        obj->regions     = reg_pkg;
        obj->regions_end = reg_pkg;
    }
    else {
        obj->regions_end->next = reg_pkg;
        obj->regions_end       = reg_pkg;
    }
    reg_pkg = NULL;

done:
    if (ret_value != SUCCEED && reg_pkg != NULL) {
        reg_pkg->reg_offset_size = (uint64_t *)PDC_free(reg_pkg->reg_offset_size);
        reg_pkg                  = (pdc_rescale_tmp_tq_region_t *)PDC_free(reg_pkg);
    }
    FUNC_LEAVE(ret_value);
}

static perr_t
rescale_tq_merge_obj(pdc_rescale_tmp_tq_obj_t **head, pdc_rescale_tmp_tq_obj_t *incoming)
{
    FUNC_ENTER(NULL);

    perr_t                    ret_value = SUCCEED;
    pdc_rescale_tmp_tq_obj_t *cur;

    if (head == NULL || incoming == NULL)
        PGOTO_ERROR(FAIL, "Invalid args to rescale_tq_merge_obj");

    for (cur = *head; cur != NULL; cur = cur->next) {
        if (cur->obj_id == incoming->obj_id) {
            if (cur->ndim != incoming->ndim)
                PGOTO_ERROR(FAIL, "ndim mismatch merging transfer_query for obj_id %" PRIu64,
                            incoming->obj_id);
            if (incoming->regions != NULL) {
                if (cur->regions_end != NULL)
                    cur->regions_end->next = incoming->regions;
                else
                    cur->regions = incoming->regions;
                cur->regions_end      = incoming->regions_end;
                incoming->regions     = NULL;
                incoming->regions_end = NULL;
            }
            rescale_free_one_tmp_tq(incoming);
            PGOTO_DONE(SUCCEED);
        }
    }

    incoming->next = *head;
    *head          = incoming;

done:
    FUNC_LEAVE(ret_value);
}

static perr_t
rescale_ingest_tq_bundle(void *buf, int buf_size, pdc_rescale_tmp_tq_obj_t **assembled, int *n_added)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    BULKI *bundle    = NULL;

    if (buf_size == 0)
        PGOTO_DONE(SUCCEED);
    if (buf == NULL || buf_size < 0 || assembled == NULL)
        PGOTO_ERROR(FAIL, "Invalid transfer_query migration bundle");

    bundle = BULKI_deserialize(buf);
    if (bundle == NULL)
        PGOTO_ERROR(FAIL, "Failed to deserialize transfer_query migration bundle");

    BULKI_Entity *objs_array = BULKI_get(bundle, BULKI_singleton_ENTITY("objects", PDC_STRING));
    if (objs_array == NULL || objs_array->pdc_type != PDC_BULKI)
        PGOTO_ERROR(FAIL, "Missing objects array in transfer_query migration bundle");

    {
        BULKI_Entity_Iterator *obj_iter = Bent_iterator_init(objs_array, NULL, PDC_BULKI);
        while (Bent_iterator_has_next_BULKI(obj_iter)) {
            BULKI *                   obj_bulki = Bent_iterator_next_BULKI(obj_iter);
            pdc_rescale_tmp_tq_obj_t *tmp_obj   = NULL;
            uint64_t                  obj_id    = 0;
            int                       ndim      = 0;

            BULKI_Entity *obj_id_ent = BULKI_get(obj_bulki, BULKI_singleton_ENTITY("obj_id", PDC_STRING));
            if (obj_id_ent == NULL || obj_id_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing obj_id in transfer_query migration bundle");
            memcpy(&obj_id, obj_id_ent->data, sizeof(uint64_t));

            BULKI_Entity *ndim_ent = BULKI_get(obj_bulki, BULKI_singleton_ENTITY("ndim", PDC_STRING));
            if (ndim_ent == NULL || ndim_ent->data == NULL)
                PGOTO_ERROR(FAIL, "Missing ndim in transfer_query migration bundle");
            memcpy(&ndim, ndim_ent->data, sizeof(int));
            if (ndim <= 0 || ndim > DIM_MAX)
                PGOTO_ERROR(FAIL, "Invalid ndim %d in transfer_query migration bundle", ndim);

            tmp_obj = (pdc_rescale_tmp_tq_obj_t *)PDC_calloc(1, sizeof(pdc_rescale_tmp_tq_obj_t));
            if (tmp_obj == NULL)
                PGOTO_ERROR(FAIL, "Cannot allocate transfer_query object during ingest");
            tmp_obj->obj_id = obj_id;
            tmp_obj->ndim   = ndim;

            BULKI_Entity *regions_array = BULKI_get(obj_bulki, BULKI_singleton_ENTITY("regions", PDC_STRING));
            if (regions_array != NULL && regions_array->pdc_type == PDC_BULKI) {
                BULKI_Entity_Iterator *reg_iter = Bent_iterator_init(regions_array, NULL, PDC_BULKI);
                while (Bent_iterator_has_next_BULKI(reg_iter)) {
                    BULKI *region_bulki = Bent_iterator_next_BULKI(reg_iter);
                    if (rescale_tq_append_region_from_bulki(region_bulki, tmp_obj) != SUCCEED)
                        PGOTO_ERROR(FAIL, "Failed to ingest transfer_query region");
                }
            }

            if (rescale_tq_merge_obj(assembled, tmp_obj) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to merge migrated transfer_query object");
            tmp_obj = NULL;
            if (n_added)
                (*n_added)++;
        }
    }

done:
    if (bundle != NULL)
        BULKI_free(bundle, 1);
    FUNC_LEAVE(ret_value);
}

/*
 * Migrate transfer-query planner records to post-elastic metadata homes,
 * remap data_server_id with % N_new, and install via elastic init API.
 */
static perr_t
PDC_Server_rescale_migrate_transfer_query(int n_new)
{
    FUNC_ENTER(NULL);

    perr_t                    ret_value     = SUCCEED;
    pdc_rescale_tmp_tq_obj_t *assembled     = NULL;
    pdc_rescale_tmp_tq_obj_t *obj           = NULL;
    pdc_rescale_tmp_tq_obj_t *obj_tmp       = NULL;
    int                       before_local  = 0;
    int                       n_dropped     = 0;
    int                       n_assembled   = 0;
    BULKI *                   install_bulki = NULL;
#ifdef ENABLE_MPI
    int *          sendcounts   = NULL;
    int *          recvcounts   = NULL;
    int *          sdispls      = NULL;
    int *          rdispls      = NULL;
    int *          dest_nobj    = NULL;
    void **        dest_bufs    = NULL;
    BULKI **       dest_bundles = NULL;
    BULKI_Entity **dest_arrays  = NULL;
    char *         sendbuf      = NULL;
    char *         recvbuf      = NULL;
    int            total_send   = 0;
    int            total_recv   = 0;
    int            r;
#endif

    before_local = rescale_tmp_n_tq_g;

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic transfer-query migrate: starting with %d local temp objects (N_new=%d)\n",
                 before_local, n_new);

    /* Remap data_server_id early so wire and local paths share one rule. */
    for (obj = rescale_tmp_tq_g; obj != NULL; obj = obj->next)
        rescale_remap_tq_obj_servers(obj, n_new);

#ifndef ENABLE_MPI
    (void)n_new;
    while (rescale_tmp_tq_g != NULL) {
        obj              = rescale_tmp_tq_g;
        rescale_tmp_tq_g = obj->next;
        obj->next        = NULL;
        if (rescale_tq_merge_obj(&assembled, obj) != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to assemble local transfer_query object");
        n_assembled++;
    }
    rescale_tmp_n_tq_g = 0;
#else
    if (n_new <= 0)
        PGOTO_ERROR(FAIL, "Invalid n_new=%d for transfer-query migrate", n_new);

    sendcounts   = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    recvcounts   = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    sdispls      = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    rdispls      = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    dest_nobj    = (int *)PDC_calloc((size_t)n_new, sizeof(int));
    dest_bufs    = (void **)PDC_calloc((size_t)n_new, sizeof(void *));
    dest_bundles = (BULKI **)PDC_calloc((size_t)n_new, sizeof(BULKI *));
    dest_arrays  = (BULKI_Entity **)PDC_calloc((size_t)n_new, sizeof(BULKI_Entity *));
    if (sendcounts == NULL || recvcounts == NULL || sdispls == NULL || rdispls == NULL || dest_nobj == NULL ||
        dest_bufs == NULL || dest_bundles == NULL || dest_arrays == NULL)
        PGOTO_ERROR(FAIL, "Cannot allocate MPI state for transfer-query migrate");

    for (obj = rescale_tmp_tq_g; obj != NULL; obj = obj->next) {
        uint32_t home = 0;
        int      dest;

        if (!PDC_Server_lookup_obj_id_home(obj->obj_id, &home)) {
            n_dropped++;
            continue;
        }
        dest = (int)home;
        if (dest < 0 || dest >= n_new)
            PGOTO_ERROR(FAIL, "Invalid home %d for transfer_query obj_id %" PRIu64, dest, obj->obj_id);
        if (dest != pdc_server_rank_g)
            dest_nobj[dest]++;
    }

    for (r = 0; r < n_new; r++) {
        if (dest_nobj[r] <= 0)
            continue;
        dest_bundles[r] = BULKI_init(1);
        dest_arrays[r]  = empty_BULKI_Array_Entity_with_capacity(dest_nobj[r]);
    }

    obj = rescale_tmp_tq_g;
    while (obj) {
        uint32_t home = 0;
        int      dest;

        obj_tmp          = obj->next;
        obj->next        = NULL;
        rescale_tmp_tq_g = obj_tmp;

        if (!PDC_Server_lookup_obj_id_home(obj->obj_id, &home)) {
            LOG_ERROR("Dropping transfer_query for obj_id %" PRIu64 " (no home in elastic map)\n",
                      obj->obj_id);
            rescale_free_one_tmp_tq(obj);
            obj = obj_tmp;
            continue;
        }
        dest = (int)home;

        if (dest == pdc_server_rank_g) {
            if (rescale_tq_merge_obj(&assembled, obj) != SUCCEED)
                PGOTO_ERROR(FAIL, "Failed to keep local-home transfer_query object");
            n_assembled++;
            obj = obj_tmp;
            continue;
        }

        {
            BULKI *packed = rescale_pack_tq_obj(obj);
            if (packed == NULL)
                PGOTO_ERROR(FAIL, "Failed to pack transfer_query object for migrate");
            BULKI_ENTITY_append_BULKI_incremental(dest_arrays[dest], packed);
            packed = (BULKI *)PDC_free(packed);
            rescale_free_one_tmp_tq(obj);
            obj = obj_tmp;
        }
    }
    rescale_tmp_n_tq_g = 0;

    for (r = 0; r < n_new; r++) {
        size_t ser_size = 0;

        if (dest_bundles[r] == NULL)
            continue;

        BULKI_put_incremental(dest_bundles[r], BULKI_singleton_ENTITY("objects", PDC_STRING), dest_arrays[r]);
        dest_arrays[r] = NULL;

        dest_bufs[r] = BULKI_serialize(dest_bundles[r], &ser_size);
        BULKI_free(dest_bundles[r], 1);
        dest_bundles[r] = NULL;

        if (dest_bufs[r] == NULL)
            PGOTO_ERROR(FAIL, "Failed to serialize transfer_query migrate bundle for dest %d", r);
        if (ser_size > (size_t)INT_MAX)
            PGOTO_ERROR(FAIL, "Transfer_query migrate bundle too large for MPI counts");
        sendcounts[r] = (int)ser_size;
    }

    MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    total_send = 0;
    total_recv = 0;
    for (r = 0; r < n_new; r++) {
        sdispls[r] = total_send;
        rdispls[r] = total_recv;
        total_send += sendcounts[r];
        total_recv += recvcounts[r];
    }

    if (total_send > 0) {
        sendbuf = (char *)PDC_malloc((size_t)total_send);
        if (sendbuf == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate transfer_query Alltoallv send buffer");
        for (r = 0; r < n_new; r++) {
            if (sendcounts[r] > 0 && dest_bufs[r] != NULL) {
                memcpy(sendbuf + sdispls[r], dest_bufs[r], (size_t)sendcounts[r]);
                dest_bufs[r] = PDC_free(dest_bufs[r]);
            }
        }
    }
    for (r = 0; r < n_new; r++) {
        if (dest_bufs[r] != NULL)
            dest_bufs[r] = PDC_free(dest_bufs[r]);
    }

    if (total_recv > 0) {
        recvbuf = (char *)PDC_malloc((size_t)total_recv);
        if (recvbuf == NULL)
            PGOTO_ERROR(FAIL, "Cannot allocate transfer_query Alltoallv recv buffer");
    }

    {
        char dummy = 0;
        MPI_Alltoallv(total_send > 0 ? sendbuf : &dummy, sendcounts, sdispls, MPI_BYTE,
                      total_recv > 0 ? recvbuf : &dummy, recvcounts, rdispls, MPI_BYTE, MPI_COMM_WORLD);
    }

    if (sendbuf != NULL)
        sendbuf = (char *)PDC_free(sendbuf);

    for (r = 0; r < n_new; r++) {
        if (recvcounts[r] <= 0)
            continue;
        if (rescale_ingest_tq_bundle(recvbuf + rdispls[r], recvcounts[r], &assembled, &n_assembled) !=
            SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to ingest migrated transfer_query from rank %d", r);
    }

    if (recvbuf != NULL)
        recvbuf = (char *)PDC_free(recvbuf);
#endif /* ENABLE_MPI */

    /* Build checkpoint-shaped BULKI for local install. */
    {
        int                       n_local = 0;
        pdc_rescale_tmp_tq_obj_t *cur;
        BULKI_Entity *            objects_array;

        for (cur = assembled; cur != NULL; cur = cur->next)
            n_local++;

        install_bulki = BULKI_init(1);
        objects_array = empty_BULKI_Array_Entity_with_capacity(n_local > 0 ? n_local : 1);
        for (cur = assembled; cur != NULL; cur = cur->next) {
            BULKI *packed = rescale_pack_tq_obj(cur);
            if (packed == NULL)
                PGOTO_ERROR(FAIL, "Failed to pack assembled transfer_query for install");
            BULKI_ENTITY_append_BULKI_incremental(objects_array, packed);
            packed = (BULKI *)PDC_free(packed);
        }
        BULKI_put_incremental(install_bulki, BULKI_singleton_ENTITY("objects", PDC_STRING), objects_array);
    }

    ret_value = transfer_request_metadata_query_init_elastic_bulki(n_new, install_bulki);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to install remapped transfer_query state");

    LOG_INFO("Rank %d installed transfer_query after elastic migrate "
             "(%d assembled, %d dropped without home map entry)\n",
             pdc_server_rank_g, n_assembled, n_dropped);

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic transfer-query migrate complete\n");

done:
    if (install_bulki != NULL)
        BULKI_free(install_bulki, 1);
    while (assembled != NULL) {
        obj_tmp = assembled->next;
        rescale_free_one_tmp_tq(assembled);
        assembled = obj_tmp;
    }
    PDC_Server_rescale_free_tmp_tq();
#ifdef ENABLE_MPI
    if (ret_value != SUCCEED) {
        for (r = 0; r < n_new; r++) {
            if (dest_bufs != NULL && dest_bufs[r] != NULL)
                dest_bufs[r] = PDC_free(dest_bufs[r]);
            if (dest_bundles != NULL && dest_bundles[r] != NULL) {
                BULKI_free(dest_bundles[r], 1);
                dest_bundles[r] = NULL;
            }
            if (dest_arrays != NULL && dest_arrays[r] != NULL) {
                BULKI_Entity_free(dest_arrays[r], 1);
                dest_arrays[r] = NULL;
            }
        }
        if (sendbuf != NULL)
            sendbuf = (char *)PDC_free(sendbuf);
        if (recvbuf != NULL)
            recvbuf = (char *)PDC_free(recvbuf);
    }
    if (sendcounts != NULL)
        sendcounts = (int *)PDC_free(sendcounts);
    if (recvcounts != NULL)
        recvcounts = (int *)PDC_free(recvcounts);
    if (sdispls != NULL)
        sdispls = (int *)PDC_free(sdispls);
    if (rdispls != NULL)
        rdispls = (int *)PDC_free(rdispls);
    if (dest_nobj != NULL)
        dest_nobj = (int *)PDC_free(dest_nobj);
    if (dest_bufs != NULL)
        dest_bufs = (void **)PDC_free(dest_bufs);
    if (dest_bundles != NULL)
        dest_bundles = (BULKI **)PDC_free(dest_bundles);
    if (dest_arrays != NULL)
        dest_arrays = (BULKI_Entity **)PDC_free(dest_arrays);
#endif
    FUNC_LEAVE(ret_value);
}

/*
 * Remove stale server.cfg so clients cannot attach while elastic migration runs.
 * Rank 0 only; ENOENT is OK (no prior config).
 */
static perr_t
PDC_Server_rescale_unpublish_server_cfg(void)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    char   config_fname[ADDR_MAX];

    if (pdc_server_rank_g != 0)
        PGOTO_DONE(SUCCEED);

    if (strpbrk(pdc_server_tmp_dir_g, ";&|`$<>") != NULL)
        PGOTO_ERROR(FAIL, "Invalid characters in server tmp dir path");

    snprintf(config_fname, ADDR_MAX, "%s%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);
    if (remove(config_fname) != 0 && errno != ENOENT)
        PGOTO_ERROR(FAIL, "Unable to remove stale config file [%s]: %s", config_fname, strerror(errno));

    LOG_INFO("Unpublished stale %s pending elastic metadata migration\n", pdc_server_cfg_name_g);

done:
    FUNC_LEAVE(ret_value);
}

/* Ensure $PDC_TMPDIR/{0..n_new-1}/ exist (needed on scale-up for later checkpoints). */
static perr_t
PDC_Server_rescale_ensure_tmp_dirs(int n_new)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    i;

    if (n_new <= 0)
        PGOTO_ERROR(FAIL, "Invalid n_new=%d for tmp dir setup", n_new);
    if (strpbrk(pdc_server_tmp_dir_g, ";&|`$<>") != NULL)
        PGOTO_ERROR(FAIL, "Invalid characters in server tmp dir path");

    PDC_mkdir(pdc_server_tmp_dir_g);

    for (i = 0; i < n_new; i++) {
        char dir[ADDR_MAX];

        snprintf(dir, sizeof(dir), "%s/%d", pdc_server_tmp_dir_g, i);
        /* PDC_mkdir creates parents only; mkdir creates the leaf rank dir. */
        PDC_mkdir(dir);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST)
            PGOTO_ERROR(FAIL, "Failed to create server tmp dir [%s]: %s", dir, strerror(errno));
        if (access(dir, F_OK) != 0)
            PGOTO_ERROR(FAIL, "Server tmp dir missing after create [%s]: %s", dir, strerror(errno));
    }

    if (pdc_server_rank_g == 0)
        LOG_INFO("Ensured %d per-rank directories under %s for elastic restart\n", n_new,
                 pdc_server_tmp_dir_g);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_restart_elastic(int n_old, int n_new)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (n_old <= 0 || n_new <= 0)
        PGOTO_ERROR(FAIL, "Invalid server counts for elastic restart (n_old=%d, n_new=%d)", n_old, n_new);

    if (n_old == n_new)
        PGOTO_ERROR(FAIL,
                    "PDC_Server_restart_elastic called with n_old == n_new (%d); use PDC_Server_restart",
                    n_old);

    if (n_old > 65536 || n_new > 65536)
        PGOTO_ERROR(FAIL, "Server count out of range for elastic restart (n_old=%d, n_new=%d)", n_old, n_new);

#ifdef PDC_ENABLE_IDIOMS
    /* Index shards assume fixed N; do not migrate or recover under N_new until rebuild exists. */
    PGOTO_ERROR(FAIL,
                "Elastic server-count restart (%d -> %d) is not supported when IDIOMS is enabled; "
                "restart with the same server count, or rebuild without PDC_ENABLE_IDIOMS until "
                "IDIOMS index rebuild after metadata migration is implemented",
                n_old, n_new);
#endif

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic server restart requested: %d -> %d\n", n_old, n_new);

#ifndef ENABLE_MPI
    if (n_new != 1)
        PGOTO_ERROR(FAIL, "Elastic restart with n_new=%d requires MPI", n_new);
#endif

    /* Gate: drop stale server.cfg before long migration so clients wait for republish. */
    ret_value = PDC_Server_rescale_unpublish_server_cfg();
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to unpublish stale server.cfg before elastic migrate");
    ret_value = PDC_Server_unpublish_obj_id_home_map();
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to unpublish stale obj_id home map before elastic migrate");
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    ret_value = PDC_Server_rescale_validate_shards(n_old, n_new);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Elastic restart shard validation failed");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    ret_value = PDC_Server_rescale_load_shards(n_old, n_new);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Elastic restart shard load failed");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    ret_value = PDC_Server_rescale_migrate_objects(n_new);
    if (ret_value != SUCCEED) {
        PDC_Server_rescale_free_tmp_conts();
        PDC_Server_rescale_free_tmp_tq();
        PGOTO_ERROR(FAIL, "Elastic object migration failed");
    }

    /* Objects now live in metadata_hash_table_g — do not free them. */
    if (rescale_tmp_objs_g != NULL)
        PDC_Server_rescale_free_tmp_objs();

    ret_value = PDC_Server_rescale_migrate_containers(n_new);
    if (ret_value != SUCCEED) {
        PDC_Server_rescale_free_tmp_tq();
        PGOTO_ERROR(FAIL, "Elastic container migration failed");
    }

    /* Containers now live in container_hash_table_g — do not free HT-owned entries. */
    if (rescale_tmp_conts_g != NULL)
        PDC_Server_rescale_free_tmp_conts();

    /* New creates must not reuse IDs from this rank's interval after restore. */
    ret_value = PDC_Server_rescale_recompute_id_seq();
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to recompute pdc_id_seq_g after elastic migrate");

    /* Full cluster obj_id/cont_id → home map for server-side ID forwarding. */
    ret_value = PDC_Server_rescale_build_obj_id_home_map(n_new);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to build elastic obj_id home map");

    /* Client-visible sidecar before server.cfg so connect can load the map. */
    ret_value = PDC_Server_publish_obj_id_home_map(n_new);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to publish elastic obj_id home map");
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    /* Transfer-query: migrate to metadata homes, remap data_server_id, install. */
    ret_value = PDC_Server_rescale_migrate_transfer_query(n_new);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Elastic transfer-query migration failed");

    /* Scale-up: create missing per-rank dirs before clients attach. */
    ret_value = PDC_Server_rescale_ensure_tmp_dirs(n_new);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Elastic restart tmp dir setup failed");

        /*
         * Metadata tables are stable. Barrier so no rank returns early; caller
         * (server_run) then publishes server.cfg with N_new via write_addr_to_file.
         */
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (pdc_server_rank_g == 0)
        LOG_INFO("Elastic metadata migration complete (%d -> %d); ready for server.cfg publish\n", n_old,
                 n_new);

done:
    FUNC_LEAVE(ret_value);
}
