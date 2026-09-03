#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <mpi.h>

#include "pdc_utlist.h"
#include "pdc_config.h"
#include "pdc_id_pkg.h"
#include "pdc_obj.h"
#include "pdc_malloc.h"
#include "pdc_region.h"
#include "pdc_region_pkg.h"
#include "pdc_client_cache.h"
#include "pdc_client_cache_dl.h"
#include "pdc_client_cache_prefetch.h"
#include "pdc_client_connect.h"

MPI_Comm client_cache_world_comm;
MPI_Comm client_cache_node_comm;

pdc_client_info client_info;

perr_t
PDC_client_cache_dl_prepend(pdc_object_data *obj_cache_item)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    // Update metadata list information
    // Current item will be prepended to the head node
    obj_cache_item->next = client_info.local_cache_list_head;
    obj_cache_item->prev = NULL;

    // If list already exists, current head node's previous index should also point to itself
    if (client_info.local_cache_list_head != NULL) {
        obj_cache_item->next->prev = obj_cache_item;
    }

    // The new node becomes the head node of the object cache list
    client_info.local_cache_list_head = obj_cache_item;

    // If list did not exist, update the tail node metadata
    if (client_info.local_cache_list_tail == NULL) {
        client_info.local_cache_list_tail = obj_cache_item;
    }

    client_info.cached_item_num++;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_client_cache_dl_delete(pdc_object_data *obj_cache_item)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    // Update current item prev item's next item index
    if (obj_cache_item->prev != NULL) {
        obj_cache_item->prev->next = obj_cache_item->next;
    }
    else {
        client_info.local_cache_list_head = obj_cache_item->next; // if the item is the head
    }

    // Update current item next item's previous item index
    if (obj_cache_item->next != NULL) {
        obj_cache_item->next->prev = obj_cache_item->prev;
    }
    else {
        client_info.local_cache_list_tail = obj_cache_item->prev; // if the item is the tail
    }

    client_info.cached_item_num--;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void
init_free_stack()
{
    for (int i = 0; i < MAX_SLOTS_PER_NODE - 1; i++) {
        client_info.header->free_list[i] = i + 1;
    }
    client_info.header->free_list[MAX_SLOTS_PER_NODE - 1] = SLOT_INVALID;
    client_info.header->free_stack_head                   = 0;
}

char *
get_data_ptr(int shared_index)
{
    size_t offset = (size_t)shared_index * (size_t)MAX_ITEM_SIZE;

    return client_info.node_shared_data_base + offset;
}

int
pop_free_slot()
{
    int current_head, next_head;

    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, 0, 0, client_info.node_shared_data_win);

    MPI_Get(&current_head, 1, MPI_INT, 0, offsetof(SharedMemoryHeader, free_stack_head), 1, MPI_INT,
            client_info.node_shared_data_win);

    if (current_head == SLOT_INVALID) {
        MPI_Win_unlock(0, client_info.node_shared_data_win);
        return SLOT_INVALID;
    }

    MPI_Get(&next_head, 1, MPI_INT, 0, offsetof(SharedMemoryHeader, free_list) + current_head * sizeof(int),
            1, MPI_INT, client_info.node_shared_data_win);

    MPI_Put(&next_head, 1, MPI_INT, 0, offsetof(SharedMemoryHeader, free_stack_head), 1, MPI_INT,
            client_info.node_shared_data_win);

    MPI_Win_unlock(0, client_info.node_shared_data_win);

    // printf("[RANK %d] pop_free_slot index: %d\n", client_info.world_rank, current_head);
    // fflush(stdout);

    return current_head;
}

void
push_free_slot(int slot_idx)
{
    if (slot_idx < 0 || slot_idx >= MAX_SLOTS_PER_NODE)
        return;

    int current_head;

    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, 0, 0, client_info.node_shared_data_win);

    MPI_Get(&current_head, 1, MPI_INT, 0, offsetof(SharedMemoryHeader, free_stack_head), 1, MPI_INT,
            client_info.node_shared_data_win);

    MPI_Put(&current_head, 1, MPI_INT, 0, offsetof(SharedMemoryHeader, free_list) + slot_idx * sizeof(int), 1,
            MPI_INT, client_info.node_shared_data_win);

    MPI_Put(&slot_idx, 1, MPI_INT, 0, offsetof(SharedMemoryHeader, free_stack_head), 1, MPI_INT,
            client_info.node_shared_data_win);

    MPI_Win_unlock(0, client_info.node_shared_data_win);
}

int
count_free_slots()
{
    int count = 0, current;

    MPI_Win_lock(MPI_LOCK_SHARED, 0, 0, client_info.node_shared_data_win);
    MPI_Fetch_and_op(NULL, &current, MPI_INT, 0, offsetof(SharedMemoryHeader, free_stack_head), MPI_NO_OP,
                     client_info.node_shared_data_win);
    MPI_Win_flush(0, client_info.node_shared_data_win);

    while (current != SLOT_INVALID && count < MAX_SLOTS_PER_NODE) {
        count++;
        current = client_info.header->free_list[current];
    }

    MPI_Win_unlock(0, client_info.node_shared_data_win);

    return count;
}

perr_t
PDC_client_cache_dl_init()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int *  node_world_ranks;
    int    mpi_alloc_error;

    client_info.world_rank = pdc_client_mpi_rank_g;
    client_info.world_size = pdc_client_mpi_size_g;

    MPI_Comm_dup(MPI_COMM_WORLD, &client_cache_world_comm); // Duplicate MPI_COMM_WORLD for client cache

    // Step 1. Node-level shared memory client_init setup
    MPI_Comm_split_type(client_cache_world_comm, MPI_COMM_TYPE_SHARED, client_info.world_rank, MPI_INFO_NULL,
                        &client_cache_node_comm);
    MPI_Comm_rank(client_cache_node_comm, &client_info.node_rank);
    MPI_Comm_size(client_cache_node_comm, &client_info.node_size);

    // Step 2. Broadcast node manager's rank to group clients by node
    if (client_info.node_rank == 0)
        client_info.node_manager_rank = client_info.world_rank;

    MPI_Bcast(&client_info.node_manager_rank, 1, MPI_INT, 0, client_cache_node_comm);

    // Step 3. Create a map for distinguishing inter-node and intra-node client group
    client_info.rank_to_node_id_map = (int *)PDC_malloc(client_info.world_size * sizeof(int));
    if (!client_info.rank_to_node_id_map)
        PGOTO_ERROR(FAIL, "PDC_client_cache_dl_init - rank_to_node_id_map memory allocation failed");

    MPI_Allgather(&client_info.node_manager_rank, 1, MPI_INT, client_info.rank_to_node_id_map, 1, MPI_INT,
                  client_cache_world_comm);

    printf("[RANK %d] PDC_client_cache_dl_init - step 3: rank_to_node_id_map\n", client_info.world_rank);
    fflush(stdout);

    // Step 4. Create local node map
    node_world_ranks = (int *)PDC_malloc(client_info.node_size * sizeof(int));
    if (!node_world_ranks)
        PGOTO_ERROR(FAIL, "PDC_client_cache_dl_init - node_world_ranks memory allocation failed");

    client_info.world_to_node_rank_map = (int *)PDC_calloc(client_info.world_size, sizeof(int));
    if (!client_info.world_to_node_rank_map)
        PGOTO_ERROR(FAIL, "PDC_client_cache_dl_init - world_to_node_rank_map memory allocation failed");

    MPI_Allgather(&client_info.world_rank, 1, MPI_INT, node_world_ranks, 1, MPI_INT, client_cache_node_comm);
    for (int i = 0; i < client_info.node_size; ++i) {
        client_info.world_to_node_rank_map[node_world_ranks[i]] = i;
    }

    PDC_free(node_world_ranks);

    printf("[RANK %d] PDC_client_cache_dl_init - step 4: world_to_node_rank_map memory allocation\n",
           client_info.world_rank);
    fflush(stdout);

    // Step 5. Create shared memory window for node shared data
    MPI_Aint total_bytes =
        (MPI_Aint)sizeof(SharedMemoryHeader) + ((MPI_Aint)MAX_ITEM_SIZE * (MPI_Aint)MAX_SLOTS_PER_NODE);
    MPI_Aint local_alloc_size = (client_info.node_rank == 0) ? total_bytes : 0;

    mpi_alloc_error =
        MPI_Win_allocate_shared(local_alloc_size, 1, MPI_INFO_NULL, client_cache_node_comm,
                                &client_info.node_shared_base, &client_info.node_shared_data_win);

    printf("[RANK %d] PDC_client_cache_dl_init - step 5: Create shared memory window\n",
           client_info.world_rank);
    fflush(stdout);

    if (mpi_alloc_error != MPI_SUCCESS)
        MPI_Abort(client_cache_world_comm, 1);

    if (client_info.node_rank != 0) {
        MPI_Aint size_out;
        int      disp_unit;

        MPI_Win_shared_query(client_info.node_shared_data_win, 0, &size_out, &disp_unit,
                             &client_info.node_shared_base);
    }

    printf("[RANK %d] PDC_client_cache_dl_init - step 6: Query shared memory window\n",
           client_info.world_rank);
    fflush(stdout);

    client_info.header                = (SharedMemoryHeader *)client_info.node_shared_base;
    client_info.node_shared_data_base = (char *)client_info.node_shared_base + sizeof(SharedMemoryHeader);

    if (client_info.node_rank == 0) {
        init_free_stack();
        // printf("PDC_client_cache_dl_init - step 7 -1 : Init the node shared memory\n",
        // client_info.world_rank); fflush(stdout); memset(client_info.node_shared_data_base, 0,
        // (size_t)MAX_SLOTS_PER_NODE * (size_t)MAX_ITEM_SIZE);
    }

    printf("[RANK %d] PDC_client_cache_dl_init - step 7: Init the node shared memory\n",
           client_info.world_rank);
    fflush(stdout);

    MPI_Barrier(client_cache_node_comm);
    MPI_Barrier(client_cache_world_comm);

    client_info.local_cache_list_head = NULL;
    client_info.local_cache_list_tail = NULL;
    client_info.cached_item_num       = 0;
    client_info.client_cache_init     = 1;

done:
    FUNC_LEAVE(ret_value);
}

// TODO:
// Management of overalapping regions
perr_t
PDC_client_cache_dl_insert(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                           void *buf, uint64_t read_size)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    pdc_object_data *obj_cache_item = NULL;
    double           start          = MPI_Wtime();
    int              slot_idx;
    char *           data_ptr;

    // Check if the client cache has been initialized
    if (!client_info.client_cache_init)
        PGOTO_ERROR(FAIL, "PDC_client_cache_dl_insert - object cache list not initialized");

    // Evict item if exceeding cache size
    // while (client_info.cached_item_num > MAX_ITEM_NUM)
    //     PDC_client_cache_evict();

    slot_idx = pop_free_slot();

    while (slot_idx == SLOT_INVALID) {
        PDC_client_cache_evict();
        slot_idx = pop_free_slot();
    }

    obj_cache_item = (pdc_object_data *)PDC_malloc(sizeof(pdc_object_data));
    if (!obj_cache_item)
        PGOTO_ERROR(FAIL, "PDC region cache - obj_cache_item memory allocation failed");

    // Create object data
    obj_cache_item->obj_id             = obj_id;
    obj_cache_item->unit               = unit;
    obj_cache_item->reg_ndim           = ndim;
    obj_cache_item->reg_buf_size       = (read_size > MAX_ITEM_SIZE) ? MAX_ITEM_SIZE : read_size;
    obj_cache_item->target_rank        = -1;
    obj_cache_item->data_exchange_type = 0;
    obj_cache_item->slot_idx           = slot_idx;

    memcpy(obj_cache_item->reg_offset, offset, ndim * sizeof(uint64_t));
    memcpy(obj_cache_item->reg_size, size, ndim * sizeof(uint64_t));

    data_ptr = get_data_ptr(slot_idx);
    memcpy(data_ptr, buf, obj_cache_item->reg_buf_size * sizeof(char));

    obj_cache_item->prev = NULL;
    obj_cache_item->next = NULL;

    ret_value = PDC_client_cache_dl_prepend(obj_cache_item);

    PDC_client_cache_timelog(start, "PDC_client_cache_dl_insert - prepend new item to obj_cache list time");

done:
    FUNC_LEAVE(ret_value);
}

// TODO:
// Need to manage the object's offset cache information
// Need to manage when partial region is contained within
int
PDC_client_cache_dl_local_search(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                                 void *buf, uint64_t read_size)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    uint64_t *       overlap_offset, *overlap_size;
    int              region_copy = 1;
    char *           data_ptr;
    int              is_cached = 0;
    pdc_object_data *obj_cache_iter;
    double           start, tmp_start, total_start = MPI_Wtime();

    if (!client_info.client_cache_init)
        PGOTO_ERROR(FAIL, "PDC_client_cache_dl_search - object cache list not initialized");

    // Find if region is cached into local object list cache
    // If region contained, return the rank that contains the region
    obj_cache_iter = client_info.local_cache_list_head;

    while (obj_cache_iter != NULL) {
        if (obj_cache_iter->obj_id == obj_id) {
            is_cached = detect_region_contained(offset, size, obj_cache_iter->reg_offset,
                                                obj_cache_iter->reg_size, ndim);

            // If region contained, memcpy cached region data into transfer_request->buf
            if (is_cached) {
                start = MPI_Wtime();

                // Detect the offset range that is overlapped
                PDC_region_overlap_detect(ndim, offset, size, obj_cache_iter->reg_offset,
                                          obj_cache_iter->reg_size, &overlap_offset, &overlap_size);

                // memcpy the overlapped region
                data_ptr = get_data_ptr(obj_cache_iter->slot_idx);

                tmp_start = MPI_Wtime();

                for (int i = 0; i < ndim; ++i) {
                    if (offset[i] != obj_cache_iter->reg_offset[i]) {
                        region_copy = 0;
                        break;
                    }
                    if (size[i] != obj_cache_iter->reg_size[i]) {
                        region_copy = 0;
                        break;
                    }
                }

                if (region_copy) {
                    memcpy(buf, data_ptr, obj_cache_iter->reg_buf_size);
                    if (client_info.world_rank == 0)
                        printf("[RANK %d] Read entire region for obj_id: %lld\n", client_info.world_rank,
                               obj_id);
                }
                else {
                    memcpy_overlap_subregion(obj_cache_iter->reg_ndim, unit, data_ptr,
                                             obj_cache_iter->reg_offset, obj_cache_iter->reg_size, buf,
                                             offset, size, overlap_offset, overlap_size);
                }

                PDC_client_cache_timelog(tmp_start, "PDC_client_cache_dl_local_search - memcpy data to buf");

                // Follow the LRU policy
                ret_value = PDC_client_cache_dl_delete(obj_cache_iter);
                ret_value = PDC_client_cache_dl_prepend(obj_cache_iter);

                free(overlap_offset);

                PDC_client_cache_timelog(start, "PDC_client_cache_dl_local_search - local cache hit time");

                break;
            }
        }

        obj_cache_iter = obj_cache_iter->next;
    }

    if (!is_cached)
        printf("[RANK %d] cache miss for obj_id: %lld\n", client_info.world_rank, obj_id);
    else
        printf("[RANK %d] cache hit for obj_id: %lld\n", client_info.world_rank, obj_id);

    PDC_client_cache_timelog(total_start, "PDC_client_cache_dl_local_search search time");

done:
    FUNC_LEAVE(is_cached);
}

perr_t
PDC_client_cache_dl_prepare_data_exchange(pdcid_t *global_prefetch_list, uint64_t *offset, uint64_t *size,
                                          int obj_prefetch_list_len)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    pdc_object_data *obj_cache_iter;
    int              i, j, prefetch_list_idx = 0, region_compare = 0, is_contained = 0;
    double           start = MPI_Wtime();

    if (offset != NULL) {
        region_compare = 1;
    }

    // Set the target rank for local cached object item
    obj_cache_iter = client_info.local_cache_list_head;

    while (obj_cache_iter != NULL) {
        for (i = 0; i < client_info.world_size; i++) {
            for (j = 0; j < obj_prefetch_list_len; j++) {
                if (obj_cache_iter->obj_id == global_prefetch_list[prefetch_list_idx]) {
                    if (!region_compare) {
                        obj_cache_iter->target_rank = i;
                        break;
                    }
                    else {
                        is_contained =
                            detect_region_contained(&offset[i], &size[i], obj_cache_iter->reg_offset,
                                                    obj_cache_iter->reg_size, obj_cache_iter->reg_ndim);

                        // If the prefetch list is contained within the local object cache list
                        // and if the target_rank is not the same as node_manager_rank
                        if (is_contained) {
                            obj_cache_iter->target_rank = i;
                            break;
                        }
                    }
                }

                prefetch_list_idx++;
            }
            if (obj_cache_iter->target_rank != -1) {
                break;
            }
        }

        is_contained      = 0;
        obj_cache_iter    = obj_cache_iter->next;
        prefetch_list_idx = 0;
    }

    // For debugging purpose
    // obj_cache_iter = client_info.local_cache_list_head;
    // while (obj_cache_iter != NULL) {
    //     printf("[RANK %d] prepare_data_exchange: object_id %lld , target_rank: %d\n",
    //     client_info.world_rank, obj_cache_iter->obj_id, obj_cache_iter->target_rank);

    //     fflush(stdout);

    //     obj_cache_iter = obj_cache_iter->next;
    // }

    PDC_client_cache_timelog(start,
                             "PDC_client_cache_dl_prepare_data_exchange - global prefetch list preparation");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_client_cache_dl_data_exchange(pdcid_t *global_prefetch_list, int obj_prefetch_list_len)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    pdc_object_data *obj_cache_iter, *exchange_head;
    char *intra_node_send_buf, *inter_node_send_buf, *temp_intra_recv_buf = NULL, *temp_inter_recv_buf = NULL;
    double start = MPI_Wtime(), tmp_timer, tmp_timer2;

    int old_cached_item_num = client_info.cached_item_num;

    tmp_timer = MPI_Wtime();

    // Step 1: Calculating the max buffer sizes and allocate reusable data exchange buffers
    int    chunk_size           = old_cached_item_num / NUM_CHUNKS;
    size_t max_intra_send_chunk = 0, max_inter_send_chunk = 0, local_max_send_chunk = 0,
           max_intra_recv_chunk = 0, max_inter_recv_chunk = 0;
    // size_t max_intra_send_chunk = 0, max_inter_send_chunk = 0, local_max_send_chunk = 0;

    // Prepare the buffers required within the data exchange
    int intra_node_item_count[NUM_CHUNKS];
    int inter_node_item_count[NUM_CHUNKS];

    memset(intra_node_item_count, 0, sizeof(intra_node_item_count));
    memset(inter_node_item_count, 0, sizeof(inter_node_item_count));

    int intra_node_send_counts[NUM_CHUNKS][client_info.node_size];
    int inter_node_send_counts[NUM_CHUNKS][client_info.world_size];
    int intra_node_recv_counts[NUM_CHUNKS][client_info.node_size];
    int inter_node_recv_counts[NUM_CHUNKS][client_info.world_size];

    memset(intra_node_send_counts, 0, sizeof(intra_node_send_counts));
    memset(inter_node_send_counts, 0, sizeof(inter_node_send_counts));
    memset(intra_node_recv_counts, 0, sizeof(intra_node_recv_counts));
    memset(inter_node_recv_counts, 0, sizeof(inter_node_recv_counts));

    int *sdispls_intra = (int *)PDC_malloc(client_info.node_size * sizeof(int));
    int *rdispls_intra = (int *)PDC_malloc(client_info.node_size * sizeof(int));
    int *sdispls_inter = (int *)PDC_malloc(client_info.world_size * sizeof(int));
    int *rdispls_inter = (int *)PDC_malloc(client_info.world_size * sizeof(int));

    MPI_Datatype mpi_intra_transfer_unit;
    MPI_Type_contiguous(INTRA_TRANSFER_UNIT_SIZE, MPI_BYTE, &mpi_intra_transfer_unit);
    MPI_Type_commit(&mpi_intra_transfer_unit);

    MPI_Datatype mpi_inter_transfer_unit;
    MPI_Type_contiguous(INTER_TRANSFER_UNIT_SIZE, MPI_BYTE, &mpi_inter_transfer_unit);
    MPI_Type_commit(&mpi_inter_transfer_unit);

    for (int c = 0; c < NUM_CHUNKS; c++) {
        int start_idx = c * chunk_size;
        int end_idx   = (c == NUM_CHUNKS - 1) ? old_cached_item_num : (c + 1) * chunk_size;

        int i = 0;
        for (obj_cache_iter = client_info.local_cache_list_head; obj_cache_iter != NULL;
             obj_cache_iter = obj_cache_iter->next) {
            if (i >= start_idx && i < end_idx) {
                if (client_info.rank_to_node_id_map[obj_cache_iter->target_rank] ==
                    client_info.node_manager_rank) {
                    int target_nrank = client_info.world_to_node_rank_map[obj_cache_iter->target_rank];
                    intra_node_send_counts[c][target_nrank]++;
                    intra_node_item_count[c]++;
                }
                else {
                    inter_node_send_counts[c][obj_cache_iter->target_rank]++;
                    inter_node_item_count[c]++;
                }
            }
            i++;
        }
        if (intra_node_item_count[c] > max_intra_send_chunk)
            max_intra_send_chunk = intra_node_item_count[c];
        if (inter_node_item_count[c] > max_inter_send_chunk)
            max_inter_send_chunk = inter_node_item_count[c];

        MPI_Alltoall(intra_node_send_counts[c], 1, MPI_INT, intra_node_recv_counts[c], 1, MPI_INT,
                     client_cache_node_comm);
        MPI_Alltoall(inter_node_send_counts[c], 1, MPI_INT, inter_node_recv_counts[c], 1, MPI_INT,
                     client_cache_world_comm);

        size_t total_intra_recv_items_chunk = 0;
        for (i = 0; i < client_info.node_size; i++) {
            total_intra_recv_items_chunk += (size_t)intra_node_recv_counts[c][i];
        }

        if (total_intra_recv_items_chunk > max_intra_recv_chunk)
            max_intra_recv_chunk = total_intra_recv_items_chunk;

        size_t total_inter_recv_items_chunk = 0;
        for (i = 0; i < client_info.world_size; i++) {
            total_inter_recv_items_chunk += inter_node_recv_counts[c][i];
        }

        if (total_inter_recv_items_chunk > max_inter_recv_chunk)
            max_inter_recv_chunk = total_inter_recv_items_chunk;
    }

    if (max_intra_send_chunk > max_inter_send_chunk)
        local_max_send_chunk = max_intra_send_chunk;
    else
        local_max_send_chunk = max_inter_send_chunk;

    size_t global_max_chunk_size = 0;
    MPI_Allreduce(&local_max_send_chunk, &global_max_chunk_size, 1, MPI_UNSIGNED_LONG, MPI_MAX,
                  client_cache_world_comm);

    intra_node_send_buf = (char *)PDC_malloc(global_max_chunk_size * INTRA_TRANSFER_UNIT_SIZE + 1);
    inter_node_send_buf = (char *)PDC_malloc(global_max_chunk_size * INTER_TRANSFER_UNIT_SIZE + 1);
    temp_intra_recv_buf = (char *)PDC_malloc(max_intra_recv_chunk * INTRA_TRANSFER_UNIT_SIZE + 1);
    temp_inter_recv_buf = (char *)PDC_malloc(max_inter_recv_chunk * INTER_TRANSFER_UNIT_SIZE + 1);

    printf("[RANK %d] global_max_chunk_size: %d, max_intra_recv_chunk: %d, max_inter_recv_chunk: %d\n",
           client_info.world_rank, global_max_chunk_size, max_intra_recv_chunk, max_inter_recv_chunk);

    memset(intra_node_send_buf, 0, global_max_chunk_size * INTRA_TRANSFER_UNIT_SIZE + 1);
    memset(inter_node_send_buf, 0, global_max_chunk_size * INTER_TRANSFER_UNIT_SIZE + 1);
    memset(temp_intra_recv_buf, 0, max_intra_recv_chunk * INTRA_TRANSFER_UNIT_SIZE + 1);
    memset(temp_inter_recv_buf, 0, max_inter_recv_chunk * INTER_TRANSFER_UNIT_SIZE + 1);

    MPI_Barrier(client_cache_world_comm);

    // if (client_info.world_rank == 0)
    //     printf("[RANK %d] Before data exchange total cached item num %d %d\n", client_info.world_rank,
    //     old_cached_item_num, client_info.cached_item_num);

    PDC_client_cache_timelog(tmp_timer, "PDC_client_cache_dl_data_exchange - Step 1");

    tmp_timer = MPI_Wtime();

    // Step 2: Data exchange loop
    exchange_head = client_info.local_cache_list_head;
    for (int c = 0; c < NUM_CHUNKS; c++) {
        tmp_timer2    = MPI_Wtime();
        int start_idx = 0;
        int end_idx   = chunk_size;

        if (c == NUM_CHUNKS - 1) {
            if (old_cached_item_num % NUM_CHUNKS != 0)
                end_idx = old_cached_item_num % NUM_CHUNKS;
        }

        // Step 2-1: Intra-node shuffle for current chunk
        int    i                            = 0;
        int    current_sdisp_items          = 0;
        int    current_rdisp_items          = 0;
        size_t total_intra_recv_items_chunk = 0;

        tmp_timer2 = MPI_Wtime();

        for (i = 0; i < client_info.node_size; i++) {
            // printf("[RANK %d] data_Exchange - intra_node_send_counts %d intra_node_recv_counts %d\n",
            // pdc_client_mpi_rank_g, intra_node_send_counts[i], intra_node_recv_counts[i], i);
            total_intra_recv_items_chunk += (size_t)intra_node_recv_counts[c][i];

            sdispls_intra[i] = current_sdisp_items;
            rdispls_intra[i] = current_rdisp_items;

            current_sdisp_items += intra_node_send_counts[c][i];
            current_rdisp_items += intra_node_recv_counts[c][i];
        }

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - intra-node shuffle preparation");
        tmp_timer2 = MPI_Wtime();

        // Packing intra-node send buffer
        if (intra_node_item_count[c] > 0) {
            int *temp_offsets_intra = (int *)PDC_calloc(client_info.node_size, sizeof(int));
            int  item_idx           = 0;

            for (obj_cache_iter = exchange_head; obj_cache_iter != NULL;
                 obj_cache_iter = obj_cache_iter->next) {
                if (item_idx >= start_idx && item_idx < end_idx) {
                    int trg_rank = obj_cache_iter->target_rank;

                    if (client_info.rank_to_node_id_map[trg_rank] == client_info.node_manager_rank) {
                        int    trg_nrank = client_info.world_to_node_rank_map[trg_rank];
                        size_t offset =
                            ((size_t)sdispls_intra[trg_nrank] + (size_t)temp_offsets_intra[trg_nrank]) *
                            INTRA_TRANSFER_UNIT_SIZE;
                        char * send_ptr       = intra_node_send_buf + offset;
                        size_t current_offset = 0;

                        memcpy(send_ptr + current_offset, &obj_cache_iter->obj_id, sizeof(pdcid_t));
                        current_offset += sizeof(pdcid_t);
                        memcpy(send_ptr + current_offset, &obj_cache_iter->unit, sizeof(uint64_t));
                        current_offset += sizeof(uint64_t);
                        memcpy(send_ptr + current_offset, &obj_cache_iter->reg_ndim, sizeof(int));
                        current_offset += sizeof(int);
                        memcpy(send_ptr + current_offset, &obj_cache_iter->slot_idx, sizeof(int));
                        current_offset += sizeof(int);
                        memcpy(send_ptr + current_offset, obj_cache_iter->reg_offset, sizeof(uint64_t) * 3);
                        current_offset += (sizeof(uint64_t) * 3);
                        memcpy(send_ptr + current_offset, obj_cache_iter->reg_size, sizeof(uint64_t) * 3);
                        current_offset += (sizeof(uint64_t) * 3);
                        memcpy(send_ptr + current_offset, &obj_cache_iter->reg_buf_size, sizeof(uint64_t));

                        obj_cache_iter->data_exchange_type = 0; // intra node data exchange

                        temp_offsets_intra[trg_nrank]++;
                    }
                }

                item_idx++;
            }
            free(temp_offsets_intra);
        }

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - intra-node shuffle pack send buffer");
        tmp_timer2 = MPI_Wtime();

        // Collective call for intra node shuffle
        MPI_Alltoallv(intra_node_send_buf, intra_node_send_counts[c], sdispls_intra, mpi_intra_transfer_unit,
                      temp_intra_recv_buf, intra_node_recv_counts[c], rdispls_intra, mpi_intra_transfer_unit,
                      client_cache_node_comm);

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - intra-node shuffle alltoall");
        tmp_timer2 = MPI_Wtime();

        // Unpacking intra-node recv buffer
        if (total_intra_recv_items_chunk > 0) {
            for (int sender_nrank = 0; sender_nrank < client_info.node_size; sender_nrank++) {
                int   items_from_sender = intra_node_recv_counts[c][sender_nrank];
                char *base_recv_ptr =
                    temp_intra_recv_buf + ((size_t)rdispls_intra[sender_nrank] * INTRA_TRANSFER_UNIT_SIZE);

                for (int j = 0; j < items_from_sender; ++j) {
                    pdc_object_data *obj_cache_item = (pdc_object_data *)PDC_malloc(sizeof(pdc_object_data));
                    char *           recv_ptr       = base_recv_ptr + (j * (size_t)INTRA_TRANSFER_UNIT_SIZE);
                    size_t           current_offset = 0;

                    memcpy(&obj_cache_item->obj_id, recv_ptr + current_offset, sizeof(pdcid_t));
                    current_offset += sizeof(pdcid_t);
                    memcpy(&obj_cache_item->unit, recv_ptr + current_offset, sizeof(uint64_t));
                    current_offset += sizeof(uint64_t);
                    memcpy(&obj_cache_item->reg_ndim, recv_ptr + current_offset, sizeof(int));
                    current_offset += sizeof(int);
                    memcpy(&obj_cache_item->slot_idx, recv_ptr + current_offset, sizeof(int));
                    current_offset += sizeof(int);
                    memcpy(obj_cache_item->reg_offset, recv_ptr + current_offset, sizeof(uint64_t) * 3);
                    current_offset += (sizeof(uint64_t) * 3);
                    memcpy(obj_cache_item->reg_size, recv_ptr + current_offset, sizeof(uint64_t) * 3);
                    current_offset += (sizeof(uint64_t) * 3);
                    memcpy(&obj_cache_item->reg_buf_size, recv_ptr + current_offset, sizeof(uint64_t));

                    obj_cache_item->target_rank        = -1;
                    obj_cache_item->data_exchange_type = 0;

                    PDC_client_cache_dl_prepend(obj_cache_item);
                }
            }
        }

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - intra-node shuffle unpack recv buffer");
        tmp_timer2 = MPI_Wtime();

        // Stage 3: Inter-node data exchange for current chunk
        size_t total_inter_recv_items_chunk = 0;
        current_sdisp_items                 = 0;
        current_rdisp_items                 = 0;

        for (i = 0; i < client_info.world_size; i++) {
            total_inter_recv_items_chunk += inter_node_recv_counts[c][i];

            sdispls_inter[i] = current_sdisp_items;
            rdispls_inter[i] = current_rdisp_items;

            current_sdisp_items += inter_node_send_counts[c][i];
            current_rdisp_items += inter_node_recv_counts[c][i];
        }

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - inter-node shuffle preparation");
        tmp_timer2 = MPI_Wtime();

        // Packing inter node send buffer
        if (inter_node_item_count[c] > 0) {
            int *temp_offsets_inter = (int *)PDC_calloc(client_info.world_size, sizeof(int));
            int  item_idx           = 0;
            for (obj_cache_iter = exchange_head; obj_cache_iter != NULL;
                 obj_cache_iter = obj_cache_iter->next) {
                if (item_idx >= start_idx && item_idx < end_idx) {
                    int trg_rank = obj_cache_iter->target_rank;
                    if (client_info.rank_to_node_id_map[trg_rank] != client_info.node_manager_rank) {
                        size_t offset =
                            ((size_t)sdispls_inter[trg_rank] + (size_t)temp_offsets_inter[trg_rank]) *
                            INTER_TRANSFER_UNIT_SIZE;
                        char * send_ptr       = inter_node_send_buf + offset;
                        size_t current_offset = 0;

                        memcpy(send_ptr + current_offset, &obj_cache_iter->obj_id, sizeof(pdcid_t));
                        current_offset += sizeof(pdcid_t);
                        memcpy(send_ptr + current_offset, &obj_cache_iter->unit, sizeof(uint64_t));
                        current_offset += sizeof(uint64_t);
                        memcpy(send_ptr + current_offset, &obj_cache_iter->reg_ndim, sizeof(int));
                        current_offset += sizeof(int);
                        memcpy(send_ptr + current_offset, obj_cache_iter->reg_offset, sizeof(uint64_t) * 3);
                        current_offset += (sizeof(uint64_t) * 3);
                        memcpy(send_ptr + current_offset, obj_cache_iter->reg_size, sizeof(uint64_t) * 3);
                        current_offset += (sizeof(uint64_t) * 3);
                        memcpy(send_ptr + current_offset, &obj_cache_iter->reg_buf_size, sizeof(uint64_t));
                        current_offset += sizeof(uint64_t);

                        char *data_ptr = get_data_ptr(obj_cache_iter->slot_idx);

                        memcpy(send_ptr + current_offset, data_ptr,
                               obj_cache_iter->reg_buf_size * sizeof(char));

                        obj_cache_iter->data_exchange_type = 1; // inter node data exchange

                        temp_offsets_inter[trg_rank]++;
                    }
                }
                item_idx++;
            }
            free(temp_offsets_inter);
        }

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - inter-node shuffle pack send buffer");
        tmp_timer2 = MPI_Wtime();

        MPI_Alltoallv(inter_node_send_buf, inter_node_send_counts[c], sdispls_inter, mpi_inter_transfer_unit,
                      temp_inter_recv_buf, inter_node_recv_counts[c], rdispls_inter, mpi_inter_transfer_unit,
                      client_cache_world_comm);

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - inter-node shuffle alltoall");
        tmp_timer2 = MPI_Wtime();

        // Unpack inter-node exchange
        if (total_inter_recv_items_chunk > 0) {
            for (int sender_wrank = 0; sender_wrank < client_info.world_size; sender_wrank++) {
                int   items_from_sender = inter_node_recv_counts[c][sender_wrank];
                char *base_recv_ptr =
                    temp_inter_recv_buf + ((size_t)rdispls_inter[sender_wrank] * INTER_TRANSFER_UNIT_SIZE);

                for (int j = 0; j < items_from_sender; ++j) {
                    pdc_object_data *obj_cache_item = (pdc_object_data *)PDC_malloc(sizeof(pdc_object_data));
                    char *           recv_ptr       = base_recv_ptr + (j * (size_t)INTER_TRANSFER_UNIT_SIZE);
                    size_t           current_offset = 0;

                    memcpy(&obj_cache_item->obj_id, recv_ptr + current_offset, sizeof(pdcid_t));
                    current_offset += sizeof(pdcid_t);
                    memcpy(&obj_cache_item->unit, recv_ptr + current_offset, sizeof(uint64_t));
                    current_offset += sizeof(uint64_t);
                    memcpy(&obj_cache_item->reg_ndim, recv_ptr + current_offset, sizeof(int));
                    current_offset += sizeof(int);
                    memcpy(obj_cache_item->reg_offset, recv_ptr + current_offset, sizeof(uint64_t) * 3);
                    current_offset += (sizeof(uint64_t) * 3);
                    memcpy(obj_cache_item->reg_size, recv_ptr + current_offset, sizeof(uint64_t) * 3);
                    current_offset += (sizeof(uint64_t) * 3);
                    memcpy(&obj_cache_item->reg_buf_size, recv_ptr + current_offset, sizeof(uint64_t));
                    current_offset += sizeof(uint64_t);

                    obj_cache_item->target_rank        = -1;
                    obj_cache_item->data_exchange_type = 0;

                    obj_cache_item->slot_idx = pop_free_slot();
                    // while (obj_cache_item->slot_idx == SLOT_INVALID) {
                    //     PDC_client_cache_evict();
                    //     obj_cache_item->slot_idx = pop_free_slot();
                    // }

                    if (obj_cache_item->slot_idx != SLOT_INVALID) {
                        char *data_ptr = get_data_ptr(obj_cache_item->slot_idx);

                        // memcpy(data_ptr, recv_ptr + current_offset, MAX_ITEM_SIZE);
                        memcpy(data_ptr, recv_ptr + current_offset,
                               obj_cache_item->reg_buf_size * sizeof(char));

                        PDC_client_cache_dl_prepend(obj_cache_item);
                    }
                    else {
                        free(obj_cache_item);
                    }
                }
            }
        }

        PDC_client_cache_timelog(tmp_timer2,
                                 "PDC_client_cache_dl_data_exchange - inter-node shuffle recv buffer unpack");
        tmp_timer2 = MPI_Wtime();

        MPI_Barrier(client_cache_world_comm);

        i = 0;
        while (i >= start_idx && i < end_idx && exchange_head != NULL) {
            obj_cache_iter = exchange_head;
            exchange_head  = obj_cache_iter->next;

            // Delete if the item was exchanged during inter-node shuffle only
            if (obj_cache_iter->data_exchange_type) {
                push_free_slot(obj_cache_iter->slot_idx);
            }

            PDC_client_cache_dl_delete(obj_cache_iter);
            free(obj_cache_iter);
            i++;
        }

        PDC_client_cache_timelog(tmp_timer2, "PDC_client_cache_dl_data_exchange - delete item");

        MPI_Barrier(client_cache_world_comm);
    }

    PDC_client_cache_timelog(tmp_timer, "PDC_client_cache_dl_data_exchange - Step 2");

    MPI_Barrier(client_cache_world_comm);

    // For debugging purpose
    // obj_cache_iter = client_info.local_cache_list_head;
    // while (obj_cache_iter != NULL) {
    //     if (client_info.world_rank == 0) {
    //         printf("[RANK %d] object_id %lld offset %lld, num_item %d\n", client_info.world_rank,
    //         obj_cache_iter->obj_id, obj_cache_iter->reg_offset[0], client_info.cached_item_num);
    //     }

    //     fflush(stdout);

    //     obj_cache_iter = obj_cache_iter->next;
    // }

    free(sdispls_intra);
    free(rdispls_intra);

    free(sdispls_inter);
    free(rdispls_inter);

    free(intra_node_send_buf);
    free(inter_node_send_buf);
    free(temp_intra_recv_buf);
    free(temp_inter_recv_buf);

    PDC_client_cache_timelog(start, "PDC_client_cache_dl_data_exchange - data exchange total time");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_client_cache_dl_update(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size,
                           void *buf)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    pdc_object_data *obj_cache_iter, *obj_cache_item;
    int              is_overlapped = 0;

    if (client_info.cached_item_num == 0)
        goto done;

    // Find overlapping regions from the head of the list
    obj_cache_iter = client_info.local_cache_list_head;

    while (obj_cache_iter != NULL) {
        obj_cache_item = obj_cache_iter;
        obj_cache_iter = obj_cache_iter->next;

        if (obj_cache_item->obj_id == obj_id) {
            // Compare offset and offset + size and see if there is an overlap
            is_overlapped =
                check_overlap(ndim, offset, size, obj_cache_item->reg_offset, obj_cache_item->reg_size);

            // If there is overlapping region remove item from list
            if (is_overlapped) {
                // Delete the overlapped object item
                ret_value = PDC_client_cache_dl_delete(obj_cache_item);

                push_free_slot(obj_cache_item->slot_idx);

                free(obj_cache_item);
            }
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_client_cache_dl_evict()
{
    FUNC_ENTER(NULL);

    perr_t           ret_value = SUCCEED;
    pdc_object_data *obj_cache_item;

    if (client_info.cached_item_num == 0)
        goto done;

    // Evict the last item following LRU policy
    obj_cache_item = client_info.local_cache_list_tail;

    ret_value = PDC_client_cache_dl_delete(obj_cache_item);

    push_free_slot(obj_cache_item->slot_idx);

    free(obj_cache_item);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_client_cache_dl_finalize()
{
    FUNC_ENTER(NULL);

    perr_t           ret_value = SUCCEED;
    pdc_object_data *obj_cache_iter, *obj_cache_item;
    double           start = MPI_Wtime();

    if (!client_info.client_cache_init)
        goto done;

    MPI_Barrier(client_cache_world_comm); // Make sure no shared memory is currently used

    free(client_info.world_to_node_rank_map);
    free(client_info.rank_to_node_id_map); // Free the rank_to_node_id_map

    obj_cache_iter = client_info.local_cache_list_tail;

    while (obj_cache_iter != NULL) {
        obj_cache_item = obj_cache_iter;
        obj_cache_iter = obj_cache_iter->prev;

        ret_value = PDC_client_cache_dl_delete(obj_cache_item);
        free(obj_cache_item);
    }

    MPI_Win_free(&client_info.node_shared_data_win);
    MPI_Comm_free(&client_cache_node_comm);
    MPI_Comm_free(&client_cache_world_comm);

    PDC_client_cache_timelog(start, "PDC_client_cache_dl_finalize - finalization time");

done:
    FUNC_LEAVE(ret_value);
}
