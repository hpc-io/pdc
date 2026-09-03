#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <mpi.h>

#include "pdc_utlist.h"
#include "pdc_config.h"
#include "pdc_id_pkg.h"
#include "pdc_obj.h"
#include "pdc_malloc.h"
#include "pdc_interface.h"
#include "pdc_region.h"
#include "pdc_region_pkg.h"
#include "pdc_client_cache.h"
#include "pdc_client_cache_prefetch.h"
#include "pdc_client_cache_dl.h"
#include "pdc_client_connect.h"

pdcid_t * obj_prefetch_list;
uint64_t *reg_offset_list;
uint64_t *reg_size_list;

// Global prefetch list
pdcid_t *global_obj_prefetch_list;
// int *     global_list_len;
uint64_t *global_offset_list;
uint64_t *global_size_list;

int reg_dim;
int obj_prefetch_list_len;

perr_t
PDC_client_cache_prefetch_init()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    obj_prefetch_list = NULL;
    reg_offset_list   = NULL;
    reg_size_list     = NULL;

    reg_dim               = 1;
    obj_prefetch_list_len = 0;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_print_prefetch_list()
{
    perr_t ret_value = SUCCEED;
    int    i;

    FUNC_ENTER(NULL);

    printf("[RANK %d] Prefetch list item number %d\n ", pdc_client_mpi_rank_g, obj_prefetch_list_len);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_receive_prefetch_hint(pdcid_t *obj_arr, pdcid_t *reg_arr, int obj_array_len)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

#ifdef ENABLE_CLIENT_CACHE
    struct _pdc_id_info *   objinfo2, *reginfo2;
    struct pdc_region_info *reg2;
    struct _pdc_obj_info *  obj2;
    uint64_t *              ptr, *ptr2;

    int    i;
    double start = MPI_Wtime();

    obj_prefetch_list_len = obj_array_len;
    obj_prefetch_list     = (pdcid_t *)PDC_malloc(obj_prefetch_list_len * sizeof(pdcid_t));
    if (!obj_prefetch_list)
        PGOTO_ERROR(FAIL, "PDCregion_receive_prefetch_hint - obj_prefetch_list memory allocation failed");

    // If reg_arr is null, it will read the whole region of the object
    // Else, we need to know the offset and size of the region
    if (reg_arr != NULL) {
        reginfo2 = PDC_find_id(reg_arr[0]);
        if (reginfo2 == NULL)
            PGOTO_ERROR(FAIL, "cannot locate remote region ID");

        reg2 = (struct pdc_region_info *)(reginfo2->obj_ptr);

        reg_dim = reg2->ndim;

        reg_offset_list = (uint64_t *)PDC_malloc(sizeof(uint64_t) * reg_dim * obj_prefetch_list_len);
        reg_size_list   = (uint64_t *)PDC_malloc(sizeof(uint64_t) * reg_dim * obj_prefetch_list_len);

        ptr  = reg_offset_list;
        ptr2 = reg_size_list;
    }

    // Convert received object id
    for (i = 0; i < obj_prefetch_list_len; i++) {
        objinfo2 = PDC_find_id(obj_arr[i]);
        if (objinfo2 == NULL)
            PGOTO_ERROR(FAIL, "cannot locate remote object ID");

        obj2                 = (struct _pdc_obj_info *)(objinfo2->obj_ptr);
        obj_prefetch_list[i] = obj2->obj_info_pub->meta_id;

        if (reg_arr != NULL) {
            reginfo2 = PDC_find_id(reg_arr[i]);
            if (reginfo2 == NULL)
                PGOTO_ERROR(FAIL, "cannot locate remote region ID");

            reg2 = (struct pdc_region_info *)(reginfo2->obj_ptr);
            memcpy(ptr, reg2->offset, sizeof(uint64_t) * reg_dim);
            ptr += reg_dim;

            memcpy(ptr2, reg2->size, sizeof(uint64_t) * reg_dim);
            ptr2 += reg_dim;
        }
    }

    PDC_client_cache_timelog(start, "PDCregion_receive_prefetch_hint - Total time");
#else
    if (pdc_client_mpi_rank_g == 0)
        printf("[RANK %d] Client cache disabled.\n", pdc_client_mpi_rank_g);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
pdc_region_prepare_global_prefetch_list()
{
    perr_t ret_value = SUCCEED;

    double start = MPI_Wtime();

    FUNC_ENTER(NULL);

    // Gather how many objects participate in data exchange for each client
    // TODO: Think about if gathering global list len or assuming list_len is equal among clients is okay
    // global_list_len = (int *)PDC_malloc(pdc_client_mpi_size_g * sizeof(int));
    // MPI_Allgather(&obj_prefetch_list_len, 1, MPI_INT, global_list_len, 1, MPI_INT, MPI_COMM_WORLD);

    global_obj_prefetch_list =
        (pdcid_t *)PDC_malloc(obj_prefetch_list_len * pdc_client_mpi_size_g * sizeof(pdcid_t));

    MPI_Allgather(obj_prefetch_list, obj_prefetch_list_len, MPI_UINT64_T, global_obj_prefetch_list,
                  obj_prefetch_list_len, MPI_UINT64_T, MPI_COMM_WORLD);

    if (reg_offset_list != NULL) {
        global_offset_list = (uint64_t *)PDC_malloc(reg_dim * pdc_client_mpi_size_g * sizeof(uint64_t));
        global_size_list   = (uint64_t *)PDC_malloc(reg_dim * pdc_client_mpi_size_g * sizeof(uint64_t));

        MPI_Allgather(reg_offset_list, reg_dim, MPI_UINT64_T, global_offset_list, reg_dim, MPI_UINT64_T,
                      MPI_COMM_WORLD);
        MPI_Allgather(reg_size_list, reg_dim, MPI_UINT64_T, global_size_list, reg_dim, MPI_UINT64_T,
                      MPI_COMM_WORLD);
    }
    else {
        global_offset_list = NULL;
        global_size_list   = NULL;
    }

    PDC_client_cache_timelog(start, "pdc_region_prepare_global_prefetch_list - Total time");

    // if (pdc_client_mpi_rank_g == 0) {
    //     printf("Rank %d received:\n", pdc_client_mpi_rank_g);
    //     for (int i = 0; i < obj_prefetch_list_len * pdc_client_mpi_size_g; i++) {
    //         printf("  [%d] %d\n", i, global_obj_prefetch_list[i]);
    //         fflush(stdout);
    //     }

    //     // printf("  From rank %d: list1 =", pdc_client_mpi_rank_g);
    //     // for (int i = 0; i < obj_prefetch_list_len; i++) {
    //     //     printf(" %" PRIu64, global_offset_list[pdc_client_mpi_rank_g * obj_prefetch_list_len + i]);
    //     //     fflush(stdout);
    //     // }
    //     // printf(" | list2 =");
    //     // for (int i = 0; i < obj_prefetch_list_len; i++) {
    //     //     printf(" %" PRIu64, global_size_list[pdc_client_mpi_rank_g * obj_prefetch_list_len + i]);
    //     //     fflush(stdout);
    //     // }
    //     printf("\n");
    // }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_prefetch_by_objid()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

#ifdef ENABLE_CLIENT_CACHE
    int    i, is_cached;
    double start = MPI_Wtime();

    if (obj_prefetch_list == NULL) {
        if (pdc_client_mpi_rank_g == 0)
            printf("[RANK %d] PDC_client_cache_prefetch_by_objid - object list not created\n",
                   pdc_client_mpi_rank_g);

        goto done;
    }

    ret_value = pdc_region_prepare_global_prefetch_list();
    ret_value = PDC_client_cache_dl_prepare_data_exchange(global_obj_prefetch_list, global_offset_list,
                                                          global_size_list, obj_prefetch_list_len);

    ret_value = PDC_client_cache_dl_data_exchange(global_obj_prefetch_list, obj_prefetch_list_len);

    free(global_obj_prefetch_list);
    free(global_offset_list);
    free(global_size_list);
    free(obj_prefetch_list);

    if (reg_offset_list != NULL) {
        free(reg_offset_list);
        free(reg_size_list);
        reg_offset_list = NULL;
    }

    obj_prefetch_list        = NULL;
    global_obj_prefetch_list = NULL;
    global_offset_list       = NULL;
    global_size_list         = NULL;
    reg_offset_list          = NULL;
    reg_size_list            = NULL;

    PDC_client_cache_timelog(start, "PDCregion_prefetch_by_objid - Total time");
#else
    if (pdc_client_mpi_rank_g == 0)
        printf("[RANK %d] Client cache disabled.\n", pdc_client_mpi_rank_g);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
