#include <time.h>
#include <stdlib.h>
#include <unistd.h>
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

pdcid_t pdc_id;

// Initialization of global variables
perr_t
PDC_client_cache_init(pdcid_t pdcid)
{
    perr_t ret_value = SUCCEED;
    double start;

    FUNC_ENTER(NULL);

    start = MPI_Wtime();

    pdc_id = pdcid;

    ret_value = PDC_client_cache_dl_init();
    PDC_client_cache_timelog(start, "PDC_client_cache_dl_init - total time");

    ret_value = PDC_client_cache_prefetch_init();
    PDC_client_cache_timelog(start, "PDC_client_cache_prefetch_init - total time");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// TODO:
// Implement when the region is overlapping - partially containing
// Need to manage the object's offset cache information
// Currently considering fully contained case
int
PDC_client_cache_search(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size, void *buf)
{
    int      region_contained = 0;
    uint64_t read_size        = 0;
    double   start;

    start = MPI_Wtime();

    // Calculation of read size
    // if (ndim >= 1)
    //     read_size = unit * size[0];
    // if (ndim >= 2)
    //     read_size *= size[1];
    // if (ndim >= 3)
    //     read_size *= size[2];

    read_size = unit;
    for (int i = 0; i < ndim; ++i) {
        read_size *= size[i];
    }

    // Search on doubly linked list
    region_contained = PDC_client_cache_dl_local_search(obj_id, ndim, unit, offset, size, buf, read_size);

    // if (!region_contained)
    //    region_contained = PDC_client_cache_dl_node_search(obj_id, ndim, unit, offset, size, buf,
    //    read_size);

    // printf("[RANK %d] PDC_client_cache_search: region contained: %d\n", pdc_client_mpi_rank_g,
    // region_contained);

    PDC_client_cache_timelog(start, "PDC_client_cache_search - total time");

    return region_contained;
}

// Insert the region to the list
perr_t
PDC_client_cache_insert(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size, void *buf)
{
    perr_t ret_value = SUCCEED;

    uint64_t read_size = 0;
    int      by_size;
    double   start;

    FUNC_ENTER(NULL);

    start = MPI_Wtime();

    // Calculation of read size
    if (ndim >= 1)
        read_size = unit * size[0];
    if (ndim >= 2)
        read_size *= size[1];
    if (ndim >= 3)
        read_size *= size[2];

    ret_value = PDC_client_cache_dl_insert(obj_id, ndim, unit, offset, size, buf, read_size);

    // printf(
    //     "[RANK %d] PDC_client_cache_insert: pid=%d, var_a=%d, &var_a=%p,
    //     PDCregion_print_prefetch_list=%p\n", pdc_client_mpi_rank_g, getpid(), obj_prefetch_list_len, (void
    //     *)&obj_prefetch_list_len, (void *)PDCregion_print_prefetch_list);

    PDC_client_cache_timelog(start, "PDC_client_cache_insert - total time");
    // printf("[RANK %d] PDC_client_cache_insert - total size: %zu bytes, total item num: %d \n",
    // pdc_client_mpi_rank_g, total_buf_size, total_item_num); fflush(stdout);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Check if there are overlapping parts when PDC_WRITE transfer_request is executed
// If there is evict that item since it is out of date
perr_t
PDC_client_cache_update(pdcid_t obj_id, int ndim, uint64_t unit, uint64_t *offset, uint64_t *size, void *buf)
{
    perr_t ret_value = SUCCEED;
    double start     = MPI_Wtime();

    FUNC_ENTER(NULL);

    ret_value = PDC_client_cache_dl_update(obj_id, ndim, unit, offset, size, buf);

    PDC_client_cache_timelog(start, "PDC_client_cache_update - update time");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Evict the region cache and object cache according to LRU policy
perr_t
PDC_client_cache_evict()
{
    perr_t ret_value = SUCCEED;

    double start;

    start = MPI_Wtime();

    FUNC_ENTER(NULL);

    ret_value = PDC_client_cache_dl_evict();

    PDC_client_cache_timelog(start, "PDC_client_cache_evict - PDC_client_cache_dl_evict time");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_client_cache_finalize()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_client_cache_dl_finalize();

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void
PDC_client_cache_timelog(double start_time, const char *message)
{
    int        rank_limit = 5;
    double     end_time;
    time_t     cur_time = time(NULL);
    struct tm *log_time = localtime(&cur_time);

    end_time = MPI_Wtime();
    if (pdc_client_mpi_rank_g < rank_limit) {
        cur_time = time(NULL);
        log_time = localtime(&cur_time);
        printf("[CACHE_LOG] [%02d:%02d:%02d] [RANK %d] [TOTAL_RANK %d] | %s : %f\n", log_time->tm_hour,
               log_time->tm_min, log_time->tm_sec, pdc_client_mpi_rank_g, pdc_client_mpi_size_g, message,
               end_time - start_time);
        fflush(stdout);
    }
}
