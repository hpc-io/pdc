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

#ifndef PDC_ANALYSIS_SUPPORT_H
#define PDC_ANALYSIS_SUPPORT_H

#include "pdc_public.h"

#define PDC_TIMING 1

#if PDC_TIMING == 1

#include <mpi.h>

typedef struct pdc_timing {
    double PDCbuf_obj_map_rpc;
    double PDCbuf_obj_unmap_rpc;
    double PDCreg_obtain_lock_rpc;
    double PDCreg_release_lock_rpc;
} pdc_timing;


pdc_timing *timings;

int PDC_timing_init() {
    timings = calloc(1, sizeof(pdc_timing));
}

int PDC_timing_report() {
    double max_time;
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Reduce(&(timings->PDCbuf_obj_map_rpc), &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        timings->PDCbuf_obj_map_rpc = max_time;
        printf("PDCbuf_obj_map_rpc = %lf\n", timings->PDCbuf_obj_map_rpc);
    }
    MPI_Reduce(&(timings->PDCreg_obtain_lock_rpc), &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        timings->PDCreg_obtain_lock_rpc = max_time;
        printf("PDCreg_obtain_lock_rpc = %lf\n", timings->PDCreg_obtain_lock_rpc);
    }
    MPI_Reduce(&(timings->PDCreg_release_lock_rpc), &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        timings->PDCreg_release_lock_rpc = max_time;
        printf("PDCreg_release_lock_rpc = %lf\n", timings->PDCreg_release_lock_rpc);
    }
    MPI_Reduce(&(timings->PDCbuf_obj_unmap_rpc), &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        timings->PDCbuf_obj_unmap_rpc = max_time;
        printf("PDCbuf_obj_unmap_rpc = %lf\n", timings->PDCbuf_obj_unmap_rpc);
    }

    free(timings);
}
#endif

/*********************/
/* Public Prototypes */
/*********************/
/**
 * Create a PDC iterator (basic form which internally calls the block version with contig_blocks = 1)
 *
 * \param obj_id [IN]           PDC object ID
 * \param reg_id [IN]           PDC region ID
 *
 * \return Iterator id on success/Zero on failure
 */
pdcid_t PDCobj_data_iter_create(pdcid_t obj_id, pdcid_t reg_id);

/**
 * Create a PDC block iterator (general form)
 *
 * \param obj_id [IN]           PDC object ID
 * \param reg_id [IN]           PDC region ID
 * \param contig_blocks [IN]    How many rows or columns the iterator should return
 *
 * \return Iterator id on success/Zero on failure
 */
pdcid_t PDCobj_data_block_iterator_create(pdcid_t obj_id, pdcid_t reg_id, int contig_blocks);

/**
 * *****
 *
 * \param iter [IN]             The number iteration
 *
 * \return ********
 */
size_t PDCobj_data_getSliceCount(pdcid_t iter);

/**
 * Use a PDC data iterator to retrieve object data for PDC in-locus Analysis
 *
 * \param iter [IN]             PDC iterator id
 * \param nextBlock [OUT]       Pointer to a contiguous block of memory that contains object data
 * \param dims [IN/OUT]         A pointer to an array (2D) of rows/columns 
 *
 * \return Total number of elements contained by the datablock.
 */
size_t PDCobj_data_getNextBlock(pdcid_t iter, void **nextBlock, size_t *dims);

/**
 * Register a function to be invoked at registration time and/or when data buffers
 * referenced by the input iterator is modified.
 *
 * \param func [IN]             String containing the [libraryname:]function to be registered.
 *                              (default library name = "libpdcanalysis")
 * \param iterIn [IN]           PDC iterator id containing the input data 
 *                              (may be a NULL iterator == 0).
 * \param dims [IN]             PDC iterator id containing the output data
 *                              (may be a NULL iterator == 0).
 *
 * \return Non-negative on success/Negative on failure
 * NOTES:
 * In the use case where null iterators are supplied, the function will always be
 * invoked on the data server.  Because there are no data references, the supplied function
 * will only be called at registration time.   This can be used to invoke special functions
 * such as the reading of data from files (HDF5, etc.) or to start monitoring or timing
 * functions. One could for example provide an API to start or stop profilers, take data 
 * snapshots, etc.
 */
perr_t PDCobj_analysis_register(char *func, pdcid_t iterIn, pdcid_t iterOut);

/**
 * ****
 *
 * \return ****
 */
int PDCiter_get_nextId(void);

#endif /* PDC_ANALYSIS_SUPPORT_H */
