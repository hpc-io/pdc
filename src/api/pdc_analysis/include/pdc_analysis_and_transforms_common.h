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
#ifndef PDC_OBJ_ANALYSIS_H
#define PDC_OBJ_ANALYSIS_H

#include "pdc_private.h"
#include "mercury_proc_string.h"
#include "mercury_atomic.h"
#include <dlfcn.h>

#define PDC_REGION_ALL (pdcid_t)(-1)
#define PDC_ITER_NULL  (pdcid_t)(0)

extern int                      pdc_server_num_g;
extern int                      pdc_client_mpi_rank_g;
extern struct _pdc_server_info *pdc_server_info_g;

struct _pdc_region_analysis_ftn_info {
    int      meta_index;
    int      n_args;
    pdcid_t *object_id;
    pdcid_t *region_id;
    int (*ftnPtr)();
    int                      readyCount;
    int                      client_id;
    int                      ftn_lastResult;
    _pdc_analysis_language_t lang;
    void *                   data;
};

/*
 *  The basic idea for introducing an iterator data structure is that
 *  this approach allows "lazy" processing for accessing sub-regions.
 *  The tradeoff is that by introducing temp arrays to either capture
 *  an input region or an output region one allows user code
 *  to have illusion of continguous storage.  The price to paid however,
 *  is that for input data the object content needs to be copied from the
 *  base data container into the temp storage.  On output, the problem
 *  is somewhat exasperated by the fact that post execution (which may
 *  be asynchronous), the generated output data needs to be copied back
 *  into the actual object storage.
 *
 *  Design of these simple iterators is straight forward; we maintain
 *  the iterator 'state' within the interator_info struct (shown
 *  below), which we index using the pdcid_t that was returned when the
 *  iterator was initialized.  There's enough info contained within to
 *  structure allow a reset operation if required. Upon free'ing the struct,
 *  we enter the free index into a second cache which we access in a
 *  LIFO manner.  Eventually, if a free index cannot be found, we
 *  reallocate a larger iterator cache, copy the state contained in the old,
 *  to the new and then fill the next new entry and continue.
 *
 *  Special cases:
 *  We've added "block" iterators which are a convenient mechanism for
 *  thread parallelism. The basic idea is that we can farm out some number of
 *  data blocks to new threads for analysis. This provides a basic control
 *  mechanism to avoid creating and possibly destroying threads for each
 *  row or column.
 *  Expanding on the above capability, when users specify block iterators
 *  in conjunction with subregions, then we alloc a temporary data buffer
 *  which is subsequently filled from the specified subregion.
 *  CAUTION: the copy_region needs to be managed on a per-thread basis!
 */

struct _pdc_iterator_info {
    void *  srcStart;         /**** Constant that points to the data buffer  */
    void *  srcNext;          /**** Updated data pointer for each iteration  */
    void *  copy_region;      /* Normally unused (see special cases)         */
    size_t  sliceCount;       /**** Total # of slices to return              */
    size_t  sliceNext;        /* Current count that we are going to return   */
    size_t  sliceResetCount;  /* For 3d, when to recalculate 'srcNext'       */
    size_t  elementsPerSlice; /* # of elements in a slice the data           */
    size_t  slicePerBlock;    /* # of slices to be returned to the user      */
    size_t  elementsPerPlane; /* rows * columns                              */
    size_t  elementsPerBlock; /* Total elements in a block (return value?) */
    size_t  skipCount;        /* Offset from the start of a new plane  (Used to initialize srcNext) */
    size_t  element_size;     /* Byte length of a single object element      */
    size_t  srcBlockCount;    /* Current count of 2d blocks                  */
    size_t  contigBlockSize;  /* Number of elements in each slice (bytes) (Used to move to the next Block) */
    size_t  totalElements;
    size_t  dims[4];     /* [ planes, rows,columns]                     */
    size_t *srcDims;     /* Values passed into create_iterator          */
    size_t  ndim;        /* number of values in srcDims                 */
    size_t  startOffset; /* Used to initialize the srcNext field      */
    pdc_var_type_t    pdc_datatype;
    _pdc_major_type_t storage_order; /* Copied from the object storage order        */
    pdcid_t           objectId;      /* Reference object ID                         */
    pdcid_t           reg_id;        /* Reference region ID                         */
    pdcid_t           local_id;      /* Our local reference id                      */
    pdcid_t           meta_id;       /* The server registration id                  */
};

/* struct used to carry state of overall RPC operation across callbacks
 * Modified from Mercury: examples
 */
struct _pdc_my_rpc_state {
    hg_int64_t  value;
    hg_size_t   size;
    void *      buffer;
    hg_bulk_t   bulk_handle;
    hg_handle_t handle;
};

struct _pdc_iterator_cbs_t {
    size_t (*getSliceCount)(pdcid_t);
    size_t (*getNextBlock)(pdcid_t, void **, size_t *);
};

// Analysis
#define CACHE_SIZE 8192
extern struct _pdc_iterator_info *PDC_Block_iterator_cache;
extern _pdc_loci_t                execution_locus;

typedef struct analysis_ftn_in_t {
    hg_const_string_t ftn_name;
    hg_const_string_t loadpath;
    pdcid_t           local_obj_id;
    pdcid_t           iter_in;
    pdcid_t           iter_out;
} analysis_ftn_in_t;

typedef struct analysis_ftn_out_t {
    uint64_t remote_ftn_id;
} analysis_ftn_out_t;

typedef struct obj_data_iterator_in_t {
    pdcid_t  client_iter_id;
    pdcid_t  object_id;
    pdcid_t  reg_id;
    uint64_t sliceCount;
    uint64_t sliceNext;
    uint64_t sliceResetCount;
    uint64_t elementsPerSlice;
    uint64_t slicePerBlock;
    uint64_t elementsPerPlane;
    uint64_t elementsPerBlock;
    uint64_t skipCount;
    uint64_t element_size;
    uint64_t srcBlockCount;
    uint64_t contigBlockSize;
    uint64_t totalElements;
    uint64_t ndim;
    uint64_t dims_0;
    uint64_t dims_1;
    uint64_t dims_2;
    uint64_t dims_3;
    /*
     * The datatype isn't strictly needed but it can be nice
     * to have if we eventually provide a default 'fill value'.
     * This would be used when the server creates a temp in
     * place of a mapped region.
     * Generally, we assume that either a region is mapped
     * we haven't mapped because the object is an output
     * and we really don't care what the initial values are.
     * Note we package storage_order and pdc_datatype
     * into storage_info...
     * +--------+---------------+---------------+
     * |XXXXXXX | storage-order | pdc_datatype  |
     * +---//---+---------------+---------------+
     * 31     16 15            8 7             0
     */
    int storageinfo;
    int server_id;
} obj_data_iterator_in_t;

typedef struct obj_data_iterator_out_t {
    uint64_t server_iter_id;
    uint64_t client_iter_id;
    /*
     * If the container/region utilized by this iterator
     * doesn't exist on the server, then it provides a temporary
     * region with dimensions provided in the obj_data_iterator_in_t.
     * If not temporary is created, then the 'server_region_id' is
     * set to -1;
     */
    uint64_t server_region_id;
    int32_t  ret;
} obj_data_iterator_out_t;

static HG_INLINE hg_return_t
hg_proc_analysis_ftn_in_t(hg_proc_t proc, void *data)
{
    hg_return_t        ret;
    analysis_ftn_in_t *struct_data = (analysis_ftn_in_t *)data;
    ret                            = hg_proc_hg_const_string_t(proc, &struct_data->ftn_name);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->loadpath);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->local_obj_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->iter_in);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->iter_out);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_obj_data_iterator_in_t(hg_proc_t proc, void *data)
{
    hg_return_t             ret;
    obj_data_iterator_in_t *struct_data = (obj_data_iterator_in_t *)data;
    ret                                 = hg_proc_uint64_t(proc, &struct_data->client_iter_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->object_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->reg_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->sliceCount);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->sliceNext);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->sliceResetCount);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->elementsPerSlice);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->slicePerBlock);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->elementsPerPlane);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->elementsPerBlock);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->skipCount);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->element_size);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->srcBlockCount);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->contigBlockSize);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->totalElements);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->ndim);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims_0);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims_1);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims_2);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims_3);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->storageinfo);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->server_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_analysis_ftn_out_t(hg_proc_t proc, void *data)
{
    hg_return_t         ret;
    analysis_ftn_out_t *struct_data = (analysis_ftn_out_t *)data;

    ret = hg_proc_uint64_t(proc, &struct_data->remote_ftn_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_obj_data_iterator_out_t(hg_proc_t proc, void *data)
{
    hg_return_t              ret;
    obj_data_iterator_out_t *struct_data = (obj_data_iterator_out_t *)data;

    ret = hg_proc_uint64_t(proc, &struct_data->server_iter_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->client_iter_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->server_region_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * Registers an iterator
 *
 * \param iter_id [IN]          The ID of iteration
 * \param meta_id [OUT]         Metadata ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_iter_recv_id(pdcid_t iter_id, pdcid_t *meta_id);

/**
 * *******
 *
 * \param iterthisFtn_id [IN]   *****
 * \param func [IN]             *****
 * \param loadpath [IN]         *****
 * \param in_local [IN]         *****
 * \param out_local [OUT]       *****
 * \param in_meta [IN]          *****
 * \param out_meta [OUT]        *****
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_register_obj_analysis(struct _pdc_region_analysis_ftn_info *thisFtn, const char *func,
                                        const char *loadpath, pdcid_t in_local, pdcid_t out_local,
                                        pdcid_t in_meta, pdcid_t out_meta);

/**
 * ******
 *
 * \param func [IN]             Name of the function
 * \param loadpath [IN]         Name of the path
 * \param src_region_id [IN]    ID of source region
 * \param dest_region_id [IN]   ID of destination region
 * \param obj_id [IN]           ID of the object
 * \param start_state [IN]      Start start
 * \param next_state [IN]       Next state
 * \param op_type [IN]          Operation type
 * \param when [IN]             When to start transformation
 * \param client_index [IN]     Client index
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_register_region_transform(const char *func, const char *loadpath,
                                            pdcid_t src_region_id ATTRIBUTE(unused), pdcid_t dest_region_id,
                                            pdcid_t obj_id, int start_state, int next_state, int op_type,
                                            int when, int client_index);

#endif /* PDC_OBJ_ANALYSIS_H */
