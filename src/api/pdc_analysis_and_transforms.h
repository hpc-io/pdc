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

#include <dlfcn.h>
#include "mercury.h"
#include "mercury_proc_string.h"

#define PDC_REGION_ALL (pdcid_t)(-1)

// Analysis
extern size_t                     analysis_registry_size;
/* extern hg_atomic_int32_t          registered_ftn_count_g; */
extern struct PDC_iterator_info * PDC_Block_iterator_cache;
extern int                      * i_cache_freed;
extern size_t                     iterator_cache_entries;
/* extern hg_atomic_int32_t          i_cache_index; */
/* extern hg_atomic_int32_t          i_free_index; */
extern PDC_loci                   execution_locus;

extern hg_id_t                    analysis_ftn_register_id_g;
extern hg_id_t                    transform_ftn_register_id_g;
extern hg_id_t                    object_data_iterator_register_id_g;
/* extern hg_thread_mutex_t          insert_iterator_mutex_g; */

struct analysis_ftn_info {
    int (*ftnPtr)(void *, size_t *, size_t);
    pdcid_t object_id;
    PDC_Analysis_language lang;
    struct analysis_ftn_info *prev;
    struct analysis_ftn_info *next;
};

struct region_analysis_ftn_info {
    int n_args;
    pdcid_t *object_id;
    pdcid_t *region_id;
    int (*ftnPtr)();
    int readyCount;
    int client_id;
    int ftn_lastResult;
    PDC_Analysis_language lang;
    void *data;
//  PDC_LIST_ENTRY(region_analysis_ftn_info) entry;
};

struct PDC_iterator_info {
    void   *srcStart;                /* Constant that points to the data buffer  */
    void   *srcNext;                 /* Updated data pointer for each iteration  */
    void   *copy_region;             /* Normally unused (see special cases)      */
    size_t  startOffset;	     /* The first data pointer gets this offset  */
    size_t  skipCount;               /* Offset from the start of a new plane     */
    size_t  sliceCount;              /* Total # of slices to return              */
    size_t  sliceNext;               /* Count that we are going to return        */
    size_t  sliceResetCount;         /* For 3d, when to recalculate 'srcNext'    */
    size_t  elementsPerSlice;        /* Dimension 1 (elements/row) of the data   */
    size_t  slicePerBlock;           /* Dimension 2 (number of rows) in region   */
    size_t  elementsPerPlane;        /* elementsPerSlice * slicePerBlock         */
    size_t  elementsPerBlock;        /* elements in block (rows * cols)          */
    size_t  element_size;            /* Byte length of a single object element   */
    size_t  srcBlockCount;           /* Current count of 2d blocks               */
    size_t  contigBlockSize;         /* Number of elements in each slice (bytes) */
    size_t  totalElements;
    size_t  dims[2];                 /* [rows,columns]                           */
    size_t *srcDims;                 /* Values passed into create_iterator       */
    size_t  ndim;                    /* number of values in srcDims              */
    PDC_var_type_t pdc_datatype;     /* Copied from the object type              */
    PDC_major_type storage_order;    /* Copied from the object storage order     */
    pdcid_t objectId;                /* object ID Reference  */
    pdcid_t reg_id;                  /* region ID Reference  */
    pdcid_t meta_id;		     /* Server reference */
    pdcid_t local_id;		     /* Local reference */
};


typedef struct {
    hg_const_string_t           ftn_name;
    hg_const_string_t           loadpath;
    pdcid_t                     local_obj_id;
    pdcid_t                     iter_in;
    pdcid_t                     iter_out;
} analysis_ftn_in_t;

typedef struct {
    uint64_t                    remote_ftn_id;
} analysis_ftn_out_t;


typedef struct {
    pdcid_t                     client_iter_id;
    pdcid_t                     object_id;
    pdcid_t                     reg_id;
    uint64_t                    sliceCount;
    uint64_t                    sliceNext;
    uint64_t                    sliceResetCount;
    uint64_t                    elementsPerSlice;
    uint64_t                    slicePerBlock;
    uint64_t                    elementsPerPlane;
    uint64_t                    elementsPerBlock;
    uint64_t                    skipCount;
    uint64_t                    element_size;
    uint64_t                    srcBlockCount;
    uint64_t                    contigBlockSize;
    uint64_t                    totalElements;
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
    int                         storageinfo;
    int                         server_id;
} obj_data_iterator_in_t;

typedef struct {
    uint64_t                    server_iter_id;
    uint64_t                    client_iter_id;  
   /* 
    * If the container/region utilized by this iterator
    * doesn't exist on the server, then it provides a temporary
    * region with dimensions provided in the obj_data_iterator_in_t.
    * If not temporary is created, then the 'server_region_id' is
    * set to -1;
    */
    uint64_t                    server_region_id;
    int32_t                     ret;
} obj_data_iterator_out_t;

/* struct used to carry state of overall RPC operation across callbacks
 * Modified from Mercury: examples
 */
struct my_rpc_state
{
    hg_int64_t value;
    hg_size_t   size;
    void      * buffer;
    hg_bulk_t   bulk_handle;
    hg_handle_t handle;
};

extern int PDCiter_get_nextId(void);
extern int pdc_add_analysis_ptr_to_registry_(struct region_analysis_ftn_info *ftnPtr);
extern perr_t pdc_client_send_iter_recv_id(pdcid_t iter_id, pdcid_t *meta_id);
extern perr_t pdc_client_register_obj_analysis(const char *func, const char *loadpath, pdcid_t iter_in, pdcid_t iter_out);
extern perr_t pdc_client_register_obj_transform(const char *func, const char *loadpath, pdcid_t obj_id, int start_state, int next_state, int op_type, int when);
extern perr_t pdc_client_register_region_transform(const char *func, const char *loadpath, pdcid_t src_region_id, pdcid_t dest_region_id, int start_state, int next_state, int op_type, int when);
extern int get_ftnPtr_(char *ftn, char *loadpath, void **ftnPtr);
extern void set_execution_locus(PDC_loci locus_identifier);
extern PDC_loci get_execution_locus(void);

static HG_INLINE hg_return_t
hg_proc_analysis_ftn_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    analysis_ftn_in_t *struct_data = (analysis_ftn_in_t*) data;
    ret = hg_proc_hg_const_string_t(proc, &struct_data->ftn_name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->loadpath);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->local_obj_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->iter_in);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->iter_out);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_obj_data_iterator_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    obj_data_iterator_in_t *struct_data = (obj_data_iterator_in_t*) data;
    ret = hg_proc_uint64_t(proc, &struct_data->client_iter_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->object_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->reg_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->sliceCount);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->sliceNext);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->sliceResetCount);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->elementsPerSlice);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->slicePerBlock);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->elementsPerPlane);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->elementsPerBlock);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->skipCount);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->element_size);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->srcBlockCount);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->contigBlockSize);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->totalElements);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->storageinfo);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->server_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }

    return ret;
}

static HG_INLINE hg_return_t
hg_proc_analysis_ftn_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    analysis_ftn_out_t *struct_data = (analysis_ftn_out_t *) data;

    ret = hg_proc_uint64_t(proc, &struct_data->remote_ftn_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }

    return ret;
}

static HG_INLINE hg_return_t
hg_proc_obj_data_iterator_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    obj_data_iterator_out_t *struct_data = (obj_data_iterator_out_t *) data;

    ret = hg_proc_uint64_t(proc, &struct_data->server_iter_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->client_iter_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->server_region_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

#endif
