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

#include "mercury.h"
#include "mercury_macros.h"

// Mercury hash table and list
#include "mercury_hash_table.h"
#include "mercury_list.h"

#include "pdc_config.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc_utlist.h"
#include "pdc_dablooms.h"
#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server.h"
#include "pdc_analysis_pkg.h"
#include "pdc_analysis.h"

#ifdef PDC_HAS_CRAY_DRC
#include <rdmacred.h>
#endif

#define BLOOM_TYPE_T counting_bloom_t
#define BLOOM_NEW    new_counting_bloom
#define BLOOM_CHECK  counting_bloom_check
#define BLOOM_ADD    counting_bloom_add
#define BLOOM_REMOVE counting_bloom_remove
#define BLOOM_FREE   free_counting_bloom

#ifdef ENABLE_MULTITHREAD
hg_thread_mutex_t insert_iterator_mutex_g = HG_THREAD_MUTEX_INITIALIZER;
#endif

/*
 * Insert an iterator received from client into a collection
 *
 * \param  in[IN]       Input structure received from client
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t
PDC_Server_instantiate_data_iterator(obj_data_iterator_in_t *in, obj_data_iterator_out_t *out)
{
    perr_t                     ret_value        = SUCCEED;
    data_server_region_t *     region_reference = NULL;
    struct _pdc_iterator_info *thisIter;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&insert_iterator_mutex_g);
#endif
    int nextId = PDCiter_get_nextId();
    thisIter   = &PDC_Block_iterator_cache[nextId];
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&insert_iterator_mutex_g);
#endif
    thisIter->objectId         = in->object_id;
    thisIter->reg_id           = in->reg_id;
    thisIter->sliceCount       = in->sliceCount;
    thisIter->sliceNext        = in->sliceNext;
    thisIter->sliceResetCount  = in->sliceResetCount;
    thisIter->elementsPerSlice = in->elementsPerSlice;
    thisIter->slicePerBlock    = in->slicePerBlock;
    thisIter->elementsPerPlane = in->elementsPerPlane;
    thisIter->elementsPerBlock = in->elementsPerBlock;
    thisIter->skipCount        = in->skipCount;
    thisIter->element_size     = in->element_size;
    thisIter->srcBlockCount    = in->srcBlockCount;
    thisIter->contigBlockSize  = in->contigBlockSize;
    thisIter->totalElements    = in->totalElements;
    thisIter->pdc_datatype     = (pdc_var_type_t)(in->storageinfo & 0x0FF);
    thisIter->storage_order    = (_pdc_major_type_t)((in->storageinfo >> 8) & 0xFF);
    region_reference           = PDC_Server_get_obj_region(in->object_id);
    if (region_reference == NULL) {
        printf("==PDC_ANALYSIS_SERVER: Unable to locate object region (id=%" PRIu64 ")\n", in->object_id);
        /* The most likely cause of this condition is that the client never
         * created an object mapping which would move the client data to the data-server.
         * We now have the option to either fail, or to create a new temporary region.
         * With the latter, we can indicate in our return that we've created the
         * data-server temp so that the client can update the locus info.
         */
        out->server_region_id = -1;

#if 0
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_lock(&create_region_struct_mutex_g);
#endif

        new_obj_reg = (data_server_region_t *)malloc(sizeof(struct data_server_region_t));
        if(new_obj_reg == NULL) {
#ifdef ENABLE_MULTITHREAD 
            // Unlock before we bail out...
            hg_thread_mutex_unlock(&create_region_struct_mutex_g);
#endif
            PGOTO_ERROR(NULL, "PDC_Server_instantiate_data_iterator() allocation of new object region failed");
	}
        new_obj_reg->obj_id = in->object_id;
        new_obj_reg->region_lock_head = NULL;
        new_obj_reg->region_buf_map_head = NULL;
        new_obj_reg->region_lock_request_head = NULL;
        new_obj_reg->obj_data_ptr = malloc(thisIter->totalElements * thisIter->element_size);
        if (new_obj_reg->obj_data_ptr != NULL) {
            DL_APPEND(dataserver_region_g, new_obj_reg);
	}
	else {
#ifdef ENABLE_MULTITHREAD 
            // Unlock before we bail out...
            hg_thread_mutex_unlock(&create_region_struct_mutex_g);
#endif
            free(new_obj_reg);
            PGOTO_ERROR(NULL, "PDC_Server_instantiate_data_iterator() allocation of object data failed");
	}
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&create_region_struct_mutex_g);
#endif
	out->server_region_id = in->object_id;
        printf("==PDC_ANALYSIS_SERVER: Created a temp data container for id=%ld\n", in->object_id);
#endif /* #if 0 */
    }
    else {
        printf("==PDC_ANALYSIS_SERVER: Found object region for id=%" PRIu64 "\n", in->object_id);
        out->server_region_id = in->object_id;
    }

    out->client_iter_id = in->client_iter_id;
    out->server_iter_id = nextId;
    out->ret            = 0;

    FUNC_LEAVE(ret_value);
}

/*
 * Insert an iterator received from client into a collection
 *
 * \param  in[IN]       Input structure received from client
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */

void *
PDC_Server_get_region_data_ptr(pdcid_t object_id)
{
    data_server_region_t *region_reference = NULL;
    region_reference                       = PDC_Server_get_obj_region(object_id);
    if (region_reference) {
        /* See if this is a mapped object/region */
        if (region_reference->region_buf_map_head) {
            return region_reference->region_buf_map_head->remote_data_ptr;
        }
        else
            return region_reference->obj_data_ptr;
    }
    return NULL;
}

void *
PDC_Server_get_ftn_reference(char *ftn)
{
    static void *appHandle = NULL;
    void *       ftnHandle = NULL;
    if (appHandle == NULL) {
        /* We need the in_process address of the function */
        if ((appHandle = dlopen(0, RTLD_NOW)) == NULL) {
            char *this_error = dlerror();
            fprintf(stderr, "dlopen failed: %s\n", this_error);
            fflush(stderr);
            return NULL;
        }
    }
    ftnHandle = dlsym(appHandle, ftn);
    return ftnHandle;
}

size_t
PDCobj_data_getSliceCount(pdcid_t iter)
{
    struct _pdc_iterator_info *thisIter = NULL;
    /* Special case to handle a NULL iterator */
    if (iter == 0)
        return 0;
    /* FIXME: Should add another check to see that the input
     *        iter id is in the range of cached values...
     */
    if ((PDC_Block_iterator_cache != NULL) && (iter > 0)) {
        thisIter = &PDC_Block_iterator_cache[iter];
        return thisIter->sliceCount;
    }
    return 0;
}

size_t
PDCobj_data_getNextBlock(pdcid_t iter, void **nextBlock, size_t *dims)
{
    struct _pdc_iterator_info *thisIter = NULL;
    /* Special case to handle a NULL iterator */
    if (iter == 0) {
        if (nextBlock != NULL)
            *nextBlock = NULL;
        if (dims != NULL)
            *dims = 0;
        return 0;
    }

    if ((PDC_Block_iterator_cache != NULL) && (iter > 0)) {
        thisIter = &PDC_Block_iterator_cache[iter];
        if (thisIter->srcStart == NULL) {
            if (execution_locus == SERVER_MEMORY) {
                if ((thisIter->srcNext = PDC_Server_get_region_data_ptr(thisIter->objectId)) == NULL)
                    thisIter->srcNext = malloc(thisIter->totalElements * thisIter->element_size);
                if ((thisIter->srcStart = thisIter->srcNext) == NULL) {
                    printf("==PDC_ANALYSIS_SERVER: Unable to allocate iterator storage\n");
                    return 0;
                }
                thisIter->srcNext += thisIter->startOffset + thisIter->skipCount;
            }
        }
        if (thisIter->srcNext != NULL) {
            if (thisIter->sliceNext == thisIter->sliceCount) {
                /* May need to adjust the elements in this last
                 * block...
                 */
                size_t current_total = thisIter->sliceCount * thisIter->elementsPerBlock;
                size_t remaining     = 0;

                if (current_total == thisIter->totalElements) {
                    if (nextBlock)
                        *nextBlock = NULL;
                    thisIter->sliceNext = 0;
                    thisIter->srcNext   = NULL;
                    goto done;
                }
                if (nextBlock)
                    *nextBlock = thisIter->srcNext;
                thisIter->srcNext = NULL;
                remaining         = thisIter->totalElements - current_total;
                if (dims) {
                    if (thisIter->storage_order == ROW_major)
                        dims[0] = remaining / thisIter->elementsPerSlice;
                    else
                        dims[1] = remaining / thisIter->elementsPerSlice;
                }
                return remaining;
            }
            else if (thisIter->sliceNext && (thisIter->sliceNext % thisIter->sliceResetCount) == 0) {
                size_t offset     = ++thisIter->srcBlockCount * thisIter->elementsPerBlock;
                thisIter->srcNext = thisIter->srcStart + offset + thisIter->skipCount;
                if (nextBlock)
                    *nextBlock = thisIter->srcNext;
            }
            else {
                *nextBlock = thisIter->srcNext;
                thisIter->srcNext += thisIter->contigBlockSize;
            }
            thisIter->sliceNext += 1;
            if (dims != NULL) {
                dims[0] = thisIter->dims[0];
                if (thisIter->ndim > 1)
                    dims[1] = thisIter->dims[1];
                if (thisIter->ndim > 2)
                    dims[2] = thisIter->dims[2];
                if (thisIter->ndim > 3)
                    dims[2] = thisIter->dims[3];
            }
            return thisIter->elementsPerBlock;
        }
    }

done:
    if (dims)
        dims[0] = dims[1] = 0;
    if (nextBlock)
        *nextBlock = NULL;
    return 0;
}

int
PDC_get_analysis_registry(struct _pdc_region_analysis_ftn_info ***registry)
{
    if (registry) {
        *registry = pdc_region_analysis_registry;
        return hg_atomic_get32(&registered_analysis_ftn_count_g);
    }
    return 0;
}
