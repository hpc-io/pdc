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

#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <libgen.h>
#include <inttypes.h>

#include "mercury_atomic.h"

#include "pdc_config.h"
#include "../server/pdc_utlist.h"
#include "pdc_obj.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_obj_pkg.h"
#include "pdc_prop.h"
#include "pdc_region.h"
#include "pdc_analysis.h"
#include "pdc_analysis_pkg.h"
#include "pdc_analysis_and_transforms_common.h"
#include "pdc_client_server_common.h"

static char *default_pdc_analysis_lib = "libpdcanalysis.so";

#define UNUSED(x) (void)(x)

#ifndef IS_PDC_SERVER
void *
PDC_Server_get_region_data_ptr(pdcid_t object_id)
{
    UNUSED(object_id);
    return NULL;
}
#endif

static int
iterator_init(pdcid_t objectId, pdcid_t reg_id, int blocks, struct _pdc_iterator_info *iter)
{
    int                     ret_value = 0;
    int                     k, d_offset;
    size_t                  i, element_size = 0;
    char *                  data             = NULL;
    size_t                  sliceCount       = 1;
    size_t                  elementsPerSlice = 1;
    size_t                  elementsPerBlock;
    size_t                  obj_elementsPerSlice;
    size_t                  startOffset = 0;
    size_t                  skipCount   = 0;
    size_t                  totalElements;
    struct pdc_region_info *region_info = NULL;
    struct _pdc_obj_info *  object_info = PDC_obj_get_info(objectId);
    struct _pdc_obj_prop *  obj_prop_ptr;

    FUNC_ENTER(NULL);

    /* Gather Information about the underlying object, e.g.
     * the object data type, object size, etc. refers to the
     * containing array. These are for the most part, applicable
     * by the iterator for managing a potentially smaller region
     * (see region size info).
     */
    if ((obj_prop_ptr = object_info->obj_pt) != NULL) {
        if ((iter->storage_order = obj_prop_ptr->transform_prop.storage_order) == ROW_major) {
            obj_elementsPerSlice = obj_prop_ptr->obj_prop_pub->dims[obj_prop_ptr->obj_prop_pub->ndim - 1];
            if (obj_prop_ptr->obj_prop_pub->ndim > 2) {
                obj_elementsPerSlice *=
                    obj_prop_ptr->obj_prop_pub->dims[obj_prop_ptr->obj_prop_pub->ndim - 2];
            }
        }
        else {
            /* Is this ok for Fortran? */
            obj_elementsPerSlice = obj_prop_ptr->obj_prop_pub->dims[0];
            /*
            if (obj_prop_ptr->ndim > 1)
                obj_slicePerBlock = obj_prop_ptr->dims[1]; */
            if (obj_prop_ptr->obj_prop_pub->ndim > 2) {
                obj_elementsPerSlice *= obj_prop_ptr->obj_prop_pub->dims[1];
            }
        }
        iter->totalElements = 1;
        if ((iter->srcDims = (size_t *)calloc(obj_prop_ptr->obj_prop_pub->ndim, sizeof(size_t))) != NULL) {
            iter->ndim = obj_prop_ptr->obj_prop_pub->ndim;
            for (i = 0; i < iter->ndim; i++) {
                iter->srcDims[i] = (size_t)obj_prop_ptr->obj_prop_pub->dims[i];
                iter->totalElements *= iter->srcDims[i];
            }
        }

        /* Data TYPE and SIZE */
        iter->pdc_datatype = obj_prop_ptr->obj_prop_pub->type;

        if ((element_size = obj_prop_ptr->type_extent) == 0)
            element_size = PDC_get_var_type_size(obj_prop_ptr->obj_prop_pub->type);

        /* 'contigBlockSize' is the increment amount to move from
         * the current data pointer to the start of the next slice.
         */
        iter->contigBlockSize = obj_elementsPerSlice * element_size;
    }
    else
        PGOTO_ERROR(-1, "Error: object (%" PRIu64 ") has not been initalized correctly!", objectId);

    iter->element_size = element_size;
    iter->objectId     = objectId;
    iter->reg_id       = reg_id;

    if (reg_id == PDC_REGION_ALL) { /* Special handling:: We'll provide entire slices */
        iter->srcStart = data;      /* (rows,columns,planes) for each successive call */
        iter->srcNext  = data;      /* of pdc_getNextBlock.                           */

        iter->elementsPerSlice      = iter->slicePerBlock /* values are those signifying a single slice     */
            = iter->contigBlockSize = obj_elementsPerSlice;
        iter->contigBlockSize *= element_size; /* Increment value that is used to increment    */
                                               /* the data pointer to the next row...          */
        sliceCount            = iter->totalElements / obj_elementsPerSlice;
        iter->sliceCount      = sliceCount;
        iter->sliceResetCount = sliceCount + 1; /* Never */
    }
    else {
        /* The elementsPerSlice (or row length in bytes for this case), is used
         * to calculate how much to advance the current iterator data pointer
         * to point to the start of a new row, once we've found the start
         * of a particular block.  Note that for dimensions greater than 2,
         * there are TWO steps that take to get to the start of a new block, i.e.
         *   1.  Using the block number, we use the object dimensions to find
         *       the start of a block and then;
         *   2.  Add skipCount to point to the element within a row of the block.
         *
         * Subsequent advancement within a 2D block is accomplished
         * by adding the byte length of an object ROW, e.g.
         *
         *   +-----------------------+
         *   |  1  |  2  |  3  |  4  |    Example:  we want the interior region
         *   +-----============------+              6  7
         *   |  5  ||  6 |  7 ||  8  |             10 11
         *   +-----------------------+             14 15
         *   |  9  || 10 | 11 || 12  |
         *   +-----------------------+    Once the data pointer is pointing at '1'
         *   | 13  || 14 | 15 || 16  |    we add the skipCount of 5 x elementSize
         *   +-----============------+    and return the datapointer and a
         *   | 17  |  18 | 19  | 19  |    contigBlockSize of 2.
         *   +-----------------------+    We also add 5 x elementSize to srcNext
         *                                to now point at element '10'
         *
         */

        region_info = PDCregion_get_info(reg_id);

        /* How many rows in the selected region? */
        if (iter->storage_order == ROW_major) {
            iter->dims[0] = 1;
            sliceCount    = region_info->size[0];
            totalElements = sliceCount;
            for (i = 1; i < region_info->ndim; i++) {
                iter->dims[i] = region_info->size[i];
                elementsPerSlice *= region_info->size[i];
            }
            totalElements = elementsPerSlice * region_info->size[0];
        }
        else {
            k             = (int)iter->ndim - 1;
            iter->dims[k] = 1;
            sliceCount    = region_info->size[k];
            totalElements = sliceCount;
            for (; k >= 0; k--) {
                iter->dims[k] = region_info->size[k];
                elementsPerSlice *= region_info->size[k];
            }
            totalElements = elementsPerSlice * region_info->size[iter->ndim - 1];
        }

        if (totalElements == iter->totalElements) {
            iter->elementsPerBlock = elementsPerSlice;
        }
        /* For regions, the total element count is that of the region
         * not the size of the containing object...
         */
        iter->totalElements = totalElements;
        if (region_info->ndim > 1) {
            skipCount =
                ((region_info->offset[1] * obj_prop_ptr->obj_prop_pub->dims[0]) + region_info->offset[0]) *
                element_size;
            iter->sliceResetCount = iter->slicePerBlock = region_info->size[1] + 1;
            elementsPerBlock =
                obj_prop_ptr->obj_prop_pub->dims[0] * obj_prop_ptr->obj_prop_pub->dims[1] * element_size;

            if (region_info->ndim > 2) {
                d_offset = obj_prop_ptr->obj_prop_pub->dims[0] * obj_prop_ptr->obj_prop_pub->dims[1];
                iter->sliceResetCount = region_info->size[1];
                for (i = 2; i < region_info->ndim; i++) {
                    if (region_info->offset[i] > 0) {
                        startOffset = d_offset * region_info->offset[i];
                    }
                }
                startOffset *= element_size;
                iter->srcBlockCount = startOffset / elementsPerBlock;
            }
        }
        else {
            skipCount = region_info->offset[0] * element_size;
        }
        /* These next should be filled in at the first call
         * to PDC_getNextBlock().
         */
        iter->srcStart    = data;
        iter->srcNext     = data + startOffset + skipCount;
        iter->startOffset = startOffset;
        iter->sliceCount  = sliceCount;
        iter->skipCount   = skipCount;
    }
    if (blocks > 1) {
        iter->contigBlockSize *= blocks;
        iter->slicePerBlock *= blocks;
        iter->elementsPerBlock *= blocks;
        iter->sliceCount /= blocks;
        if (iter->sliceResetCount)
            iter->sliceResetCount = iter->sliceCount + 1;
        if (iter->storage_order == ROW_major)
            iter->dims[0] = blocks;
        else
            iter->dims[1] = blocks;
    }

    ret_value = 0;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCobj_data_block_iterator_create(pdcid_t obj_id, pdcid_t reg_id, int contig_blocks)
{
    pdcid_t                    ret_value = 0;
    pdcid_t                    iterId;
    struct _pdc_iterator_info *p = NULL;

    FUNC_ENTER(NULL);

    iterId = PDCiter_get_nextId();

    p = &PDC_Block_iterator_cache[iterId];
    if ((iterator_init(obj_id, reg_id, contig_blocks, p)) < 0)
        PGOTO_ERROR(0, "PDC iterator_init returned an error");

    ret_value = p->local_id = iterId;
    if (PDC_Client_send_iter_recv_id(iterId, &p->meta_id) != SUCCEED)
        PGOTO_ERROR(0, "Unable to register a new iterator");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * A wrapper to the more general "block_iterator_create" (see above)
 */
pdcid_t
PDCobj_data_iter_create(pdcid_t obj_id, pdcid_t reg_id)
{
    pdcid_t ret_value = 0;

    FUNC_ENTER(NULL);

    ret_value = PDCobj_data_block_iterator_create(obj_id, reg_id, 1);

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
PDC_get_argv0_()
{
    char *       ret_value          = NULL;
    static char *_argv0             = NULL;
    char         fullPath[PATH_MAX] = {
        0,
    };
    char currentDir[PATH_MAX] = {
        0,
    };
    pid_t  mypid = getpid();
    char * next;
    char * procpath = NULL;
    FILE * shellcmd = NULL;
    size_t cmdLength;

    FUNC_ENTER(NULL);

    if (_argv0 == NULL) {
        // UNIX derived systems e.g. linux, allow us to find the
        // command line as the user (or another application)
        // invoked us by reading the /proc/{pid}/cmdline
        // file.  Note that I'm assuming standard posix
        // file paths, so the directory seperator is a foward
        // slash ('/') and relative paths will include dot ('.')
        // dot-dot ("..").
        sprintf(fullPath, "/proc/%u/cmdline", mypid);
        procpath = strdup(fullPath);
        shellcmd = fopen(procpath, "r");
        if (shellcmd == NULL) {
            free(procpath);
            PGOTO_ERROR(NULL, "fopen failed!");
        }
        else {
            cmdLength = fread(fullPath, 1, sizeof(fullPath), shellcmd);
            if (procpath)
                free(procpath);
            if (cmdLength > 0) {
                _argv0 = strdup(fullPath);
                /* truncate the cmdline if any whitespace (space or tab) */
                if ((next = strchr(_argv0, ' ')) != NULL) {
                    *next = 0;
                }
                else if ((next = strchr(_argv0, '\t')) != NULL) {
                    *next = 0;
                }
            }
            fclose(shellcmd);
        }
        if (_argv0[0] != '/') {
            if (getcwd(currentDir, sizeof(currentDir)) == NULL) {
                PGOTO_ERROR(NULL, "Very long path name detected.");
            }
            next = PDC_find_in_path(currentDir, _argv0);
            if (next == NULL)
                PGOTO_ERROR(NULL, "WARNING: Unable to locate application (%s) in user $PATH", _argv0);
            else {
                /* Get rid of the copy (strdup) of fullPath now in _argv0.
                 * and replace it with the next (modified/fully_qualified?) version.
                 */
                free(_argv0);
                _argv0 = next;
            }
        }
    }
    if (_argv0 == NULL)
        PGOTO_ERROR(NULL, "ERROR: Unable to resolve user application name!");

    ret_value = _argv0;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

char *
PDC_get_realpath(char *fname, char *app_path)
{
    char *ret_value = NULL;
    int   notreadable;
    char  fullPath[PATH_MAX] = {
        0,
    };

    FUNC_ENTER(NULL);

    do {
        if (app_path) {
            sprintf(fullPath, "%s/%s", app_path, fname);
            app_path = NULL;
        }
        notreadable = access(fullPath, R_OK);
        if (notreadable && (app_path == NULL)) {
            perror("access");
            PGOTO_DONE(NULL);
        }
    } while (notreadable);

    ret_value = strdup(fullPath);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_analysis_register(char *func, pdcid_t iterIn, pdcid_t iterOut)
{
    perr_t ret_value                              = SUCCEED; /* Return value */
    void * ftnHandle                              = NULL;
    int (*ftnPtr)(pdcid_t, pdcid_t)               = NULL;
    struct _pdc_region_analysis_ftn_info *thisFtn = NULL;
    struct _pdc_iterator_info *           i_in = NULL, *i_out = NULL;
    pdcid_t                               meta_id_in = 0, meta_id_out = 0;
    pdcid_t                               local_id_in = 0, local_id_out = 0;
    char *                                thisApp        = NULL;
    char *                                colonsep       = NULL;
    char *                                analyislibrary = NULL;
    char *                                applicationDir = NULL;
    char *                                userdefinedftn = NULL;
    char *                                loadpath       = NULL;

    FUNC_ENTER(NULL);

    thisApp = PDC_get_argv0_();
    if (thisApp) {
        applicationDir = dirname(strdup(thisApp));
    }
    userdefinedftn = strdup(func);

    if ((colonsep = strrchr(userdefinedftn, ':')) != NULL) {
        *colonsep++    = 0;
        analyislibrary = colonsep;
    }
    else
        analyislibrary = default_pdc_analysis_lib;

    // TODO:
    // Should probably validate the location of the "analysislibrary"
    //
    loadpath = PDC_get_realpath(analyislibrary, applicationDir);
    if (PDC_get_ftnPtr_((const char *)userdefinedftn, (const char *)loadpath, &ftnHandle) < 0)
        PGOTO_ERROR(FAIL, "PDC_get_ftnPtr_ returned an error!");

    if ((ftnPtr = ftnHandle) == NULL)
        PGOTO_ERROR(FAIL, "Analysis function lookup failed");

    if ((thisFtn = PDC_MALLOC(struct _pdc_region_analysis_ftn_info)) == NULL)
        PGOTO_ERROR(FAIL, "PDC register_obj_analysis memory allocation failed");

    thisFtn->ftnPtr = (int (*)())ftnPtr;
    thisFtn->n_args = 2;
    /* Allocate for iterator ids and region ids */
    if ((thisFtn->object_id = (pdcid_t *)calloc(4, sizeof(pdcid_t))) != NULL) {
        thisFtn->object_id[0] = iterIn;
        thisFtn->object_id[1] = iterOut;
    }
    else
        PGOTO_ERROR(FAIL, "PDC register_obj_analysis memory allocation failed - object_ids");

    thisFtn->region_id = (pdcid_t *)&thisFtn->object_id[2];

    thisFtn->lang = C_lang;

    if (PDC_Block_iterator_cache) {
        if (iterIn != 0) {
            i_in        = &PDC_Block_iterator_cache[iterIn];
            meta_id_in  = i_in->meta_id;
            local_id_in = i_in->local_id;
        }
        if (iterOut != 0) {
            i_out        = &PDC_Block_iterator_cache[iterOut];
            meta_id_out  = i_out->meta_id;
            local_id_out = i_out->local_id;
        }
    }

    PDC_Client_register_obj_analysis(thisFtn, userdefinedftn, loadpath, local_id_in, local_id_out, meta_id_in,
                                     meta_id_out);

    // Add region IDs
    thisFtn->region_id[0] = i_in->reg_id;
    thisFtn->region_id[1] = i_out->reg_id;

    // Add to our own list of analysis functions
    if (PDC_add_analysis_ptr_to_registry_(thisFtn) < 0)
        PGOTO_ERROR(FAIL, "PDC unable to register analysis function!");

done:
    if (applicationDir)
        free(applicationDir);
    if (userdefinedftn)
        free(userdefinedftn);
    if (loadpath)
        free(loadpath);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

size_t
PDCobj_data_getSliceCount(pdcid_t iter)
{
    size_t                     ret_value = 0;
    struct _pdc_iterator_info *thisIter  = NULL;

    FUNC_ENTER(NULL);

    /* Special case to handle a NULL iterator */
    if (iter == 0)
        PGOTO_DONE(0);
    /* FIXME: Should add another check to see that the input
     *        iter id is in the range of cached values...
     */
    if ((PDC_Block_iterator_cache != NULL) && (iter > 0)) {
        thisIter = &PDC_Block_iterator_cache[iter];
        PGOTO_DONE(thisIter->sliceCount);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

size_t
PDCobj_data_getNextBlock(pdcid_t iter, void **nextBlock, size_t *dims)
{
    size_t                     ret_value = 0;
    struct _pdc_iterator_info *thisIter  = NULL;
    size_t                     current_total, remaining, offset;
    struct _pdc_obj_info *     object_info;
    struct _pdc_obj_prop *     obj_prop_ptr;

    FUNC_ENTER(NULL);

    /* Special case to handle a NULL iterator */
    if (iter == 0) {
        if (nextBlock != NULL)
            *nextBlock = NULL;
        if (dims != NULL)
            *dims = 0;
        PGOTO_DONE(0);
    }

    if ((PDC_Block_iterator_cache != NULL) && (iter > 0)) {
        thisIter = &PDC_Block_iterator_cache[iter];
        if (thisIter->srcStart == NULL) {
            if (execution_locus == SERVER_MEMORY) {
                if ((thisIter->srcNext = PDC_Server_get_region_data_ptr(thisIter->objectId)) == NULL)
                    thisIter->srcNext = malloc(thisIter->totalElements * thisIter->element_size);
                if ((thisIter->srcStart = thisIter->srcNext) == NULL)
                    PGOTO_ERROR(0, "==PDC_ANALYSIS_SERVER: Unable to allocate iterator storage");

                thisIter->srcNext += thisIter->startOffset + thisIter->skipCount;
            }
            else if (execution_locus == CLIENT_MEMORY) {
                object_info  = PDC_obj_get_info(thisIter->objectId);
                obj_prop_ptr = object_info->obj_pt;
                if (obj_prop_ptr) {
                    thisIter->srcNext = thisIter->srcStart = obj_prop_ptr->buf;
                    thisIter->srcNext += thisIter->startOffset + thisIter->skipCount;
                }
            }
        }
        if (thisIter->srcNext != NULL) {
            if (thisIter->sliceNext == thisIter->sliceCount) {
                /* May need to adjust the elements in this last
                 * block...
                 */
                current_total = thisIter->sliceCount * thisIter->elementsPerBlock;
                remaining     = 0;

                if (current_total == thisIter->totalElements) {
                    if (nextBlock)
                        *nextBlock = NULL;
                    thisIter->sliceNext = 0;
                    thisIter->srcNext   = NULL;
                    if (dims)
                        dims[0] = dims[1] = 0;
                    if (nextBlock)
                        *nextBlock = NULL;
                    PGOTO_DONE(0);
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
                PGOTO_DONE(remaining);
            }
            else if (thisIter->sliceNext && (thisIter->sliceNext % thisIter->sliceResetCount) == 0) {
                offset            = ++thisIter->srcBlockCount * thisIter->elementsPerBlock;
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
            PGOTO_DONE(thisIter->elementsPerBlock);
        }
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_analysis_end()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    PDC_free_analysis_registry();

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_iterator_end()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    PDC_free_iterator_cache();

    FUNC_LEAVE(ret_value);
}
