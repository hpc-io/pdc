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

/************************************************************************
 * This file includes the functionality to support PDC in-locus analysis
 ************************************************************************ */

#include "pdc_analysis_and_transforms.h"
#include "../server/pdc_analysis_server.h"

size_t                     analysis_registry_size = 0;
size_t                     transform_registry_size = 0;
hg_atomic_int32_t          registered_analysis_ftn_count_g;
hg_atomic_int32_t          registered_transform_ftn_count_g;
struct PDC_iterator_info * PDC_Block_iterator_cache = NULL;
int                      * i_cache_freed = NULL;

size_t                     iterator_cache_entries = 256;
hg_atomic_int32_t          i_cache_index;
hg_atomic_int32_t          i_free_index;
PDC_loci                   execution_locus = UNKNOWN;

extern hg_thread_mutex_t          insert_iterator_mutex_g;

static inline int compare_gt(int *a, int b) { return (*a) > (b); }
struct region_analysis_ftn_info **pdc_region_analysis_registry = NULL; 
struct region_transform_ftn_info **pdc_region_transform_registry = NULL; 

#ifndef IS_PDC_SERVER
// Dummy function for client to compile, real function is used only by server and code is in pdc_server.c
perr_t PDC_Server_instantiate_data_iterator(obj_data_iterator_in_t *in, obj_data_iterator_out_t *out) {return SUCCEED;}
#endif

/* Internal support functions */
static int pdc_analysis_registry_init_(size_t newSize)
{
    struct region_analysis_ftn_info **new_registry;
    if (pdc_region_analysis_registry == NULL) {
	new_registry = (struct region_analysis_ftn_info **)calloc(sizeof(void *),newSize);
        if (new_registry) {
            hg_atomic_init32(&registered_analysis_ftn_count_g, 0);
            pdc_region_analysis_registry = new_registry;
            return analysis_registry_size = newSize;
	}
    }
    else if (newSize > analysis_registry_size) {
	new_registry = (struct region_analysis_ftn_info **)calloc(sizeof(void *),newSize);
        if (new_registry) {
            size_t copysize = analysis_registry_size * sizeof(void *);
            memcpy(new_registry, pdc_region_analysis_registry, copysize);
            free(pdc_region_analysis_registry);
            pdc_region_analysis_registry = new_registry;
            return analysis_registry_size = newSize;
        }
    }
    return 0;
}

static int pdc_transform_registry_init_(size_t newSize)
{
    struct region_transform_ftn_info **new_registry;
    if (pdc_region_transform_registry == NULL) {
	new_registry = (struct region_transform_ftn_info **)calloc(sizeof(void *),newSize);
        if (new_registry) {
            hg_atomic_init32(&registered_transform_ftn_count_g, 0);
            pdc_region_transform_registry = new_registry;
            return transform_registry_size = newSize;
	}
    }
    else if (newSize > transform_registry_size) {
	new_registry = (struct region_transform_ftn_info **)calloc(sizeof(void *),newSize);
        if (new_registry) {
            size_t copysize = transform_registry_size * sizeof(void *);
            memcpy(new_registry, pdc_region_transform_registry, copysize);
            free(pdc_region_transform_registry);
            pdc_region_transform_registry = new_registry;
            return transform_registry_size = newSize;
        }
    }
    return 0;
}

int pdc_analysis_registry_finalize_()
{
    if ((pdc_region_analysis_registry != NULL) &&
        (analysis_registry_size > 0)) {
        int i;
        for(i=0; compare_gt((void *)&registered_analysis_ftn_count_g, i); i++) {
            if (pdc_region_analysis_registry[i]) free(pdc_region_analysis_registry[i]);
            pdc_region_analysis_registry[i] = NULL;
        }
        free(pdc_region_analysis_registry);
        analysis_registry_size = 0;
        hg_atomic_init32(&registered_analysis_ftn_count_g,0);
    }
    return 0;
}

int pdc_add_analysis_ptr_to_registry_(struct region_analysis_ftn_info *ftnPtr)
{
    size_t initial_registry_size = 64;
    size_t currentCount = (size_t) hg_atomic_get32(&registered_analysis_ftn_count_g);
    int registry_index;
    if (analysis_registry_size == 0) {
        if (pdc_analysis_registry_init_(initial_registry_size) == 0) {
            perror("Unable to initialize analysis registry!");
            return -1;
        }
    }
    if (currentCount == analysis_registry_size) {
        if (pdc_analysis_registry_init_(analysis_registry_size * 2) == 0) {
            perror("memory allocation failed");
            return -1;
        }
    }

    registry_index = hg_atomic_get32(&registered_analysis_ftn_count_g);
    hg_atomic_incr32(&registered_analysis_ftn_count_g);
    pdc_region_analysis_registry[registry_index] = ftnPtr;

    return registry_index;
}

int PDCiter_get_nextId(void)
{
    size_t nextId = 0;
    if (PDC_Block_iterator_cache == NULL) {
        PDC_Block_iterator_cache = (struct PDC_iterator_info *)calloc(iterator_cache_entries, sizeof(struct PDC_iterator_info));
        i_cache_freed = (int *) calloc(iterator_cache_entries, sizeof(int));
        /* Index 0 is NOT-USED other than to indicate an empty iterator */
        hg_atomic_init32(&i_cache_index,1);
        hg_atomic_init32(&i_free_index,0);
    }
    if (compare_gt((void *)&i_free_index, 0)) {
        int next_free = hg_atomic_decr32(&i_free_index);
        nextId = i_cache_freed[next_free];
    }
    else {
        int next_free = hg_atomic_incr32(&i_cache_index);
        nextId = next_free -1;        /* use the "current" index */
    }
    if (nextId == iterator_cache_entries) {
        /* Realloc the cache and free list */
        int *previous_i_cache_freed = i_cache_freed;
        struct PDC_iterator_info * previous_state = PDC_Block_iterator_cache;
        PDC_Block_iterator_cache = (struct PDC_iterator_info *)calloc(iterator_cache_entries *2, sizeof(struct PDC_iterator_info));
        memcpy(PDC_Block_iterator_cache, previous_state, iterator_cache_entries * sizeof(struct PDC_iterator_info));
        i_cache_freed = (int *)calloc(iterator_cache_entries * 2, sizeof(int));
        memcpy(i_cache_freed, previous_i_cache_freed, iterator_cache_entries * sizeof(int));
        iterator_cache_entries *= 2;
        free( previous_i_cache_freed );
        free( previous_state );
    }
    return nextId;
}  

/* 
 * Analysis and Transform
 */
int check_transform(PDCobj_transform_t op_type, struct PDC_region_info *dest_region)
{
    if (transform_registry_size > 0) {
       int i, count = hg_atomic_get32(&registered_transform_ftn_count_g);
       for(i=0; i<count; i++) {
           if ((pdc_region_transform_registry[i]->op_type == op_type) &&
	       (pdc_region_transform_registry[i]->dest_region == dest_region)) {
               dest_region->registered_transform = 1;
	       return 1;
           }
       }
    }
    dest_region->registered_transform = 0;
    return 0;
}

int pdc_get_transforms(struct region_transform_ftn_info ***registry)
{
    if(registry) {
       *registry = pdc_region_transform_registry;
       return hg_atomic_get32(&registered_transform_ftn_count_g);
    }
    return 0;
}

int pdc_add_transform_ptr_to_registry_(struct region_transform_ftn_info *ftnPtr)
{
    size_t initial_registry_size = 64;
    size_t currentCount = (size_t) hg_atomic_get32(&registered_transform_ftn_count_g);
    int registry_index;
    if (transform_registry_size == 0) {
        if (pdc_transform_registry_init_(initial_registry_size) == 0) {
            perror("Unable to initialize transform registry!");
            return -1;
        }
    }
    if (currentCount == transform_registry_size) {
        if (pdc_transform_registry_init_(transform_registry_size * 2) == 0) {
            perror("memory allocation failed");
            return -1;
        }
    }

    registry_index = hg_atomic_get32(&registered_transform_ftn_count_g);
    hg_atomic_incr32(&registered_transform_ftn_count_g);
    pdc_region_transform_registry[registry_index] = ftnPtr;

    return registry_index;
}


void
set_execution_locus(PDC_loci locus_identifier)
{
    execution_locus = locus_identifier;
}

PDC_loci
get_execution_locus()
{
    return execution_locus;
}

int
get_ftnPtr_(const char *ftn, char *loadpath, void **ftnPtr)
{
  static void *appHandle = NULL;
  static char *lastopened = NULL;
  void *ftnHandle = NULL;    

  if (lastopened && strcmp(lastopened, loadpath)) {
    /* Rather than closing the previously opened library
     * we should record it as being opened and then
     * eventually close it when we know it's safe, e.g.
     * at shutdown?
     */
    // dlclose(appHandle);
    appHandle = NULL;
    free(lastopened);
  }

  if (appHandle == NULL) {
    if ((appHandle = dlopen(loadpath,RTLD_NOW)) == NULL) {
      fprintf(stderr, "dlopen failed: %s\n", dlerror());
      fflush(stderr);
      return -1;
    }
  }
  ftnHandle = dlsym(appHandle, ftn);
  if (ftnHandle == NULL) {
    fprintf(stderr, "dlsym failed: %s\n", dlerror());
    fflush(stderr);
    return -1;
  } else {
    lastopened = strdup(loadpath);
  }
  *ftnPtr = ftnHandle;
  return 0;
}


// analysis_ftn_cb(hg_handle_t handle)
HG_TEST_RPC_CB(analysis_ftn, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    analysis_ftn_in_t in;
    analysis_ftn_out_t out;
    //struct PDC_iterator_info *inputIter = NULL;
    struct region_analysis_ftn_info *thisFtn = NULL;
    pdcid_t iterIn = -1, iterOut = -1;
    pdcid_t registrationId = -1;
    void *ftnHandle = NULL;
    int (*ftnPtr)(pdcid_t, pdcid_t) = NULL;
    int result;

    FUNC_ENTER(NULL);
    memset(&in,0,sizeof(in));
    // Decode input
    HG_Get_input(handle, &in);
    printf("analysis_ftn_cb entered!\n");

    printf("func = %s\nloadpath = %s\n", 
	   (in.ftn_name == NULL ? "unknown" : in.ftn_name),
	   (in.loadpath == NULL ? "unknown" : in.loadpath));

    if (get_ftnPtr_(in.ftn_name, (char *)in.loadpath, &ftnHandle) < 0)
      printf("get_ftnPtr_ returned an error!\n");

    if ((ftnPtr = ftnHandle) == NULL)
      PGOTO_ERROR(FAIL,"Transforms function lookup failed\n");
    
    if ( ftnPtr != NULL ) {
        if ((iterIn = in.iter_in) == 0) 
            printf("input is a NULL iterator\n");
        else if (execution_locus == SERVER_MEMORY) {
            /* inputIter = &PDC_Block_iterator_cache[iterIn]; */
	}
    if ((iterOut = in.iter_out) == 0)
        printf("output is a NULL iterator\n");
        if ((thisFtn = (struct region_analysis_ftn_info *)
            calloc(sizeof(struct region_analysis_ftn_info), 1)) != NULL) {
	        thisFtn->ftnPtr = (int (*)()) ftnPtr;
	        thisFtn->n_args = 2;
	        thisFtn->object_id = (pdcid_t *)calloc(2, sizeof(pdcid_t));
	        registrationId = pdc_add_analysis_ptr_to_registry_(thisFtn);
	        out.remote_ftn_id = registrationId;
	    }
	    else {
            printf("Unable to allocate storage for the analysis function\n");
	        out.remote_ftn_id = registrationId;
	    }
    } else {
        printf("Failed to resolve %s to a function pointer\n", in.ftn_name);
	    out.remote_ftn_id = registrationId;
    }

    HG_Respond(handle, NULL, NULL, &out);
    
    if (ftnPtr) {
        result = ftnPtr(iterIn,iterOut);
        printf("function call result was %d\n----------------\n", result);

	    /* FIXME:
	     * We might consider adding the function result into
	     * thisFtn. Under that assumption, we might need to
	     * somehow add a way to notify the invoking client
	     * with an exception to handle computation errors...
	     */
         if (thisFtn) {
            thisFtn->ftn_lastResult = result;
	     }
    }

done:
    HG_Free_input(handle, &in);
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

// obj_data_iterator_cb(hg_handle_t handle)
HG_TEST_RPC_CB(obj_data_iterator, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    obj_data_iterator_in_t in;
    obj_data_iterator_out_t out;
    
    FUNC_ENTER(NULL);
    memset(&in,0,sizeof(in));
    // Decode input
    HG_Get_input(handle, &in);
    printf("obj_data_iterator_cb entered!\n");
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&insert_iterator_mutex_g);
#endif
    ret_value = PDC_Server_instantiate_data_iterator(&in, &out);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&insert_iterator_mutex_g);
#endif

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}


HG_TEST_THREAD_CB(obj_data_iterator)
HG_TEST_THREAD_CB(analysis_ftn)


hg_id_t
analysis_ftn_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);
    ret_value = MERCURY_REGISTER(hg_class, "analysis_ftn", analysis_ftn_in_t, analysis_ftn_out_t, analysis_ftn_cb);

    FUNC_LEAVE(ret_value);
}

hg_id_t
obj_data_iterator_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);
    ret_value = MERCURY_REGISTER(hg_class, "obj_data_iterator", obj_data_iterator_in_t, obj_data_iterator_out_t, obj_data_iterator_cb);

    FUNC_LEAVE(ret_value);
}

