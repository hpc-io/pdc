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

#include "config.h"
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <libgen.h>
#include "../server/utlist.h"
#include "pdc_obj.h"
#include "pdc_obj_pkg.h"
#include "pdc_malloc.h"
#include "pdc_private.h"
#include "pdc_prop.h"
#include "pdc_obj_private.h"
#include "pdc_interface.h"
#include "pdc_transform_support.h"
#include "pdc_atomic.h"
#include "pdc_analysis_support.h"
#include "pdc_analysis_common.h"

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

static char *default_pdc_transforms_lib = "libpdctransforms.so";

perr_t
PDCobj_transform_register(char *func, pdcid_t obj_id, int current_state, int next_state, PDCobj_transform_t op_type, PDCdata_movement_t when )
{
    perr_t ret_value = SUCCEED;         /* Return value */
    void *ftnHandle = NULL;
    size_t (*ftnPtr)() = NULL;
    struct region_transform_ftn_info *thisFtn = NULL;
    struct PDC_obj_info *obj1, *obj2;
    struct PDC_id_info *objinfo1, *objinfo2;
    struct PDC_region_info *reg1, *reg2;
    pdcid_t src_region_id, dest_region_id;
    pdcid_t src_object_id, dest_object_id;
    char *thisApp = NULL;
    char *colonsep = NULL; 
    char *transformslibrary = NULL;
    char *applicationDir = NULL;
    char *userdefinedftn = NULL;
    char *loadpath = NULL;
    int local_regIndex;

    FUNC_ENTER(NULL);

    thisApp = pdc_get_argv0_();
    if (thisApp) 
      applicationDir = dirname(strdup(thisApp));
    userdefinedftn = strdup(func);

    if ((colonsep = strrchr(userdefinedftn, ':')) != NULL) {
        *colonsep++ = 0;
        transformslibrary = colonsep;
    }
    else transformslibrary = default_pdc_transforms_lib;

    // TODO:
    // Should probably validate the location of the "transformslibrary"
    //
    loadpath = get_realpath(transformslibrary, applicationDir);
    
    if (get_ftnPtr_(userdefinedftn, loadpath, &ftnHandle) < 0)
      printf("get_ftnPtr_ returned an error!\n");      

    if ((ftnPtr = ftnHandle) == NULL)
      PGOTO_ERROR(FAIL,"Transforms function lookup failed\n");

    if ((thisFtn = PDC_MALLOC(struct region_transform_ftn_info)) == NULL)
        PGOTO_ERROR(FAIL,"PDC register_obj_transforms memory allocation failed\n");

    memset(thisFtn,0, sizeof(struct region_transform_ftn_info));
    thisFtn->ftnPtr = (size_t (*)()) ftnPtr;
    thisFtn->object_id = obj_id;
    thisFtn->op_type = op_type;
    thisFtn->when = when;
    thisFtn->lang = C_lang;
    thisFtn->dest_type = PDC_UNKNOWN;

    // Add to our own list of transform functions
    if ((local_regIndex = pdc_add_transform_ptr_to_registry_(thisFtn)) < 0)
        PGOTO_ERROR(FAIL,"PDC unable to register transform function!\n");

    // pdc_client_register_obj_transform(userdefinedftn, loadpath, obj_id, current_state, next_state, (int)op_type, (int)when);

    // Flag the transform as being active on mapping operations
    if (op_type == PDC_DATA_MAP) {
        objinfo1 = pdc_find_id(obj_id);
        if(objinfo1 == NULL)
           PGOTO_ERROR(FAIL, "cannot locate local object ID");
        obj1 = (struct PDC_obj_info *)(objinfo1->obj_ptr);
	/* See if any mapping operations are defined */
	if (obj1 && (obj1->region_list_head != NULL)) {
	    struct PDC_id_info *id_info = pdc_find_id(obj1->region_list_head->orig_reg_id);
            struct PDC_obj_prop *prop;
	    src_region_id = obj1->region_list_head->orig_reg_id;
	    dest_region_id = obj1->region_list_head->des_reg_id;
            // mapping is already defined...
            if (id_info && ((reg1 = (struct PDC_region_info *)id_info->obj_ptr) != NULL)) {
                thisFtn->src_region = reg1;
                obj1 = reg1->obj;
		// Requires that the PDCprop_set_obj_buf function be used...
		if (obj1 && ((prop = obj1->obj_pt) != NULL)) {
                    thisFtn->data = prop->buf;
		    thisFtn->type = prop->type;
                    thisFtn->type_extent = prop->type_extent;
                }
            }
	    id_info = pdc_find_id(dest_region_id);
	    if (id_info && ((reg2 = (struct PDC_region_info *)id_info->obj_ptr) != NULL)) {
                thisFtn->dest_region = reg2;
                obj2 = reg2->obj;
		dest_object_id = obj2->local_id;
		if (obj2 && ((prop = obj2->obj_pt) != NULL)) {
                    thisFtn->result = prop->buf;                   
                    thisFtn->dest_type = prop->type;
                    thisFtn->dest_extent = prop->type_extent;
		}
	    }
	    // Flag the destination region with the transform
	    reg2->registered_op |= PDC_TRANSFORM;
	}
	pdc_client_register_region_transform(userdefinedftn, 
       	       	                         loadpath,
       	       	                         src_region_id,
       	       	                         dest_region_id,
                                         dest_object_id,
       	       	                         current_state,
       	       	                         thisFtn->nextState,
       	       	                         (int)PDC_DATA_MAP,
       	       	                         (int)when,
					 local_regIndex);


    }


done:
    if (applicationDir) free(applicationDir);
    if (userdefinedftn) free(userdefinedftn);

    FUNC_LEAVE(ret_value);
}


perr_t
PDCbuf_map_transform_register(char *func, void *buf,
			      pdcid_t src_region_id, pdcid_t dest_object_id, pdcid_t dest_region_id,
			      int current_state, int next_state,
			      PDCdata_movement_t when )
{
    perr_t ret_value = SUCCEED;         /* Return value */
    void *ftnHandle = NULL;
    size_t (*ftnPtr)() = NULL;
    struct PDC_obj_info *object1 = NULL;
    struct region_transform_ftn_info *thisFtn = NULL;
    struct PDC_region_info *region_info;
    struct PDC_id_info *id_info;
    char *thisApp = NULL;
    char *colonsep = NULL; 
    char *transformslibrary = NULL;
    char *applicationDir = NULL;
    char *userdefinedftn = NULL;
    char *loadpath = NULL;
    int local_regIndex;

    FUNC_ENTER(NULL);

    thisApp = pdc_get_argv0_();
    if (thisApp) 
      applicationDir = dirname(strdup(thisApp));
    userdefinedftn = strdup(func);

    if ((colonsep = strrchr(userdefinedftn, ':')) != NULL) {
        *colonsep++ = 0;
        transformslibrary = colonsep;
    }
    else transformslibrary = default_pdc_transforms_lib;

    // TODO:
    // Should probably validate the location of the "transformslibrary"
    //
    loadpath = get_realpath(transformslibrary, applicationDir);

    if (get_ftnPtr_(userdefinedftn, loadpath, &ftnHandle) < 0)
      PGOTO_ERROR(FAIL,"get_ftnPtr_ returned an error!\n");

    if ((ftnPtr = ftnHandle) == NULL)
      PGOTO_ERROR(FAIL,"Transforms function lookup failed\n");

    if ((thisFtn = PDC_MALLOC(struct region_transform_ftn_info)) == NULL)
        PGOTO_ERROR(FAIL,"PDC register_obj_transforms memory allocation failed\n");

    thisFtn->ftnPtr = (size_t (*)()) ftnPtr;
    thisFtn->object_id = dest_object_id;
    id_info = pdc_find_id(src_region_id);
    if (id_info && ((region_info = (struct PDC_region_info *)id_info->obj_ptr) != NULL))
        thisFtn->src_region = region_info;

    id_info = pdc_find_id(dest_region_id);
    if (id_info && ((region_info = (struct PDC_region_info *)id_info->obj_ptr) != NULL))
        thisFtn->dest_region = region_info;

    thisFtn->op_type = PDC_DATA_MAP;
    thisFtn->when = when;
    thisFtn->lang = C_lang;
    thisFtn->client_id = pdc_client_mpi_rank_g;
    thisFtn->readyCount = current_state;
    thisFtn->ftn_lastResult = 0;
    thisFtn->data = buf;
    id_info = pdc_find_id(dest_object_id);
    if (id_info) object1 = (struct PDC_obj_info *)(id_info->obj_ptr);
    if (object1) {
        thisFtn->type = object1->obj_pt->type;
	if ((thisFtn->type_extent = object1->obj_pt->type_extent) == 0)
            thisFtn->type_extent = get_datatype_size(object1->obj_pt->type);
    }    
    if (next_state == INCR_STATE)
        thisFtn->nextState = current_state +1;
    else if (next_state == DECR_STATE)
        thisFtn->nextState = current_state -1;
    else thisFtn->nextState = current_state;

    // Add to our own list of transforms functions
    if ((local_regIndex = pdc_add_transform_ptr_to_registry_(thisFtn)) < 0)
        PGOTO_ERROR(FAIL,"PDC unable to register transform function!\n");

    pdc_client_register_region_transform(userdefinedftn, 
       	       	                         loadpath,
       	       	                         src_region_id,
       	       	                         dest_region_id,
                                         dest_object_id,
       	       	                         current_state,
       	       	                         thisFtn->nextState,
       	       	                         (int)PDC_DATA_MAP,
       	       	                         (int)when,
					 local_regIndex);

done:
    if (applicationDir) free(applicationDir);
    if (userdefinedftn) free(userdefinedftn);

    FUNC_LEAVE(ret_value);
}
  
perr_t
PDCbuf_io_transform_register(char *func, void *buf,
                             pdcid_t src_region_id, int current_state, int next_state,
                             PDCdata_movement_t when )
{
    perr_t ret_value = FAIL;         /* Return value (not implemented) */
    void *ftnHandle = NULL;
    size_t (*ftnPtr)() = NULL;
    struct PDC_obj_info *object1 = NULL;
    struct region_transform_ftn_info *thisFtn = NULL;
    struct PDC_region_info *region_info;
    struct PDC_id_info *id_info;
    char *thisApp = NULL;
    char *colonsep = NULL; 
    char *transformslibrary = NULL;
    char *applicationDir = NULL;
    char *userdefinedftn = NULL;
    char *loadpath = NULL;
    int local_regIndex;

    FUNC_ENTER(NULL);
    printf("IO transforms are not currently supported!\n");
done:
    if (applicationDir) free(applicationDir);
    if (userdefinedftn) free(userdefinedftn);

    FUNC_LEAVE(ret_value);

}

