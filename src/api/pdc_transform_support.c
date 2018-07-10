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

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

static char *default_pdc_transforms_lib = "libpdctransforms.so";

perr_t
PDCobj_transform_register(char *func, pdcid_t obj_id, int current_state, int next_state, PDCobj_transform_t op_type, PDCdata_movement_t when )
{
    perr_t ret_value = SUCCEED;         /* Return value */
    int (*ftnPtr)() = NULL;
    struct region_transform_ftn_info *thisFtn = NULL;
    pdcid_t meta_id_in = 0, meta_id_out = 0;
    char *thisApp = NULL;
    char *colonsep = NULL; 
    char *transformslibrary = NULL;
    char *applicationDir = NULL;
    char *userdefinedftn = NULL;
    char *loadpath = NULL;

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

    if ((ftnPtr = get_ftnPtr_(userdefinedftn, loadpath)) == NULL)
        PGOTO_ERROR(FAIL,"Transforms function lookup failed\n");

    if ((thisFtn = PDC_MALLOC(struct region_transform_ftn_info)) == NULL)
        PGOTO_ERROR(FAIL,"PDC register_obj_transforms memory allocation failed\n");

    thisFtn->ftnPtr = (int (*)()) ftnPtr;
    thisFtn->object_id = obj_id;
    thisFtn->op_type = op_type;
    thisFtn->when = when;
    thisFtn->lang = C_lang;
    pdc_client_register_obj_transform(userdefinedftn, loadpath, obj_id, current_state, next_state, (int)op_type, (int)when);

#if 0
    // Add to our own list of transforms functions
    if (pdc_add_transform_ptr_to_registry_(thisFtn) < 0)
        PGOTO_ERROR(FAIL,"PDC unable to register transform function!\n");
#endif

done:
    if (applicationDir) free(applicationDir);
    if (userdefinedftn) free(userdefinedftn);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCbuf_map_transform_register(char *func, void *buf, pdcid_t src_region_id, pdcid_t dest_object_id, pdcid_t dest_region_id, int current_state, int next_state, PDCdata_movement_t when )
{
    perr_t ret_value = SUCCEED;         /* Return value */
    int (*ftnPtr)() = NULL;
    struct PDC_obj_info *object1 = NULL;
    struct region_transform_ftn_info *thisFtn = NULL;
    struct PDC_region_info *region_info;
    struct PDC_id_info *id_info;
    pdcid_t meta_id_in = 0, meta_id_out = 0;
    char *thisApp = NULL;
    char *colonsep = NULL; 
    char *transformslibrary = NULL;
    char *applicationDir = NULL;
    char *userdefinedftn = NULL;
    char *loadpath = NULL;

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

    if ((ftnPtr = get_ftnPtr_(userdefinedftn, loadpath)) == NULL)
        PGOTO_ERROR(FAIL,"Transforms function lookup failed\n");

    if ((thisFtn = PDC_MALLOC(struct region_transform_ftn_info)) == NULL)
        PGOTO_ERROR(FAIL,"PDC register_obj_transforms memory allocation failed\n");

    thisFtn->ftnPtr = (int (*)()) ftnPtr;
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
    if (object1 && object1->obj_pt->type_extent == 0) {
        thisFtn->type = object1->obj_pt->type;
        thisFtn->type_extent = get_datatype_size(object1->obj_pt->type);
    }    
    if (next_state == INCR_STATE)
        thisFtn->nextState = current_state +1;
    else if (next_state == DECR_STATE)
        thisFtn->nextState = current_state -1;
    else thisFtn->nextState = current_state;

    pdc_client_register_region_transform(userdefinedftn, 
       	       	                         loadpath,
       	       	                         src_region_id,
       	       	                         dest_region_id,
       	       	                         current_state,
       	       	                         thisFtn->nextState,
       	       	                         (int)PDC_DATA_MAP,
       	       	                         (int)when);

    // Add to our own list of transforms functions
    if (pdc_add_transform_ptr_to_registry_(thisFtn) < 0)
        PGOTO_ERROR(FAIL,"PDC unable to register transform function!\n");

done:
    if (applicationDir) free(applicationDir);
    if (userdefinedftn) free(userdefinedftn);

    FUNC_LEAVE(ret_value);
}
  
