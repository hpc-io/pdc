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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "config.h"
#include "pdc.h"
#include "pdc_private.h"
#include "pdc_prop_private.h"
#include "pdc_cont_private.h"
#include "pdc_obj_private.h"
#include "pdc_malloc.h"
#include "pdc_interface.h"
#include "pdc_client_connect.h"

#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

perr_t pdc_class__close(struct PDC_class *p);

static perr_t pdc_class_init()
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    /* Initialize the atom group for the container property IDs */
    if(PDC_register_type(PDC_CLASS, (PDC_free_t)pdc_class__close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize pdc class interface");
    
done:
    FUNC_LEAVE(ret_value);
} 

static pdcid_t pdc_class_create(const char *pdc_name)
{
    pdcid_t ret_value = SUCCEED;         /* Return value */
    pdcid_t pdcid;
    struct PDC_class *p = NULL;
    
    FUNC_ENTER(NULL);
    
    p = PDC_MALLOC(struct PDC_class);
    if(!p)
        PGOTO_ERROR(FAIL, "PDC class property memory allocation failed\n");

    p->name = strdup(pdc_name);
    pdcid = pdc_id_register(PDC_CLASS, p);
    p->local_id = pdcid;
    ret_value = pdcid;
    
done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDC_init(const char *pdc_name)
{
    pdcid_t ret_value = SUCCEED;   
    pdcid_t pdcid;
    
    FUNC_ENTER(NULL);
    
    if(NULL == (pdc_id_list_g = PDC_CALLOC(struct pdc_id_list)))
        PGOTO_ERROR(FAIL, "PDC global id list: memory allocation failed");
    
    if(pdc_class_init() < 0)
        PGOTO_ERROR(FAIL, "PDC class init error");
    pdcid = pdc_class_create(pdc_name);
    
    if(pdc_prop_init() < 0)
        PGOTO_ERROR(FAIL, "PDC property init error");
    if(pdc_cont_init() < 0)
        PGOTO_ERROR(FAIL, "PDC container init error");
    if(pdc_obj_init() < 0)
        PGOTO_ERROR(FAIL, "PDC object init error");
    if(pdc_region_init() < 0)
        PGOTO_ERROR(FAIL, "PDC object init error");

    // PDC Client Server connection init
    PDC_Client_init();

    // create pdc id
    ret_value = pdcid;
    
done:
    FUNC_LEAVE(ret_value);
} 

perr_t pdc_class__close(struct PDC_class *p)
{
    perr_t ret_value = SUCCEED;         
    
    FUNC_ENTER(NULL);
    
    free(p->name);
    p = PDC_FREE(struct PDC_class, p);
    
    FUNC_LEAVE(ret_value);
}

perr_t pdc_class_close(pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;   
    
    FUNC_ENTER(NULL);
    
    /* When the reference count reaches zero the resources are freed */
    if(pdc_dec_ref(pdc) < 0)
        PGOTO_ERROR(FAIL, "PDC: problem of freeing id");
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t pdc_class_end()
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);
    
    if(pdc_destroy_type(PDC_CLASS) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy pdc class interface");
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_close(pdcid_t pdcid)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

#ifdef ENABLE_APP_CLOSE_SERVER
    PDC_Client_close_all_server();
#endif

    // check every list before closing
    // container property
    if(pdc_prop_cont_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close container property");
    // object property
    if(pdc_prop_obj_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close object property");
    // container
    if(pdc_cont_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close container");
    // object
    if(pdc_obj_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close object");
    // region
    if(pdc_region_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close region");
    
    if(pdc_prop_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy property");
    if(pdc_cont_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy container");
    if(pdc_obj_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy object");
    if(pdc_region_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy object");
    
    pdc_class_close(pdcid);
    
    pdc_class_end();
    
    pdc_id_list_g = PDC_FREE(struct pdc_id_list, pdc_id_list_g);

    // Finalize METADATA
    PDC_Client_finalize();

done:
    FUNC_LEAVE(ret_value);
} 
