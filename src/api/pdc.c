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

#include "pdc.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_cont_pkg.h"
#include "pdc_obj_pkg.h"
#include "pdc_region_pkg.h"
#include "pdc_analysis_pkg.h"
#include "pdc_interface.h"
#include "pdc_client_connect.h"

#include "pdc_timing.h"

pbool_t err_occurred = FALSE;

perr_t PDC_class__close(struct _pdc_class *p);

static perr_t
PDC_class_init()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the container property IDs */
    if (PDC_register_type(PDC_CLASS, (PDC_free_t)PDC_class__close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize pdc class interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static pdcid_t
PDC_class_create(const char *pdc_name)
{
    pdcid_t            ret_value = SUCCEED;
    pdcid_t            pdcid;
    struct _pdc_class *p = NULL;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct _pdc_class);
    if (!p)
        PGOTO_ERROR(FAIL, "PDC class property memory allocation failed\n");

    p->name     = strdup(pdc_name);
    pdcid       = PDC_id_register(PDC_CLASS, p);
    p->local_id = pdcid;
    ret_value   = pdcid;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCinit(const char *pdc_name)
{
    pdcid_t ret_value = SUCCEED;
    pdcid_t pdcid;

    FUNC_ENTER(NULL);

    if (NULL == (pdc_id_list_g = PDC_CALLOC(struct pdc_id_list)))
        PGOTO_ERROR(FAIL, "PDC global id list: memory allocation failed");

    if (PDC_class_init() < 0)
        PGOTO_ERROR(FAIL, "PDC class init error");
    pdcid = PDC_class_create(pdc_name);

    if (PDC_prop_init() < 0)
        PGOTO_ERROR(FAIL, "PDC property init error");
    if (PDC_cont_init() < 0)
        PGOTO_ERROR(FAIL, "PDC container init error");
    if (PDC_obj_init() < 0)
        PGOTO_ERROR(FAIL, "PDC object init error");
    if (PDC_region_init() < 0)
        PGOTO_ERROR(FAIL, "PDC object init error");

    // PDC Client Server connection init
    PDC_Client_init();
#if PDC_TIMING == 1
    PDC_timing_init();
#endif
    ret_value = pdcid;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_class__close(struct _pdc_class *p)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);
#if PDC_TIMING == 1
    PDC_timing_finalize();
#endif

    free(p->name);
    p = PDC_FREE(struct _pdc_class, p);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_class_close(pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(pdc) < 0)
        PGOTO_ERROR(FAIL, "PDC: problem of freeing id");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_class_end()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (PDC_destroy_type(PDC_CLASS) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy pdc class interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCclose(pdcid_t pdcid)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

#ifdef ENABLE_APP_CLOSE_SERVER
    PDC_Client_close_all_server();
#endif

    // check every list before closing
    // container property
    if (PDC_prop_cont_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close container property");
    // object property
    if (PDC_prop_obj_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close object property");
    // container
    if (PDC_cont_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close container");
    // object
    if (PDC_obj_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close object");
    // region
    if (PDC_region_list_null() < 0)
        PGOTO_ERROR(FAIL, "fail to close region");

    if (PDC_prop_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy property");
    if (PDC_cont_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy container");
    if (PDC_obj_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy object");
    if (PDC_region_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy region");

    if (PDC_iterator_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy iterator");
    if (PDC_analysis_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy analysis");
    if (PDC_transform_end() < 0)
        PGOTO_ERROR(FAIL, "fail to destroy transform");

    PDC_class_close(pdcid);

    PDC_class_end();

    pdc_id_list_g = PDC_FREE(struct pdc_id_list, pdc_id_list_g);

    // Finalize METADATA
    PDC_Client_finalize();

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
