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
 * This file includes the functionality to support PDC transforms
 ************************************************************************ */

#include "pdc_analysis_pkg.h"
#include "pdc_transforms_common.h"
#include "pdc_client_server_common.h"

#ifdef ENABLE_MULTITHREAD
extern hg_thread_pool_t *hg_test_thread_pool_g;
#endif

// transform_ftn_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transform_ftn, handle)
{
    hg_return_t                            ret_value = HG_SUCCESS;
    transform_ftn_in_t                     in;
    transform_ftn_out_t                    out       = {0, 0, 0, -1};
    struct _pdc_region_transform_ftn_info *thisFtn   = NULL;
    void *                                 ftnHandle = NULL;

    FUNC_ENTER(NULL);

    HG_Get_input(handle, &in);

    if (PDC_get_ftnPtr_(in.ftn_name, in.loadpath, &ftnHandle) >= 0) {
        thisFtn = malloc(sizeof(struct _pdc_region_transform_ftn_info));
        if (thisFtn == NULL)
            PGOTO_ERROR(HG_OTHER_ERROR, "transform_ftn_cb: Memory allocation failed");
        /* This sets up the index return for the client!
         * We probably need to add more info to the
         * 'thisFtn' structure, but this should be a
         * decent start.
         *
         * TODO:  Add more info.
         * Note that we (the server in this case) may not
         * have the actual object or region.  The mapping
         * protocal is that the server gets a copy of the
         * data once a WRITE-UNLOCK happens.  At that point
         * the server may get an indication that it has a
         * transform to perform upon receipt of the data,
         * e.g. uncompress.
         * The client return contains the storage index of
         * the transform; which it should cache as part of
         * client data structures.  This should allow it
         * the client to include the cached index value
         * as part of a future data movement request.
         */
        thisFtn->ftnPtr    = (size_t(*)())ftnHandle;
        thisFtn->object_id = in.object_id;
        thisFtn->region_id = in.region_id;
        thisFtn->op_type   = (pdc_obj_transform_t)in.op_type;
        out.ret            = PDC_add_transform_ptr_to_registry_(thisFtn);
        out.client_index   = in.client_index;
        out.object_id      = in.object_id;
        out.region_id      = in.region_id;
    }
    else {
        printf("Unable to resolve transform function pointer\n");
        out.ret = -1;
    }

done:
    fflush(stdout);
    HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

HG_TEST_THREAD_CB(transform_ftn)

hg_id_t
PDC_transform_ftn_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = MERCURY_REGISTER(hg_class, "transform_ftn", transform_ftn_in_t, transform_ftn_out_t,
                                 transform_ftn_cb);

    FUNC_LEAVE(ret_value);
}
