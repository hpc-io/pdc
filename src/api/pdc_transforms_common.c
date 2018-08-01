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
#include "pdc_transforms_pkg.h"

#ifdef ENABLE_MULTITHREAD 
hg_thread_mutex_t  insert_iterator_mutex_g = HG_THREAD_MUTEX_INITIALIZER;
#endif

//static char *default_pdc_transform_lib = "libpdctransforms.so";


static HG_INLINE hg_return_t
hg_proc_transform_ftn_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    transform_ftn_in_t *struct_data = (transform_ftn_in_t*) data;
    ret = hg_proc_hg_string_t(proc, &struct_data->ftn_name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->loadpath);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->object_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->operation_type);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->start_state);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->next_state);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int8_t(proc, &struct_data->op_type);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int8_t(proc, &struct_data->when);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }

    return ret;
}

static HG_INLINE hg_return_t
hg_proc_transform_ftn_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    transform_ftn_out_t *struct_data = (transform_ftn_out_t *) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

// transform_ftn_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transform_ftn, handle)
{
    hg_return_t ret_value = HG_SUCCESS;
    transform_ftn_in_t in;
    transform_ftn_out_t out;
    void *ftnHandle = NULL;
    void * (*ftnPtr)(pdcid_t) = NULL;

    HG_Get_input(handle, &in);
    printf("transform_ftn_cb entered!\n");
    printf("func = %s\nloadpath = %s\n----------------\n",
	   (in.ftn_name == NULL ? "unknown" : in.ftn_name),
	   (in.loadpath == NULL ? "unknown" : in.loadpath));

    if ( get_ftnPtr_(in.ftn_name, in.loadpath, &ftnHandle) >= 0) {
      out.ret = 0;
    }
    else {
      printf("Unable to resolve transform function pointer\n");
      out.ret = -1;
    }

    HG_Respond(handle, NULL, NULL, &out);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

HG_TEST_THREAD_CB(transform_ftn)

hg_id_t
transform_ftn_register(hg_class_t *hg_class)
{
    hg_id_t ret_value;

    FUNC_ENTER(NULL);
    ret_value = MERCURY_REGISTER(hg_class, "transform_ftn", transform_ftn_in_t, transform_ftn_out_t, transform_ftn_cb);

    FUNC_LEAVE(ret_value);
}

