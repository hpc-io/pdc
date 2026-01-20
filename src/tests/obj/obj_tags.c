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
#include <string.h>
#include "pdc.h"
#include "test_helper.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop;
    perr_t  ret;
    pdcid_t obj1, obj2;
    int     ret_value = TSUCCEED;

    int rank = 0, size = 1;

    size_t   ndim = 3;
    uint64_t dims[3];
    dims[0] = 64;
    dims[1] = 3;
    dims[2] = 4;
    char           tag_value[128], tag_value2[128], *tag_value_ret;
    char           cont_name[128], obj_name1[128], obj_name2[128];
    pdc_var_type_t value_type;
    psize_t        value_size;

    strcpy(tag_value, "some tag value");
    strcpy(tag_value2, "some tag value 2 is longer");

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");

    // create a container
    sprintf(cont_name, "c%d", rank);
    TASSERT((cont = PDCcont_create(cont_name, cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");
    // create an object property
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT(PDCprop_set_obj_dims(obj_prop, ndim, dims) >= 0, "Call to PDCprop_set_obj_dims succeeded",
            "Call to PDCprop_set_obj_dims failed");
    TASSERT(PDCprop_set_obj_type(obj_prop, PDC_DOUBLE) >= 0, "Call to PDCprop_set_obj_type succeeded",
            "Call to PDCprop_set_obj_type failed");

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    TASSERT((obj1 = PDCobj_create(cont, obj_name1, obj_prop)) != 0,
            "Call to PDCobj_create succeeded for obj1", "Call to PDCobj_create failed for obj1");
    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    TASSERT((obj2 = PDCobj_create(cont, obj_name2, obj_prop)) != 0,
            "Call to PDCobj_create succeeded for obj2", "Call to PDCobj_create failed for obj2");

    // put tags in obj1
    TASSERT(PDCobj_put_tag(obj1, "some tag", tag_value, PDC_STRING, strlen(tag_value) + 1) >= 0,
            "Call to PDCobj_put_tag succeeded for obj1", "Call to PDCobj_put_tag failed for obj1");
    TASSERT(PDCobj_put_tag(obj1, "some tag 2", tag_value2, PDC_STRING, strlen(tag_value2) + 1) >= 0,
            "Call to PDCobj_put_tag succeeded for obj1", "Call to PDCobj_put_tag failed for obj1");

    // put tags in obj2
    TASSERT(PDCobj_put_tag(obj2, "some tag", tag_value, PDC_STRING, strlen(tag_value) + 1) >= 0,
            "Call to PDCobj_put_tag succeeded for obj2", "Call to PDCobj_put_tag failed for obj2");
    TASSERT(PDCobj_put_tag(obj2, "some tag 2", tag_value2, PDC_STRING, strlen(tag_value2) + 1) >= 0,
            "Call to PDCobj_put_tag succeeded for obj2", "Call to PDCobj_put_tag failed for obj2");

    // get tags in obj1
    TASSERT(PDCobj_get_tag(obj1, "some tag", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCobj_get_tag succeeded for obj1", "Call to PDCobj_get_tag failed for obj1");
    if (strcmp(tag_value, tag_value_ret) != 0) {
        LOG_ERROR("Wrong tag value at object 1, expected = %s, get %s\n", tag_value, tag_value_ret);
        TGOTO_DONE(TFAIL);
    }
    TASSERT(PDCobj_get_tag(obj1, "some tag 2", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCobj_get_tag succeeded for obj1", "Call to PDCobj_get_tag failed for obj1");
    if (strcmp(tag_value2, tag_value_ret) != 0) {
        LOG_ERROR("Wrong tag value 2 at object 1, expected = %s, get %s\n", tag_value2, tag_value_ret);
        TGOTO_DONE(TFAIL);
    }

    // get tags in obj2
    TASSERT(PDCobj_get_tag(obj2, "some tag", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCobj_get_tag succeeded for obj2", "Call to PDCobj_get_tag failed for obj2");
    if (strcmp(tag_value, tag_value_ret) != 0) {
        LOG_ERROR("Wrong tag value at object 2, expected = %s, get %s\n", tag_value, tag_value_ret);
        TGOTO_DONE(TFAIL);
    }
    TASSERT(PDCobj_get_tag(obj2, "some tag 2", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCobj_get_tag succeeded for obj2", "Call to PDCobj_get_tag failed for obj2");
    if (strcmp(tag_value2, tag_value_ret) != 0) {
        LOG_ERROR("Wrong tag value 2 at object 2, expected = %s, get %s\n", tag_value2, tag_value_ret);
        TGOTO_DONE(TFAIL);
    }

    // close objects
    TASSERT(PDCobj_close(obj1) >= 0, "Call to PDCobj_close succeeded for obj1",
            "Call to PDCobj_close failed for obj1");
    TASSERT(PDCobj_close(obj2) >= 0, "Call to PDCobj_close succeeded for obj2",
            "Call to PDCobj_close failed for obj2");
    // close container
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    // close properties
    TASSERT(PDCprop_close(obj_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
