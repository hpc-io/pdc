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
#include <getopt.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"

int
main()
{
    pdcid_t        pdc, cont_prop, cont, obj_prop1, obj_prop2, obj1, obj2;
    pdc_kvtag_t    kvtag1, kvtag2, kvtag3;
    char *         v1 = "value1";
    int            v2 = 2;
    double         v3 = 3.45;
    pdc_var_type_t type1, type2, type3;
    void *         value1, *value2, *value3;
    psize_t        value_size;

    // create a pdc
    pdc = PDCinit("pdc");
    LOG_INFO("create a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop > 0)
        LOG_INFO("Create a container property\n");
    else
        LOG_ERROR("Failed to create container property");

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont > 0)
        LOG_INFO("Create a container c1\n");
    else
        LOG_ERROR("Failed to create container");

    // create an object property
    obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop1 > 0)
        LOG_INFO("Create an object property\n");
    else
        LOG_ERROR("Failed to create object property");

    obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop2 > 0)
        LOG_INFO("Create an object property\n");
    else
        LOG_ERROR("Failed to create object property");

    // create first object
    obj1 = PDCobj_create(cont, "o1", obj_prop1);
    if (obj1 > 0)
        LOG_INFO("Create an object o1\n");
    else
        LOG_ERROR("Failed to create object");

    // create second object
    obj2 = PDCobj_create(cont, "o2", obj_prop2);
    if (obj2 > 0)
        LOG_INFO("Create an object o2\n");
    else
        LOG_ERROR("Failed to create object");

    kvtag1.name  = "key1string";
    kvtag1.value = (void *)v1;
    kvtag1.type  = PDC_STRING;
    kvtag1.size  = strlen(v1) + 1;

    kvtag2.name  = "key2int";
    kvtag2.value = (void *)&v2;
    kvtag2.type  = PDC_INT;
    kvtag2.size  = sizeof(int);

    kvtag3.name  = "key3double";
    kvtag3.value = (void *)&v3;
    kvtag3.type  = PDC_DOUBLE;
    kvtag3.size  = sizeof(double);

    if (PDCobj_put_tag(obj1, kvtag1.name, kvtag1.value, kvtag1.type, kvtag1.size) < 0)
        LOG_ERROR("Failed to add a kvtag to o1\n");
    else
        LOG_INFO("successfully added a kvtag to o1\n");

    if (PDCobj_put_tag(obj2, kvtag2.name, kvtag2.value, kvtag2.type, kvtag2.size) < 0)
        LOG_ERROR("Failed to add a kvtag to o1\n");
    else
        LOG_INFO("successfully added a kvtag to o1\n");

    if (PDCobj_put_tag(obj2, kvtag3.name, kvtag3.value, kvtag3.type, kvtag3.size) < 0)
        LOG_ERROR("Failed to add a kvtag to o1\n");
    else
        LOG_INFO("successfully added a kvtag to o1\n");

    if (PDCobj_get_tag(obj1, kvtag1.name, (void *)&value1, (void *)&type1, (void *)&value_size) < 0)
        LOG_ERROR("Failed to get a kvtag from o1\n");
    else
        LOG_INFO("successfully retrieved a kvtag [%s] = [%s] from o1\n", kvtag1.name, (char *)value1);

    if (PDCobj_get_tag(obj2, kvtag2.name, (void *)&value2, (void *)&type2, (void *)&value_size) < 0)
        LOG_ERROR("Failed to get a kvtag from o2\n");
    else
        LOG_INFO("successfully retrieved a kvtag [%s] = [%d] from o2\n", kvtag2.name, *(int *)value2);

    if (PDCobj_get_tag(obj2, kvtag3.name, (void *)&value3, (void *)&type3, (void *)&value_size) < 0)
        LOG_ERROR("Failed to get a kvtag from o2\n");
    else
        LOG_INFO("successfully retrieved a kvtag [%s] = [%f] from o2\n", kvtag3.name, *(double *)value3);

    if (PDCobj_del_tag(obj1, kvtag1.name) < 0)
        LOG_ERROR("Failed to delete a kvtag from o1\n");
    else
        LOG_INFO("successfully deleted a kvtag [%s] from o1\n", kvtag1.name);

    v1           = "New Value After Delete";
    kvtag1.value = (void *)v1;
    kvtag1.type  = PDC_STRING;
    kvtag1.size  = strlen(v1) + 1;
    if (PDCobj_put_tag(obj1, kvtag1.name, kvtag1.value, kvtag1.type, kvtag1.size) < 0)
        LOG_ERROR("Failed to add a kvtag to o1\n");
    else
        LOG_INFO("successfully added a kvtag to o1\n");

    /* PDC_free_kvtag(&value1); */

    if (PDCobj_get_tag(obj1, kvtag1.name, (void *)&value1, (void *)&type1, (void *)&value_size) < 0)
        LOG_ERROR("Failed to get a kvtag from o1\n");
    else
        LOG_INFO("successfully retrieved a kvtag [%s] = [%s] from o1\n", kvtag1.name, (char *)value1);

    /* PDC_free_kvtag(&value1); */
    /* PDC_free_kvtag(&value2); */
    /* PDC_free_kvtag(&value3); */

    // close first object
    if (PDCobj_close(obj1) < 0)
        LOG_ERROR("Failed to close object o1\n");
    else
        LOG_INFO("Successfully closed object o1\n");

    // close second object
    if (PDCobj_close(obj2) < 0)
        LOG_ERROR("Failed to close object o2\n");
    else
        LOG_INFO("Successfully closed object o2\n");

    // close a container
    if (PDCcont_close(cont) < 0)
        LOG_ERROR("Failed to close container c1\n");
    else
        LOG_INFO("Successfully closed container c1\n");

    // close a container property
    if (PDCprop_close(obj_prop1) < 0)
        LOG_ERROR("Failed to close property");
    else
        LOG_INFO("Successfully closed object property\n");

    if (PDCprop_close(obj_prop2) < 0)
        LOG_ERROR("Failed to close property");
    else
        LOG_INFO("Successfully closed object property\n");

    if (PDCprop_close(cont_prop) < 0)
        LOG_ERROR("Failed to close property");
    else
        LOG_INFO("Successfully closed container property\n");

    // close pdc
    if (PDCclose(pdc) < 0)
        LOG_ERROR("Failed to close PDC\n");

    return 0;
}
