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
#include "test_helper.h"

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
    int            ret_value = SUCCEED;
    int            rank      = 0;

    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
        // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
    TASSERT((cont = PDCcont_create("c1", cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");
    // create object properties
    TASSERT((obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT((obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create first object
    TASSERT((obj1 = PDCobj_create(cont, "o1", obj_prop1)) != 0, "Call to PDCobj_create succeeded for obj1",
            "Call to PDCobj_create failed for obj1");
    // create second object
    TASSERT((obj2 = PDCobj_create(cont, "o2", obj_prop2)) != 0, "Call to PDCobj_create succeeded for obj2",
            "Call to PDCobj_create failed for obj2");

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

    TASSERT(PDC_metadata_key_add(obj1, &kvtag1, false) >= 0,
            "Call to PDC_metadata_key_add succeeded for obj1", "Call to PDC_metadata_key_add failed for obj1");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
    return ret_value;
}
