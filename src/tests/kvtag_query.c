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
#include <inttypes.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"

int
main()
{

    int         i;
    pdcid_t     pdc, cont_prop, cont, obj_prop1, obj_prop2, obj1, obj2;
    uint64_t *  obj_ids = NULL, *obj_ids1 = NULL, *obj_ids2 = NULL;
    int         nobj;
    pdc_kvtag_t kvtag1, kvtag2, kvtag3;
    char *      v1 = "value1";
    int         v2 = 2;
    double      v3 = 3.45;

    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop > 0)
        printf("Create a container property\n");
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont > 0)
        printf("Create a container c1\n");
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop1 > 0)
        printf("Create an object property\n");
    else
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop2 > 0)
        printf("Create an object property\n");
    else
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    // create first object
    obj1 = PDCobj_create(cont, "o1", obj_prop1);
    if (obj1 > 0)
        printf("Create an object o1\n");
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);

    // create second object
    obj2 = PDCobj_create(cont, "o2", obj_prop2);
    if (obj2 > 0)
        printf("Create an object o2\n");
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);

    kvtag1.name  = "key1string";
    kvtag1.value = (void *)v1;
    kvtag1.size  = strlen(v1) + 1;

    kvtag2.name  = "key2int";
    kvtag2.value = (void *)&v2;
    kvtag2.size  = sizeof(int);

    kvtag3.name  = "key3double";
    kvtag3.value = (void *)&v3;
    kvtag3.size  = sizeof(double);

    if (PDCobj_put_tag(obj1, kvtag1.name, kvtag1.value, kvtag1.size) < 0)
        printf("fail to add a kvtag to o1\n");
    else
        printf("successfully added a kvtag to o1\n");

    if (PDCobj_put_tag(obj1, kvtag2.name, kvtag2.value, kvtag2.size) < 0)
        printf("fail to add a kvtag to o1\n");
    else
        printf("successfully added a kvtag to o1\n");

    if (PDCobj_put_tag(obj2, kvtag2.name, kvtag2.value, kvtag2.size) < 0)
        printf("fail to add a kvtag to o2\n");
    else
        printf("successfully added a kvtag to o2\n");

    if (PDCobj_put_tag(obj2, kvtag3.name, kvtag3.value, kvtag3.size) < 0)
        printf("fail to add a kvtag to o2\n");
    else
        printf("successfully added a kvtag to o2\n");

    if (PDC_Client_query_kvtag(&kvtag1, &nobj, &obj_ids) < 0)
        printf("fail to query a kvtag\n");
    else {
        printf("successfully queried a tag, nres=%d\n", nobj);
        for (i = 0; i < nobj; i++)
            printf("%" PRIu64 ", ", obj_ids[i]);
        printf("\n\n");
    }
    if (obj_ids != NULL)
        free(obj_ids);

    if (PDC_Client_query_kvtag(&kvtag3, &nobj, &obj_ids2) < 0)
        printf("fail to query a kvtag\n");
    else {
        printf("successfully queried a tag, nres=%d\n", nobj);
        for (i = 0; i < nobj; i++)
            printf("%" PRIu64 ", ", obj_ids2[i]);
        printf("\n\n");
    }
    if (obj_ids2 != NULL)
        free(obj_ids2);

    if (PDC_Client_query_kvtag(&kvtag2, &nobj, &obj_ids1) < 0)
        printf("fail to query a kvtag\n");
    else {
        printf("successfully queried a tag, nres=%d\n", nobj);
        for (i = 0; i < nobj; i++)
            printf("%" PRIu64 ", ", obj_ids1[i]);
        printf("\n\n");
    }
    if (obj_ids1 != NULL)
        free(obj_ids1);

    // close first object
    if (PDCobj_close(obj1) < 0)
        printf("fail to close object o1\n");
    else
        printf("successfully close object o1\n");

    // close second object
    if (PDCobj_close(obj2) < 0)
        printf("fail to close object o2\n");
    else
        printf("successfully close object o2\n");

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");
    else
        printf("successfully close container c1\n");

    // close a container property
    if (PDCprop_close(obj_prop1) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close object property\n");

    if (PDCprop_close(obj_prop2) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close object property\n");

    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close container property\n");

    // close pdc
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

    return 0;
}
