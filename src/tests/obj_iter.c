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
#include <inttypes.h>
#include "pdc.h"

int main(int argc, char **argv) {
    pdcid_t pdc, cont_prop, cont, obj_prop, obj1, obj2, obj3;
    int rank = 0, size = 1;
    obj_handle *oh;
    struct PDC_obj_info *info;
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop > 0)
        printf("Create a container property\n");
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if(cont > 0)
        printf("Create a container c1\n");
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
    
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop > 0)
        printf("Create an object property\n");
    else
        printf("Fail to create object property @ line  %d!\n", __LINE__);
    
    // create first object
    obj1 = PDCobj_create(cont, "o1", obj_prop);
    if(obj1 > 0)
        printf("Create an object o1\n");
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // create second object
    obj2 = PDCobj_create(cont, "o2", obj_prop);
    if(obj2 > 0)
        printf("Create an object o2\n");
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // create third object
    obj3 = PDCobj_create(cont, "o3", obj_prop);
    if(obj3 > 0)
        printf("Create an object o3\n");
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // start object iteration
    oh = PDCobj_iter_start(cont);
    
    while(!PDCobj_iter_null(oh)) {
        info = PDCobj_iter_get_info(oh);
        printf("object name is: %s\n", info->name);
        printf("object property id is %" PRIu64 "\n", info->obj_pt->obj_prop_id);
        
        oh = PDCobj_iter_next(oh, cont);
    }

    // close first object
    if(PDCobj_close(obj1) < 0)
        printf("fail to close object o1\n");
    else
        printf("successfully close object o1\n");
    
    // close second object
    if(PDCobj_close(obj2) < 0)
        printf("fail to close object o2\n");
    else
        printf("successfully close object o2\n");
    
    // close third object
    if(PDCobj_close(obj3) < 0)
        printf("fail to close object o3\n");
    else
        printf("successfully close object o3\n");
    
    // close a object property
    if(PDCprop_close(obj_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close object property\n");
       
    // close a container
    if(PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");
    else
        printf("successfully close container c1\n");

    // close a container property
    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close container property\n");

    // close pdc
    if(PDCclose(pdc) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
