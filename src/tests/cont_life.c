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

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

int main(int argc, char **argv) 
{
    pdcid_t pdc, cont_prop, cont;
    // create a pdc
int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    pdc = PDC_init("pdc");
    printf("create a new pdc, pdc id is: %lld\n", pdc);

    // create a container property
    pdcid_t create_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(create_prop > 0)
        printf("Create a container property, id is %lld\n", create_prop);
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);
    
    // print default container lifetime (persistent)
    struct PDC_cont_prop *prop = PDCcont_prop_get_info(create_prop);
    if(prop->cont_life == PDC_PERSIST)
        printf("container property (id: %lld) default lifetime is persistent\n", create_prop);
    else
        printf("container property (id: %lld) default lifetime is transient\n", create_prop);
    
    // create a container
    cont = PDCcont_create("c1", create_prop);
    if(cont > 0)
        printf("Create a container, id is %lld\n", cont);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
    
    // set container lifetime to transient
    PDCprop_set_cont_lifetime(create_prop, PDC_TRANSIENT);
    prop = PDCcont_prop_get_info(create_prop);
    if(prop->cont_life == PDC_PERSIST)
        printf("container property (id: %lld) lifetime is persistent\n", create_prop);
    else
        printf("container property (id: %lld) lifetime is transient\n", create_prop);
    
    // set container lifetime to persistent
    PDCcont_persist(cont);
    prop = PDCcont_prop_get_info(create_prop);
    if(prop->cont_life == PDC_PERSIST)
        printf("container property (id: %lld) lifetime is persistent\n", create_prop);
    else
        printf("container property (id: %lld) lifetime is transient\n", create_prop);

    // close a container
    if(PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);
    else
        printf("successfully close container # %lld\n", cont);

    // close a container property
    if(PDCprop_close(create_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close container property # %lld\n", create_prop);

    // close pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
   
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
