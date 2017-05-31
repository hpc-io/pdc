#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"


int main(int argc, char **argv) {
    pdcid_t pdc, create_prop1, create_prop2, create_prop;
    PDC_prop_type type;
    int rank = 0, size = 1;
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    pdc = PDC_init("pdc");

    // create an object property
    create_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(create_prop1 <= 0) {
        printf("Fail to create @ line %d\n", __LINE__);
    }
    // create another object property
    create_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(create_prop2 <= 0) {
        printf("Fail to create @ line %d\n", __LINE__);
    }

    if(PDCprop_close(create_prop1)<0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close property # %lld\n", create_prop1);
    if(PDCprop_close(create_prop2)<0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close property # %lld\n", create_prop2);

    // create a container property
    create_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(create_prop > 0) {
        if(type == PDC_CONT_CREATE)
            printf("Create a container property, id is %lld\n", create_prop);
        else if(type == PDC_OBJ_CREATE)
            printf("Create an object property, id is %lld\n", create_prop);
    }
    else
        printf("Fail to create @ line  %d!\n", __LINE__);

    // close property
   if(PDCprop_close(create_prop)<0)
       printf("Fail to close property @ line %d\n", __LINE__);
   else
       printf("successfully close property # %lld\n", create_prop);

    // close a pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
