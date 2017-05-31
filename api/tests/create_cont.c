#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

int main(int argc, char **argv) 
{
    pdcid_t pdc, create_prop, cont;
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc = PDC_init("pdc");
    printf("create a new pdc, pdc id is: %lld\n", pdc);

    // create a container property
    create_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(create_prop > 0)
        printf("Create a container property, id is %lld\n", create_prop);
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", create_prop);
    if(cont > 0)
        printf("Create a container, id is %lld\n", cont);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
       
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
