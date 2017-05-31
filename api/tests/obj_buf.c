#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

int main(int argc, char **argv) {
    pdcid_t pdc, cont_prop, cont, obj_prop, obj1;
    int rank = 0, size = 1;
    int myArray[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    void **rbuf;
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc = PDC_init("pdc");
    printf("create a new pdc, pdc id is: %lld\n", pdc);

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop > 0)
        printf("Create a container property, id is %lld\n", cont_prop);
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if(cont > 0)
        printf("Create a container, id is %lld\n", cont);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
    
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop > 0)
        printf("Create an object property, id is %lld\n", obj_prop);
    else
        printf("Fail to create object property @ line  %d!\n", __LINE__);
    
    // create object
    obj1 = PDCobj_create(cont, "o1", obj_prop);
    if(obj1 > 0)
        printf("Create an object, id is %lld\n", obj1);
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // set and retrieve the object buffer
    PDCprop_set_obj_buf(obj_prop, myArray);
    rbuf = PDCobj_buf_retrieve(obj1);
    printf("first number to retrieve is: %d\n", *((int *)*rbuf));
    
    // close object
    if(PDCobj_close(obj1) < 0)
        printf("fail to close object %lld\n", obj1);
    else
        printf("successfully close object # %lld\n", obj1);
    
    // close object property
    if(PDCprop_close(obj_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close object property # %lld\n", obj_prop);
       
    // close a container
    if(PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);
    else
        printf("successfully close container # %lld\n", cont);
    

    // close a container property
    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close container property # %lld\n", cont_prop);

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
