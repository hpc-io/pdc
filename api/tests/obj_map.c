#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

int main() {
    PDC_prop_t p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);
    printf("create a new pdc, pdc id is: %lld\n", pdc);

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop > 0)
        printf("Create a container property, id is %lld\n", cont_prop);
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    pdcid_t cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont > 0)
        printf("Create a container, id is %lld\n", cont);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
    
    // create an object property
    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop > 0)
        printf("Create an object property, id is %lld\n", obj_prop);
    else
        printf("Fail to create object property @ line  %d!\n", __LINE__);
    // set object dims
    uint64_t a[] = {9};
    PDCprop_set_obj_dims(obj_prop, 1, a, pdc);
    
    // create first object
    pdcid_t obj1 = PDCobj_create(pdc, cont, "o1", obj_prop);
    if(obj1 > 0)
        printf("Create an object, id is %lld\n", obj1);
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // create second object
    pdcid_t obj2 = PDCobj_create(pdc, cont, "o2", obj_prop);
    if(obj2 > 0)
        printf("Create an object, id is %lld\n", obj2);
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    int myArray1[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    int myArray2[9];
    int myArray3[9];
    uint64_t size1[] = {sizeof(myArray1)/sizeof(int)};
    uint64_t size2[] = {sizeof(myArray2)/sizeof(int)};
    uint64_t size3[] = {sizeof(myArray3)/sizeof(int)};
    
    // create a region
    pdcid_t r1 = PDCregion_create(1, (uint64_t)myArray1, size1, pdc);
    printf("first region id: %lld\n", r1);
    pdcid_t r2 = PDCregion_create(1, (uint64_t)myArray2, size2, pdc);
    printf("second region id: %lld\n", r2);
    pdcid_t r3 = PDCregion_create(1, (uint64_t)myArray3, size3, pdc);
    printf("third region id: %lld\n", r3);
    
    PDCobj_map(obj1, r1, obj2, r2, pdc);
    PDCobj_map(obj1, r1, obj2, r3, pdc);
    
    // close regions
    if(PDCregion_close(r1, pdc) < 0)
        printf("fail to close region %lld\n", r1);
    else
        printf("successfully close region # %lld\n", r1);
    
    if(PDCregion_close(r2, pdc) < 0)
        printf("fail to close region %lld\n", r2);
    else
        printf("successfully close region # %lld\n", r2);
    if(PDCregion_close(r3, pdc) < 0)
        printf("fail to close region %lld\n", r3);
    else
        printf("successfully close region # %lld\n", r3);
    
    // close first object
    if(PDCobj_close(obj1, pdc) < 0)
        printf("fail to close object %lld\n", obj1);
    else
        printf("successfully close object # %lld\n", obj1);
    
    // close second object
    if(PDCobj_close(obj2, pdc) < 0)
        printf("fail to close object %lld\n", obj2);
    else
        printf("successfully close object # %lld\n", obj2);
    
    // close a object property
    if(PDCprop_close(obj_prop, pdc) < 0)
        printf("fail to close object property %lld\n", obj_prop);
    else
        printf("successfully close object property # %lld\n", obj_prop);
       
    // close a container
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);
    else
        printf("successfully close container # %lld\n", cont);

    // close a container property
    if(PDCprop_close(cont_prop, pdc) < 0)
        printf("Fail to close container property @ line %d\n", __LINE__);
    else
        printf("successfully close container property # %lld\n", cont_prop);

    // close pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
}
