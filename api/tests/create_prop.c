#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"


int main() {
    struct PDC_prop p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);

    // create an object property
    PDC_prop_type type = PDC_OBJ_CREATE;
    pdcid_t create_prop1 = PDCprop_create(type, pdc);
    if(create_prop1 > 0) {
        if(type == PDC_CONT_CREATE)
            printf("Create a container property, id is %lld\n", create_prop1);
        else if(type == PDC_OBJ_CREATE)
            printf("Create an object property, id is %lld\n", create_prop1);
    }
    else {
        printf("Fail to create @ line %d\n", __LINE__);
    }
    // create another object property
    pdcid_t create_prop2 = PDCprop_create(type, pdc);
    if(create_prop2 > 0) {
        if(type == PDC_CONT_CREATE)
            printf("Create a container property, id is %lld\n", create_prop2);
        else if(type == PDC_OBJ_CREATE)
            printf("Create an object property, id is %lld\n", create_prop2);
    }
    else {
        printf("Fail to create @ line %d\n", __LINE__);
    }

    if(PDCprop_close(create_prop1, pdc)<0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close property # %lld\n", create_prop1);
    if(PDCprop_close(create_prop2, pdc)<0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close property # %lld\n", create_prop2);

    // create a container property
    type = PDC_CONT_CREATE;
    pdcid_t create_prop = PDCprop_create(type, pdc);
    if(create_prop > 0) {
        if(type == PDC_CONT_CREATE)
            printf("Create a container property, id is %lld\n", create_prop);
        else if(type == PDC_OBJ_CREATE)
            printf("Create an object property, id is %lld\n", create_prop);
    }
    else
        printf("Fail to create @ line  %d!\n", __LINE__);

    // close property
   if(PDCprop_close(create_prop, pdc)<0)
       printf("Fail to close property @ line %d\n", __LINE__);
   else
       printf("successfully close property # %lld\n", create_prop);

    // close a pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
}
