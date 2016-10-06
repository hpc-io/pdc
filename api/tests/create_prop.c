// based upon pdc_init.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

int main() {
    PDC_prop_type type = PDC_OBJ_CREATE;
    pdcid_t create_prop = PDCprop_create(type);
    if(create_prop > 0) {
        if(type == PDC_CONT_CREATE)
            printf("Create a container, id is %d\n", create_prop);
        else if(type == PDC_OBJ_CREATE)
            printf("Create an object, id is %d\n", create_prop);
    }
    else {
        printf("Fail to create @ line %d!\n", __LINE__);
    }

    type = PDC_CONT_CREATE;
    create_prop = PDCprop_create(type);
    if(create_prop > 0) {
        if(type == PDC_CONT_CREATE)
            printf("Create a container, id is %d\n", create_prop);
        else if(type == PDC_OBJ_CREATE)
            printf("Create an object, id is %d\n", create_prop);
    }
    else
        printf("Fail to create @ line  %d!\n", __LINE__);
}
