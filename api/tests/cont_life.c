#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

int main() {
    struct PDC_prop p;
    pdcid_t pdc, cont_prop, cont;
    // create a pdc
    pdc = PDC_init(p);
    printf("create a new pdc, pdc id is: %lld\n", pdc);

    // create a container property
    pdcid_t create_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(create_prop > 0)
        printf("Create a container property, id is %lld\n", create_prop);
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);
    
    // print default container lifetime (persistent)
    struct PDC_cont_prop *prop = PDCcont_prop_get_info(create_prop, pdc);
    if(prop->cont_life == PDC_PERSIST)
        printf("container property (id: %lld) default lifetime is persistent\n", create_prop);
    else
        printf("container property (id: %lld) default lifetime is transient\n", create_prop);
    
    // create a container
    cont = PDCcont_create(pdc, "c1", create_prop);
    if(cont > 0)
        printf("Create a container, id is %lld\n", cont);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
    
    // set container lifetime to transient
    PDCprop_set_cont_lifetime(create_prop, PDC_TRANSIENT, pdc);
    prop = PDCcont_prop_get_info(create_prop, pdc);
    if(prop->cont_life == PDC_PERSIST)
        printf("container property (id: %lld) lifetime is persistent\n", create_prop);
    else
        printf("container property (id: %lld) lifetime is transient\n", create_prop);
    
    // set container lifetime to persistent
    PDCcont_persist(cont, pdc);
    prop = PDCcont_prop_get_info(create_prop, pdc);
    if(prop->cont_life == PDC_PERSIST)
        printf("container property (id: %lld) lifetime is persistent\n", create_prop);
    else
        printf("container property (id: %lld) lifetime is transient\n", create_prop);

    // close a container
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);
    else
        printf("successfully close container # %lld\n", cont);

    // close a container property
    if(PDCprop_close(create_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close container property # %lld\n", create_prop);

    // close pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
}
