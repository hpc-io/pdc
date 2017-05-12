#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

int main() {
    struct PDC_prop p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);
    printf("create a new pdc, pdc id is: %lld\n", pdc);

    // create a container property
    pdcid_t create_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(create_prop > 0)
        printf("Create a container property, id is %lld\n", create_prop);
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    pdcid_t cont1 = PDCcont_create(pdc, "c1", create_prop);
    if(cont1 > 0)
        printf("Create a container, id is %lld\n", cont1);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
       
    // create second container
    pdcid_t cont2 = PDCcont_create(pdc, "c2", create_prop);
    if(cont2 > 0)
        printf("Create a container, id is %lld\n", cont2);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create third container
    pdcid_t cont3 = PDCcont_create(pdc, "c3", create_prop);
    if(cont3 > 0)
        printf("Create a container, id is %lld\n", cont3);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // start container iteration
    cont_handle *ch = PDCcont_iter_start(pdc);

    while(!PDCcont_iter_null(ch)) {
        struct PDC_cont_info *info = PDCcont_iter_get_info(ch);
        printf("container name is: %s\n", info->name);
        printf("container is in pdc %lld\n", info->pdc);
        printf("container property id is %lld\n", info->cont_prop);
        
        ch = PDCcont_iter_next(ch);
    }
    
    // close cont1
    if(PDCcont_close(cont1, pdc) < 0)
        printf("fail to close container %lld\n", cont1);
    else
        printf("successfully close container # %lld\n", cont1);

    // close cont2
    if(PDCcont_close(cont2, pdc) < 0)
        printf("fail to close container %lld\n", cont2);
    else
        printf("successfully close container # %lld\n", cont2);

    // close cont3
    if(PDCcont_close(cont3, pdc) < 0)
        printf("fail to close container %lld\n", cont3);
    else
        printf("successfully close container # %lld\n", cont3);

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
