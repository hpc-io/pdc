#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

int main() {
    pdcid_t pdc, cont_prop, cont, obj_prop, obj1, obj2, obj3;
    obj_handle *oh;
    struct PDC_obj_info *info;
    
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
    
    // create first object
    obj1 = PDCobj_create(cont, "o1", obj_prop);
    if(obj1 > 0)
        printf("Create an object, id is %lld\n", obj1);
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // create second object
    obj2 = PDCobj_create(cont, "o2", obj_prop);
    if(obj2 > 0)
        printf("Create an object, id is %lld\n", obj2);
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // create third object
    obj3 = PDCobj_create(cont, "o3", obj_prop);
    if(obj3 > 0)
        printf("Create an object, id is %lld\n", obj3);
    else
        printf("Fail to create object @ line  %d!\n", __LINE__);
    
    // start object iteration
    oh = PDCobj_iter_start(cont);
    
    while(!PDCobj_iter_null(oh)) {
        info = PDCobj_iter_get_info(oh);
        printf("object name is: %s\n", info->name);
        printf("object property id is %lld\n", info->obj_prop);
        
        oh = PDCobj_iter_next(oh, cont);
    }

    
    // close first object
    if(PDCobj_close(obj1) < 0)
        printf("fail to close object %lld\n", obj1);
    else
        printf("successfully close object # %lld\n", obj1);
    
    // close second object
    if(PDCobj_close(obj2) < 0)
        printf("fail to close object %lld\n", obj2);
    else
        printf("successfully close object # %lld\n", obj2);
    
    // close third object
    if(PDCobj_close(obj3) < 0)
        printf("fail to close object %lld\n", obj3);
    else
        printf("successfully close object # %lld\n", obj3);
    
    // close a object property
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
}
