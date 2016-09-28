#include <stdio.h>
//#include <stdlib.h>
#include <string.h>

#include "pdc.h"

int main(int argc, char ** argv){
    PDC_property prop;
    pid_t pdc_id, nloci = 0;
    cont_handle ch;
    obj_handle oh;

    pdc_id = PDCinit(prop);
    PDCget_loci_count(pdc_id, &nloci);
    int u;
    for(u = 0; u < nloci; u++) {
        PDC_loci_info_t *info;
        PDCget_loci_info(pdc_id, u, info);
    }

    ch = PDCcont_iter_start(pdc_id);

    while(!PDCcont_iter_null(ch)) {
        PDC_cont_info_t *info;
        info = PDCcont_iter_get_info(ch);

        if(strcmp(info->name, "interesting")) {
            pid_t cont_id = PDCcont_open(pdc_id, "container name");
            oh = PDCobj_iter_start(cont_id);
            while(!PDCobj_iter_null(oh)) {
                PDC_obj_info_t *info;
                info = PDCobj_iter_get_info(oh);
                if(!strcmp(info->name, "intteresting")) {
                    pid_t obj_id = PDCobj_open(cont_id, "obj name");
                    PDCobj_close(obj_id);
                }
                PDCobj_iter_next(oh);
            }
        }
        PDCcont_iter_next(ch);
    }

// Query for interesting objects instead
// Create a container
    pid_t cont_create_prop = PDCprop_create(PDC_CONT_CREATE);
    PDCprop_set_cont_lifetime(cont_create_prop, PDC_PERSIST);
    pid_t vpic_container_id = PDCcont_create(pdc_id, "vpic.pdc", cont_create_prop);

    pid_t x_obj_create_prop = PDCprop_create(PDC_OBJ_CREATE);
    PDCprop_set_obj_lifetime(x_obj_create_prop, PDC_PERSIST);

    uint64_t xdims[3] = {10, 20, 30};
    PDCprop_set_obj_dims(x_obj_create_prop, 3, xdims);

    PDC_STRUCT pdc_struct;
    pid_t x_type_id = PDCtype_create(pdc_struct);
    PDCtype_struct_field_insert(x_type_id, "x1", 0, PDC_INT);
    PDCtype_struct_field_insert(x_type_id, "x2", 4, PDC_FLOAT);
    PDCtype_struct_field_insert(x_type_id, "x3", 8, PDC_INT);

    PDCprop_set_obj_type(x_obj_create_prop, x_type_id);
    pid_t x_obj_id = PDCobj_create(vpic_container_id, "x", x_obj_create_prop);

    pid_t y_obj_create_prop = PDCprop_create(PDC_OBJ_CREATE);
    PDCprop_set_obj_lifetime(y_obj_create_prop, PDC_TRANSIENT);
    uint64_t ydims[2] = {20, 40};
    PDCprop_set_obj_dims(y_obj_create_prop, 2, ydims);  
    PDCprop_set_obj_type(y_obj_create_prop, PDC_FLOAT);   
    pid_t y_obj_id = PDCobj_create(vpic_container_id, "y", y_obj_create_prop);

    pid_t z_obj_create_prop = PDCprop_create(PDC_OBJ_CREATE);
    PDCprop_set_obj_lifetime(z_obj_create_prop, PDC_PERSIST);
    uint64_t zdims[3] = {30, 60, 90};
    PDCprop_set_obj_dims(z_obj_create_prop, 3, zdims);
    PDCprop_set_obj_type(z_obj_create_prop, PDC_DOUBLE);
    pid_t z_obj_id = PDCobj_create(vpic_container_id, "z", z_obj_create_prop);

// Built-in transform
    PDC_transform B;
    B.type = ROW_major;
    PDCprop_set_obj_loci_prop(z_obj_create_prop, MEMORY, B);
    B.type = COL_major;
    PDCprop_set_obj_loci_prop(z_obj_create_prop, FLASH, B);

// User transform
    PDC_transform A;
    A.type = ROW_major;
    PDCprop_set_obj_transform(z_obj_create_prop, MEMORY, A, DISK);
// or
    PDCprop_set_obj_loci_prop(z_obj_create_prop, MEMORY, A);
    PDCprop_set_obj_loci_prop(z_obj_create_prop, DISK, A);

    pid_t p_obj_create_prop = PDCprop_create(PDC_OBJ_CREATE);
    uint64_t pdims[3] = {50, 100, 200};
    PDCprop_set_obj_dims(p_obj_create_prop, 3, pdims);
    PDCprop_set_obj_type(p_obj_create_prop, PDC_FLOAT);
    pid_t p_obj_id = PDCobj_create(vpic_container_id, "p", p_obj_create_prop);  // <MEM_ID>

    void *pbuf;  //malloc later
//    PDCobj_buf_retrieve(p_obj_id, pbuf);

// Map/retrieve memory buffers for objects
    PDC_region X_REGION, P_REGION;
    X_REGION.offset = 64;
    P_REGION.offset = 256;
    X_REGION.storage_size = 512;
    P_REGION.storage_size = 512;
    X_REGION.locus = MEMORY;
    P_REGION.locus = FLASH;
    PDCobj_map(p_obj_id, P_REGION, x_obj_id, X_REGION);

    pid_t q_obj_create_prop = PDCprop_create(PDC_OBJ_CREATE);
    uint64_t qdims[3] = {40, 80, 120};
    PDCprop_set_obj_dims(q_obj_create_prop, 3, qdims);
    PDCprop_set_obj_type(q_obj_create_prop, PDC_FLOAT);
    void *qbuf;   //malloc later
    PDCprop_set_obj_buf(q_obj_create_prop, qbuf);
    pid_t q_obj_id = PDCobj_create(vpic_container_id, "q", q_obj_create_prop);  // <MEM_ID>

    PDC_region Q_REGION = P_REGION;
    PDC_region Y_REGION = X_REGION;
    PDCobj_map(q_obj_id, Q_REGION, y_obj_id, Y_REGION);

// Tell the PDC system that region in the memory is stale WRT to the container
// (i.e. read)
    PDCobj_invalidate_region(p_obj_id, X_REGION);
    PDCobj_sync(p_obj_id);
        
//  Computation
         
// Tell the PDC system that the region in memory is updated WRT to the container
// (i.e. write)
    PDCobj_update_region(p_obj_id, P_REGION);
    PDCobj_sync(p_obj_id);
   
// Disassociate memory objects from PDC container objects
    PDCobj_unmap(p_obj_id);
    PDCobj_unmap(q_obj_id);
    
 
// Release memory buffers from one of the memory objects
// (The buffer for Q will be released by PDC when it's closed)
    PDCobj_release(p_obj_id);

// Close objects & container
    PDCobj_close(p_obj_id);
    PDCobj_close(q_obj_id);
    PDCobj_close(x_obj_id);
    PDCobj_close(y_obj_id);
    PDCobj_close(z_obj_id);
    PDCcont_close(vpic_container_id);

// Re-open same container
    pid_t vpic_id = PDCcont_open(pdc_id, "vpic.pdc");
    PDCcont_close(vpic_id);
}
