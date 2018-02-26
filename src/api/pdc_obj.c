/*
 * Copyright Notice for 
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***
 
 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.
 
 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.
 
 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include "config.h"
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include "../server/utlist.h"
#include "pdc_obj.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_client_server_common.h"
#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

static perr_t PDCobj__close(struct PDC_obj_info *op);

static perr_t PDCregion__close(struct PDC_region_info *op);

/* PDC object ID class */
/*
static const PDCID_class_t PDC_OBJ_CLS[1] = {{
    PDC_OBJ,
    0,
    0,
    (PDC_free_t)PDCobj__close
}};
*/
/* PDC region ID class */
/*
static const PDCID_class_t PDC_REGION_CLS[1] = {{
    PDC_REGION,
    0,
    0,
    (PDC_free_t)PDCregion__close        
}};
*/

perr_t PDCobj_init()
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the object IDs */
    if(PDC_register_type(PDC_OBJ, (PDC_free_t)PDCobj__close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize object interface");

done:
    FUNC_LEAVE(ret_value);
} /* end PDCobj_init() */

perr_t PDCregion_init()
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* Initialize the atom group for the region IDs */
    if(PDC_register_type(PDC_REGION, (PDC_free_t)PDCregion__close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize region interface");
    
done:
    FUNC_LEAVE(ret_value);
} /* end PDCregion_init() */

pdcid_t PDCobj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_obj_info *p = NULL;
    struct PDC_id_info *id_info = NULL;
    perr_t ret;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct PDC_obj_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC object memory allocation failed\n");
    p->name = strdup(obj_name);
    p->region_list_head = NULL;

    id_info = PDC_find_id(cont_id);
    p->cont = (struct PDC_cont_info *)(id_info->obj_ptr);
    id_info = PDC_find_id(obj_prop_id);
    p->obj_pt = (struct PDC_obj_prop *)(id_info->obj_ptr);
    p->client_id = 0;

    ret = PDC_Client_send_name_recv_id(obj_name, obj_prop_id, &(p->meta_id));
    if (ret == FAIL) {
        ret_value = -1;
        PGOTO_ERROR(FAIL,"Unable to create object on server!\n");
    }

    ret = PDC_Client_add_del_objects_to_container(1, &(p->meta_id), p->cont->meta_id, ADD_OBJ);
    if (ret == FAIL) {
        ret_value = -1;
        PGOTO_ERROR(FAIL,"Unable to put object to container!\n");
    }

    p->local_id = PDC_id_register(PDC_OBJ, p);

    ret_value = p->local_id;

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCobj_create_MPI(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_obj_info *p = NULL;
    struct PDC_id_info *id_info = NULL;
    int  rank;
    char name[512];
    char srank[10];
    perr_t ret;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct PDC_obj_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC object memory allocation failed\n");
    p->name = strdup(obj_name);
    p->region_list_head = NULL;

    id_info = PDC_find_id(cont_id);
    p->cont = (struct PDC_cont_info *)(id_info->obj_ptr);
    id_info = PDC_find_id(obj_prop_id);
    p->obj_pt = (struct PDC_obj_prop *)(id_info->obj_ptr);

#ifdef ENABLE_MPI
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    sprintf(srank, "%d", rank);
    sprintf(name, "%s%s", obj_name, srank);
    p->client_id = rank;
    ret = PDC_Client_send_name_recv_id(name, obj_prop_id, &(p->meta_id));
#else
    p->client_id = 0;
    ret = PDC_Client_send_name_recv_id(obj_name, obj_prop_id, &(p->meta_id));
#endif
    if (ret == FAIL) {
        ret_value = -1;
        PGOTO_ERROR(FAIL,"Unable to create object on server!\n");
    }

    p->local_id = PDC_id_register(PDC_OBJ, p);

    ret_value = p->local_id;

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCobj_create_(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id, PDCobj_location location)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_obj_info *p = NULL;
    struct PDC_id_info *id_info = NULL;
    int rank;
    perr_t ret = SUCCEED;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct PDC_obj_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC object memory allocation failed\n");
    p->name = strdup(obj_name);
    p->location = location;
    p->region_list_head = NULL;

    id_info = PDC_find_id(cont_id);
    p->cont = (struct PDC_cont_info *)(id_info->obj_ptr);
    id_info = PDC_find_id(obj_prop_id);
    p->obj_pt = (struct PDC_obj_prop *)(id_info->obj_ptr);
    p->meta_id = 0;
    p->local_id = PDC_id_register(PDC_OBJ, p);

    if(location == PDC_OBJ_GLOBAL) {
#ifdef ENABLE_MPI
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        p->client_id = rank;
        if(rank == 0)
            ret = PDC_Client_send_name_recv_id(obj_name, obj_prop_id, &(p->meta_id));
#else
        p->client_id = 0;
        ret = PDC_Client_send_name_recv_id(obj_name, obj_prop_id, &(p->meta_id));
#endif
        if (ret == FAIL) {
            ret_value = -1;
            PGOTO_ERROR(FAIL,"Unable to create object on server!\n");
        }
    }
    ret_value = p->local_id;

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCobj_create__(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id, PDCobj_location location)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_obj_info *p = NULL;
    struct PDC_id_info *id_info = NULL;
    perr_t ret = SUCCEED;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct PDC_obj_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC object memory allocation failed\n");
    p->name = strdup(obj_name);
    p->location = location;
    p->region_list_head = NULL;

    id_info = PDC_find_id(cont_id);
    p->cont = (struct PDC_cont_info *)(id_info->obj_ptr);
    id_info = PDC_find_id(obj_prop_id);
    p->obj_pt = (struct PDC_obj_prop *)(id_info->obj_ptr);
    p->meta_id = 0;
    p->local_id = PDC_id_register(PDC_OBJ, p);

    if(location == PDC_OBJ_GLOBAL) {
        ret = PDC_Client_send_name_recv_id(obj_name, obj_prop_id, &(p->meta_id));
        p->client_id = 0;
    }
    if (ret == FAIL) {
        ret_value = -1;
        PGOTO_ERROR(FAIL,"Unable to create object on server!\n");
    }
    ret_value = p->local_id;

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCobj_create_mpi(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id, int rank_id)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_obj_info *p = NULL;
    struct PDC_id_info *id_info = NULL;
    int rank;
    perr_t ret = SUCCEED;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif
    if(rank == rank_id) {
        ret_value = PDCobj_create__(cont_id, obj_name, obj_prop_id, PDC_OBJ_GLOBAL);
    }
    else
        ret_value = PDCobj_create__(cont_id, obj_name, obj_prop_id, PDC_OBJ_LOCAL);

    id_info = PDC_find_id(ret_value);
    p = (struct PDC_obj_info *)(id_info->obj_ptr);
    p->client_id = rank;

#ifdef ENABLE_MPI
    MPI_Bcast(&(p->meta_id), 1, MPI_LONG_LONG, rank_id, MPI_COMM_WORLD);
#endif

    FUNC_LEAVE(ret_value);
}

pdcid_t PDCobj_create_mpi2(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_obj_info *p = NULL;
    struct PDC_id_info *id_info = NULL;
    int  rank;
    perr_t ret = SUCCEED;
    
    FUNC_ENTER(NULL);
    
    p = PDC_MALLOC(struct PDC_obj_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC object memory allocation failed\n");
    p->name = strdup(obj_name);
    p->region_list_head = NULL;
    
    id_info = PDC_find_id(cont_id);
    p->cont = (struct PDC_cont_info *)(id_info->obj_ptr);
    id_info = PDC_find_id(obj_prop_id);
    p->obj_pt = (struct PDC_obj_prop *)(id_info->obj_ptr);

#ifdef ENABLE_MPI
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    p->client_id = rank;
    if(rank == 0)
        ret = PDC_Client_send_name_recv_id(obj_name, obj_prop_id, &(p->meta_id));
#else
    p->client_id = 0;
    ret = PDC_Client_send_name_recv_id(obj_name, obj_prop_id, &(p->meta_id));
#endif
    if (ret == FAIL) {
        ret_value = -1;
        PGOTO_ERROR(FAIL,"Unable to create object on server!\n");
    }

    p->local_id = PDC_id_register(PDC_OBJ, p);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Bcast(&(p->meta_id), 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    ret_value = p->local_id;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_obj_list_null()
{
    perr_t ret_value = SUCCEED;   /* Return value */
    int nelemts;
    
    FUNC_ENTER(NULL);
    
    // list is not empty
    nelemts = PDC_id_list_null(PDC_OBJ);
    if(nelemts > 0) {
        /* printf("%d element(s) in the object list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_OBJ) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_region_list_null()
{
    perr_t ret_value = SUCCEED;   /* Return value */
    int nelemts;
    
    FUNC_ENTER(NULL);
    // list is not empty
    nelemts = PDC_id_list_null(PDC_REGION);
    if(nelemts > 0) {
        /* printf("%d element(s) in the region list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_REGION) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj__close(struct PDC_obj_info *op)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
   
    free((void*)(op->name));
    op = PDC_FREE(struct PDC_obj_info, op);
    
    FUNC_LEAVE(ret_value);
}

perr_t PDCregion__close(struct PDC_region_info *op)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    free(op->size);
    free(op->offset);
    if(op->obj!=NULL)
        op->obj = PDC_FREE(struct PDC_obj_info, op->obj);
    op = PDC_FREE(struct PDC_region_info, op);
    
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_close(pdcid_t obj_id)
{
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(obj_id) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCregion_close(pdcid_t region_id)
{
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(region_id) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_end()
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(PDC_destroy_type(PDC_OBJ) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy object interface");
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_end() */

perr_t PDCregion_end(pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    if(PDC_destroy_type(PDC_REGION) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy region interface");
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCregion_end() */

pdcid_t PDCobj_open(const char *obj_name)
{
    pdcid_t ret_value = SUCCEED;
    pdcid_t obj_id;
    
    FUNC_ENTER(NULL);
    
    // should wait for response from server
    // look up in the list for now
    obj_id = PDC_find_byname(PDC_OBJ, obj_name);
    if(obj_id <= 0)
        PGOTO_ERROR(FAIL, "cannot locate object");
    PDC_inc_ref(obj_id);
    ret_value = obj_id;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_open() */

obj_handle *PDCobj_iter_start(pdcid_t cont_id)
{
    obj_handle *ret_value = NULL;
    obj_handle *objhl = NULL;
    struct PDC_id_type *type_ptr;
    
    FUNC_ENTER(NULL);
    
    type_ptr  = (pdc_id_list_g->PDC_id_type_list_g)[PDC_OBJ];
    if(type_ptr == NULL)
        PGOTO_ERROR(NULL, "object list is empty");
    objhl = (&type_ptr->ids)->head;
    
    while(objhl!=NULL && ((struct PDC_obj_info *)(objhl->obj_ptr))->cont->local_id!=cont_id) {
        objhl = PDC_LIST_NEXT(objhl, entry);
    }
    
    ret_value = objhl;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_start() */

pbool_t PDCobj_iter_null(obj_handle *ohandle)
{
    pbool_t ret_value = FALSE;
    
    FUNC_ENTER(NULL);
    
    if(ohandle == NULL)
        ret_value = TRUE;
    
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_null() */

obj_handle *PDCobj_iter_next(obj_handle *ohandle, pdcid_t cont_id)
{
    obj_handle *ret_value = NULL;
    obj_handle *next = NULL;
    
    FUNC_ENTER(NULL);
    
    if(ohandle == NULL)
        PGOTO_ERROR(NULL, "no next object");
    next = PDC_LIST_NEXT(ohandle, entry);
    
    while(next!=NULL && ((struct PDC_obj_info *)(next->obj_ptr))->cont->local_id!=cont_id) {
        next = PDC_LIST_NEXT(ohandle, entry);
    }
   
    ret_value = next;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_next() */

struct PDC_obj_info *PDCobj_iter_get_info(obj_handle *ohandle)
{
    struct PDC_obj_info *ret_value = NULL;
    struct PDC_obj_info *info = NULL;
    
    FUNC_ENTER(NULL);
    info = (struct PDC_obj_info *)(ohandle->obj_ptr);
    if(info == NULL)
        PGOTO_ERROR(NULL, "PDC container info memory allocation failed");
    
    ret_value = info;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_lifetime(pdcid_t obj_prop, PDC_lifetime obj_lifetime)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->obj_life = obj_lifetime;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->user_id = user_id;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->app_name = strdup(app_name);
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->time_step = time_step;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_data_loc(pdcid_t obj_prop, char *loc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->data_loc = strdup(loc);
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->tags = strdup(tags);
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    prop->ndim = ndim;
    prop->dims = (uint64_t *)malloc(ndim * sizeof(uint64_t));
    
    int i = 0;
    for(i=0; i<ndim; i++)
        (prop->dims)[i] = dims[i];
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_type(pdcid_t obj_prop, PDC_var_type_t type)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    prop->type = type;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    prop->buf = buf;
    
done:
    FUNC_LEAVE(ret_value);
}

void **PDCobj_buf_retrieve(pdcid_t obj_id)
{
    void **ret_value = NULL;         /* Return value */
    struct PDC_id_info *info;
    struct PDC_obj_info *object;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_id);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object = (struct PDC_obj_info *)(info->obj_ptr);
    void **buffer = &(object->obj_pt->buf);
    ret_value = buffer;
    
done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCregion_create(size_t ndims, uint64_t *offset, uint64_t *size)
{
    pdcid_t ret_value = SUCCEED;         /* Return value */
    struct PDC_region_info *p = NULL;
    pdcid_t new_id;
    size_t i = 0;
 
    FUNC_ENTER(NULL);
    
    p = PDC_MALLOC(struct PDC_region_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC region memory allocation failed\n");
    p->ndim = ndims;
    p->obj = NULL;
    p->offset = (uint64_t *)malloc(ndims * sizeof(uint64_t));
    p->size = (uint64_t *)malloc(ndims * sizeof(uint64_t));
    p->mapping = 0;
    p->local_id = 0;
    for(i=0; i<ndims; i++) {
        (p->offset)[i] = offset[i];
        (p->size)[i] = size[i];
    }
    // data type?
    new_id = PDC_id_register(PDC_REGION, p);
    p->local_id = new_id;
    ret_value = new_id;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_map(pdcid_t local_obj, pdcid_t local_reg, pdcid_t remote_obj, pdcid_t remote_reg)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    size_t i;
    struct PDC_id_info *objinfo1, *objinfo2;
    struct PDC_obj_info *obj1, *obj2;
    pdcid_t local_meta_id, remote_meta_id;

    PDC_var_type_t local_type, remote_type;
    void *local_data, *remote_data;
    struct PDC_id_info *reginfo1, *reginfo2;
    struct PDC_region_info *reg1, *reg2;
    size_t ndim;
    int32_t remote_client_id;
    
    FUNC_ENTER(NULL);
    
    // PDC_obj_info defined in pdc_obj_pkg.h
    // PDC_region_info defined in pdc_obj_pkg.h
    
    objinfo1 = PDC_find_id(local_obj);
    if(objinfo1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate local object ID");
    obj1 = (struct PDC_obj_info *)(objinfo1->obj_ptr);
    local_meta_id = obj1->meta_id;
    local_type = obj1->obj_pt->type;
    local_data = obj1->obj_pt->buf;
    
    reginfo1 = PDC_find_id(local_reg);
    reg1 = (struct PDC_region_info *)(reginfo1->obj_ptr);
    if(obj1->obj_pt->ndim != reg1->ndim)
        PGOTO_ERROR(FAIL, "local object dimension and region dimension does not match");
    for(i=0; i<reg1->ndim; i++)
        if((obj1->obj_pt->dims)[i] < ((reg1->size)[i] + (reg1->offset)[i]))
            PGOTO_ERROR(FAIL, "local object region size error");

    objinfo2 = PDC_find_id(remote_obj);
    if(objinfo2 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate remote object ID");
    obj2 = (struct PDC_obj_info *)(objinfo2->obj_ptr);
    remote_meta_id = obj2->meta_id;
    remote_client_id = obj2->client_id;
    remote_type = obj2->obj_pt->type;
    remote_data = obj2->obj_pt->buf;
  
    reginfo2 = PDC_find_id(remote_reg);
    reg2 = (struct PDC_region_info *)(reginfo2->obj_ptr);
    if(obj2->obj_pt->ndim != reg2->ndim)
        PGOTO_ERROR(FAIL, "remote object dimension and region dimension does not match");
    for(i=0; i<reg2->ndim; i++)
        if((obj2->obj_pt->dims)[i] < ((reg2->size)[i] + (reg2->offset)[i]))
            PGOTO_ERROR(FAIL, "remote object region size error");

    // TODO: assume ndim is the same
    ndim = reg1->ndim;
    
    //TODO: assume type is the same
    // start mapping
    ret_value = PDC_Client_region_map(local_meta_id, local_reg, remote_meta_id, remote_reg, ndim, obj1->obj_pt->dims, reg1->offset, reg1->size, local_type, local_data, obj2->obj_pt->dims, reg2->offset, reg2->size, remote_type, remote_client_id, remote_data, reg1, reg2);
    if(ret_value == SUCCEED) {
        // state in origin obj that there is mapping
//        obj1->mapping = 1;
        reg1->mapping = 1;
        PDC_inc_ref(local_obj);
        PDC_inc_ref(local_reg);
        // update region map list
        struct region_map_list *new_map = malloc(sizeof(struct region_map_list));
        new_map->orig_reg_id = local_reg;
        new_map->des_obj_id  = remote_obj;
        new_map->des_reg_id  = remote_reg;
        DL_APPEND(obj1->region_list_head, new_map);
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_buf_map(void *buf, PDC_var_type_t local_type, pdcid_t local_reg, pdcid_t remote_obj, pdcid_t remote_reg)
{
    pdcid_t ret_value = SUCCEED;    
    size_t i;
    struct PDC_id_info *id_info = NULL, *objinfo2;
    struct PDC_obj_info *obj2;
    pdcid_t remote_meta_id;

    PDC_var_type_t remote_type;
    void *remote_data;
    struct PDC_id_info *reginfo1, *reginfo2;
    struct PDC_region_info *reg1, *reg2;
    int32_t remote_client_id;
    
    FUNC_ENTER(NULL);

    reginfo1 = PDC_find_id(local_reg);
    reg1 = (struct PDC_region_info *)(reginfo1->obj_ptr);

    objinfo2 = PDC_find_id(remote_obj);
    if(objinfo2 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate remote object ID");
    obj2 = (struct PDC_obj_info *)(objinfo2->obj_ptr);
    remote_meta_id = obj2->meta_id;
    remote_client_id = obj2->client_id;
    remote_type = obj2->obj_pt->type;
    remote_data = obj2->obj_pt->buf;
  
    reginfo2 = PDC_find_id(remote_reg);
    reg2 = (struct PDC_region_info *)(reginfo2->obj_ptr);
    if(obj2->obj_pt->ndim != reg2->ndim)
        PGOTO_ERROR(FAIL, "remote object dimension and region dimension does not match");
    for(i=0; i<reg2->ndim; i++)
        if((obj2->obj_pt->dims)[i] < ((reg2->size)[i] + (reg2->offset)[i]))
            PGOTO_ERROR(FAIL, "remote object region size error");

    ret_value = PDC_Client_buf_map(local_reg, remote_meta_id, remote_reg, reg1->ndim, reg1->size, reg1->offset, reg1->size, local_type, buf, obj2->obj_pt->dims, reg2->offset, reg2->size, remote_type, remote_client_id, remote_data, reg1, reg2);

    if(ret_value == SUCCEED) {
        PDC_inc_ref(remote_obj);
        PDC_inc_ref(remote_reg);
    }
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_encode(pdcid_t obj_id, pdcid_t *meta_id)
{
    struct PDC_id_info *objinfo;
    struct PDC_obj_info *obj;
    perr_t ret_value = FAIL;
    
#ifdef ENABLE_MPI
    int client_rank, client_size;
    
    MPI_Comm_size(MPI_COMM_WORLD, &client_size);
    if (client_size < 2) {
        PGOTO_ERROR(ret_value, "Requires at least two processes.");
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &client_rank);
    
    if(client_rank == 0) {
        objinfo = PDC_find_id(obj_id);
        if(objinfo == NULL)
            PGOTO_ERROR(ret_value, "cannot locate object ID");
        obj = (struct PDC_obj_info *)(objinfo->obj_ptr);
        if(obj->location == PDC_OBJ_LOCAL)
            PGOTO_ERROR(FAIL, "trying to encode local object");
        *meta_id = obj->meta_id;
    }
#endif
done:
    FUNC_LEAVE(ret_value);    
}

pdcid_t PDCobj_decode(pdcid_t obj_id, pdcid_t meta_id) 
{
    pdcid_t ret_value = -1;
    struct PDC_id_info *objinfo;
    struct PDC_obj_info *obj;

#ifdef ENABLE_MPI
    int client_rank, client_size;
    MPI_Comm_size(MPI_COMM_WORLD, &client_size);
    if (client_size < 2) {
        PGOTO_ERROR(ret_value, "Requires at least two processes.");
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &client_rank);
    if(client_rank != 0) {
        objinfo = PDC_find_id(obj_id);
        if(objinfo == NULL)
            PGOTO_ERROR(ret_value, "cannot locate object ID");
        obj = (struct PDC_obj_info *)(objinfo->obj_ptr);
        obj->meta_id = meta_id;
    }
#endif
done:
    FUNC_LEAVE(ret_value);
}

static struct PDC_region_info *PDCregion_get_info(pdcid_t reg_id)
{
    struct PDC_region_info *ret_value = NULL;
    struct PDC_region_info *info =  NULL;
    struct PDC_id_info *region;
    
    FUNC_ENTER(NULL);
    
    region = PDC_find_id(reg_id);
    if(region == NULL)
        PGOTO_ERROR(NULL, "cannot locate region");
    
    info = (struct PDC_region_info *)(region->obj_ptr);
    ret_value = info;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCregion_get_info() */

perr_t PDCobj_buf_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info1;
    struct PDC_obj_info *object1;
    struct PDC_region_info *reginfo;
    PDC_var_type_t data_type;

    FUNC_ENTER(NULL);

    info1 = PDC_find_id(remote_obj_id);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object1 = (struct PDC_obj_info *)(info1->obj_ptr);
    data_type = object1->obj_pt->type;

    info1 = PDC_find_id(remote_reg_id);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate region ID");
    reginfo = (struct PDC_region_info *)(info1->obj_ptr);

    ret_value = PDC_Client_buf_unmap(object1->meta_id, remote_reg_id, reginfo, data_type);

    if(ret_value == SUCCEED) { 
        PDC_dec_ref(remote_obj_id);  
        PDC_dec_ref(remote_reg_id); 
    } 
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_unmap(pdcid_t obj_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info1;
    struct PDC_obj_info *object1;
    struct PDC_region_info *reginfo;

    FUNC_ENTER(NULL);

    info1 = PDC_find_id(obj_id);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object1 = (struct PDC_obj_info *)(info1->obj_ptr);
    ret_value = PDC_Client_object_unmap(object1->meta_id);
//    object1->mapping = 0;
    if(ret_value == SUCCEED) {
        struct region_map_list *elt, *tmp;
        DL_FOREACH_SAFE(object1->region_list_head, elt, tmp){
            PDC_dec_ref(obj_id);
            if(PDC_dec_ref(elt->orig_reg_id) == 1) {
                reginfo = PDCregion_get_info(elt->orig_reg_id);
                reginfo->mapping = 0;
            }
            DL_DELETE(object1->region_list_head, elt);
            elt = PDC_FREE(struct region_map_list, elt);
        }
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCreg_unmap(pdcid_t obj_id, pdcid_t reg_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info1;
    struct PDC_obj_info *object1;
    struct PDC_region_info *reginfo;
    PDC_var_type_t data_type;
 
    FUNC_ENTER(NULL);

    info1 = PDC_find_id(obj_id);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object1 = (struct PDC_obj_info *)(info1->obj_ptr);
    data_type = object1->obj_pt->type;
    info1 = PDC_find_id(reg_id);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate region ID");
    reginfo = (struct PDC_region_info *)(info1->obj_ptr);
    ret_value = PDC_Client_region_unmap(object1->meta_id, reg_id, reginfo, data_type);
    if(ret_value == SUCCEED) {
        PDC_dec_ref(obj_id);
        if(PDC_dec_ref(reg_id) == 1) {
            reginfo = PDCregion_get_info(reg_id);
            reginfo->mapping = 0;
        }
        // delete the map info from obj map list
        struct region_map_list *elt, *tmp;
        DL_FOREACH_SAFE(object1->region_list_head, elt, tmp) {
            if(elt->orig_reg_id==reg_id) {
                DL_DELETE(object1->region_list_head, elt);
                elt = PDC_FREE(struct region_map_list, elt);
            }
        }
    }
done:
    FUNC_LEAVE(ret_value);
}

static struct PDC_obj_info *PDCobj_get_info(pdcid_t obj_id)
{
    struct PDC_obj_info *ret_value = NULL;
    struct PDC_obj_info *info =  NULL;
    struct PDC_id_info *obj;
    
    FUNC_ENTER(NULL);
    
    obj = PDC_find_id(obj_id);
    if(obj == NULL)
        PGOTO_ERROR(NULL, "cannot locate object");
    
    info = (struct PDC_obj_info *)(obj->obj_ptr);
    ret_value = info;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_get_info() */

perr_t PDCobj_release(pdcid_t obj_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = PDC_find_id(obj_id);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    ((struct PDC_obj_info *)(info->obj_ptr))->obj_pt->buf = NULL;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCreg_obtain_lock(pdcid_t obj_id, pdcid_t reg_id, PDC_access_t access_type, PDC_lock_mode_t lock_mode)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    pdcid_t meta_id;
    struct PDC_obj_info *object_info;
    struct PDC_region_info *region_info;
    PDC_var_type_t data_type;
    pbool_t obtained;
    
    FUNC_ENTER(NULL);
    
    object_info = PDCobj_get_info(obj_id);
    meta_id = object_info->meta_id;
    data_type = object_info->obj_pt->type;
    region_info = PDCregion_get_info(reg_id);
    
    ret_value = PDC_Client_obtain_region_lock(meta_id, region_info, access_type, lock_mode, data_type, &obtained);

    FUNC_LEAVE(ret_value);
}

perr_t PDCreg_release_lock(pdcid_t obj_id, pdcid_t reg_id, PDC_access_t access_type)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    pbool_t released;
    pdcid_t meta_id;
    struct PDC_obj_info *object_info;
    struct PDC_region_info *region_info;
    PDC_var_type_t data_type;
 
    FUNC_ENTER(NULL);
    
    object_info = PDCobj_get_info(obj_id);
    meta_id = object_info->meta_id;
    data_type = object_info->obj_pt->type;
    region_info = PDCregion_get_info(reg_id);
    
    ret_value = PDC_Client_release_region_lock(meta_id, region_info, access_type, data_type, &released);
 
    FUNC_LEAVE(ret_value);
}
