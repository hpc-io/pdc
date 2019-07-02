/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	The PDC VOL plugin where access is forwarded to the PDC
 * library 
 */
#include "H5VLpdc.h"            /* PDC plugin                         */
#include "H5PLextern.h"
#include "H5VLerror.h"

/* External headers needed by this file */
#include "H5linkedlist.h"
#include "pdc.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/****************/
/* Local Macros */
/****************/

#define ADDR_MAX                            256
#define H5VL_PDC_SEQ_LIST_LEN               128

/* Remove warnings when connector does not use callback arguments */
#if defined(__cplusplus)
# define H5VL_ATTR_UNUSED
#elif defined(__GNUC__) && (__GNUC__ >= 4)
# define H5VL_ATTR_UNUSED __attribute__((unused))
#else
# define H5VL_ATTR_UNUSED
#endif

/************************************/
/* Local Type and Struct Definition */
/************************************/

/* Common object and attribute information */
typedef struct H5VL_pdc_item_t {
    H5I_type_t                 type;
    struct H5VL_pdc_file_t    *file;
} H5VL_pdc_item_t;

/* Common object information */
typedef struct H5VL_pdc_obj_t {
    H5VL_pdc_item_t     item; /* Must be first */
    char                obj_name[ADDR_MAX];
    pdcid_t             obj_id;
    PDC_var_type_t      type;
    pdcid_t             reg_id_from;
    pdcid_t             reg_id_to;
} H5VL_pdc_obj_t;

/* The file struct */
typedef struct H5VL_pdc_file_t {
    H5VL_pdc_item_t     item; /* Must be first */
    char               *file_name;
    hid_t               fcpl_id;
    hid_t               fapl_id;
    MPI_Comm            comm;
    MPI_Info            info;
    int                 my_rank;
    int                 num_procs;
    pdcid_t             cont_id;
    int                 nobj;
    H5_LIST_HEAD(H5VL_pdc_dset_t)  ids;
} H5VL_pdc_file_t;

/* The dataset struct */
typedef struct H5VL_pdc_dset_t {
    H5VL_pdc_obj_t       obj; /* Must be first */
    hid_t                type_id;
    hid_t                space_id;
    hid_t                dcpl_id;
    hid_t                dapl_id;
    hbool_t              mapped;
    H5_LIST_ENTRY(H5VL_pdc_dset_t) entry;
} H5VL_pdc_dset_t;

/* PDC-specific file access properties */
typedef struct H5VL_pdc_info_t {
    MPI_Comm            comm;           /*communicator                  */
    MPI_Info            info;           /*file information              */
} H5VL_pdc_info_t;

/********************/
/* Local Prototypes */
/********************/

/* "Management" callbacks */
static herr_t H5VL_pdc_init(hid_t vipl_id);
static herr_t H5VL_pdc_term(void);

/* VOL info callbacks */
static void *H5VL_pdc_info_copy(const void *_old_info);
static herr_t H5VL_pdc_info_cmp(int *cmp_value, const void *_info1, const void *_info2);
static herr_t H5VL_pdc_info_free(void *info);

/* File callbacks */
static void *H5VL_pdc_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_pdc_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pdc_file_close(void *file, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_pdc_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_pdc_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_pdc_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_pdc_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_pdc_dataset_close(void *dset, hid_t dxpl_id, void **req);

/*******************/
/* Local variables */
/*******************/

/* The PDC VOL plugin struct */
static const H5VL_class_t H5VL_pdc_g = {
    H5VL_PDC_VERSION_MAJOR,                         /* version */
    H5VL_PDC_VALUE,                                 /* value */
    H5VL_PDC_NAME_STRING,                           /* name */
    0,                                              /* capability flags */
    H5VL_pdc_init,                                  /* initialize */
    H5VL_pdc_term,                                  /* terminate */
    {                                           /* info_cls */
        sizeof(H5VL_pdc_info_t),                    /* info size */
        H5VL_pdc_info_copy,                         /* info copy */
        NULL,                                       /* info compare */
        H5VL_pdc_info_free,                         /* info free */
        NULL,                                       /* to_str */
        NULL,                                       /* from_str */
    },
    {                                           /* wrap_cls */
        NULL,                                       /* get_object */
        NULL,                                       /* get_wrap_ctx */
        NULL,                                       /* wrap_object */
        NULL,                                       /* unwrap_object */
        NULL,                                       /* free_wrap_ctx */
    },
    {                                           /* attribute_cls */
        NULL,                                       /* create */
        NULL,                                       /* open */
        NULL,                                       /* read */
        NULL,                                       /* write */
        NULL,                                       /* get */
        NULL,                                       /* specific */
        NULL,                                       /* optional */
        NULL                                        /* close */
    },
    {                                           /* dataset_cls  */
        H5VL_pdc_dataset_create,                    /* create */
        H5VL_pdc_dataset_open,                      /* open */
        H5VL_pdc_dataset_read,                      /* read */
        H5VL_pdc_dataset_write,                     /* write */
        NULL,                                       /* get */
        NULL,                                       /* specific */
        NULL,                                       /* optional */
        H5VL_pdc_dataset_close                      /* close */
    },
    {                                           /* datatype_cls */
        NULL,                                       /* commit */
        NULL,                                       /* open */
        NULL,                                       /* get */
        NULL,                                       /* specific */
        NULL,                                       /* optional */
        NULL                                        /* close */
    },
    {                                           /* file_cls     */
        H5VL_pdc_file_create,                       /* create */
        H5VL_pdc_file_open,                         /* open */
        NULL,                                       /* get */
        NULL,                                       /* specific */
        NULL,                                       /* optional */
        H5VL_pdc_file_close                         /* close */
    },
    {                                           /* group_cls    */
        NULL,                                       /* create */
        NULL,                                       /* open */
        NULL,                                       /* get */
        NULL,                                       /* specific */
        NULL,                                       /* optional */
        NULL                                        /* close */
    },
    {                                           /* link_cls     */
        NULL,                                       /* create */
        NULL,                                       /* copy */
        NULL,                                       /* move */
        NULL,                                       /* get */
        NULL,                                       /* specific */
        NULL                                        /* optional */
    },
    {                                           /* object_cls   */
        NULL,                                       /* open */
        NULL,                                       /* copy */
        NULL,                                       /* get */
        NULL,                                       /* specific */
        NULL                                        /* optional */
    },
    {                                           /* request_cls  */
        NULL,                                       /* wait */
        NULL,                                       /* notify */
        NULL,                                       /* cancel */
        NULL,                                       /* specific */
        NULL,                                       /* optional */
        NULL                                        /* free */
    },
    NULL                                         /* optional    */
};

/* The connector identification number, initialized at runtime */
static hid_t H5VL_PDC_g = H5I_INVALID_HID;
static hbool_t H5VL_pdc_init_g = FALSE;

/* Error stack declarations */
hid_t H5VL_ERR_STACK_g = H5I_INVALID_HID;
hid_t H5VL_ERR_CLS_g = H5I_INVALID_HID;

static pdcid_t pdc_id = 0;

/*---------------------------------------------------------------------------*/

/**
 * Public definitions.
 */

/*---------------------------------------------------------------------------*/
H5PL_type_t
H5PLget_plugin_type(void) {
    return H5PL_TYPE_VOL;
}

/*---------------------------------------------------------------------------*/
const void *
H5PLget_plugin_info(void) {
    return &H5VL_pdc_g;
}

/*---------------------------------------------------------------------------*/
herr_t
H5VLpdc_term(void)
{
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    /* Terminate the plugin */
    if(H5VL_pdc_term() < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CLOSEERROR, FAIL, "can't terminate PDC VOL connector");
    
done:
    FUNC_LEAVE_VOL
}

/*---------------------------------------------------------------------------*/
static herr_t
H5VL_pdc_init(hid_t H5VL_ATTR_UNUSED vipl_id)
{
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    /* Check whether initialized */
    if(H5VL_pdc_init_g)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "attempting to initialize connector twice");
    
    /* Create error stack */
    if((H5VL_ERR_STACK_g = H5Ecreate_stack()) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTCREATE, FAIL, "can't create error stack");
    
    /* Register error class for error reporting */
    if((H5VL_ERR_CLS_g = H5Eregister_class(H5VL_PDC_PACKAGE_NAME, H5VL_PDC_LIBRARY_NAME, H5VL_PDC_VERSION_STRING)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTREGISTER, FAIL, "can't register error class");
    
    /* Init PDC */
    pdc_id = PDC_init("pdc");
    if(pdc_id <= 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "could not initialize PDC");
    
    /* Initialized */
    H5VL_pdc_init_g = TRUE;
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_init() */

/*---------------------------------------------------------------------------*/
static herr_t
H5VL_pdc_term(void)
{
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    if(!H5VL_pdc_init_g)
        HGOTO_DONE(SUCCEED);
    
    /* "Forget" plugin id.  This should normally be called by the library
     * when it is closing the id, so no need to close it here. */
    H5VL_PDC_g = H5I_INVALID_HID;
    
    H5VL_pdc_init_g = FALSE;
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_term() */


/*---------------------------------------------------------------------------*/
static void *
H5VL_pdc_info_copy(const void *_old_info)
{
    const H5VL_pdc_info_t *old_info = (const H5VL_pdc_info_t *)_old_info;
    H5VL_pdc_info_t       *new_info = NULL;
    
    FUNC_ENTER_VOL(void *, NULL)
    
    if(NULL == (new_info = (H5VL_pdc_info_t *)malloc(sizeof(H5VL_pdc_info_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    new_info->comm = MPI_COMM_NULL;
    new_info->info = MPI_INFO_NULL;
    
    /* Duplicate communicator and Info object. */
    if(MPI_SUCCESS != MPI_Comm_dup(old_info->comm, &new_info->comm))
        HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "Communicator duplicate failed");
    if(MPI_SUCCESS != MPI_Comm_set_errhandler(new_info->comm, MPI_ERRORS_RETURN))
        HGOTO_ERROR(H5E_INTERNAL, H5E_CANTSET, NULL, "Cannot set MPI error handler");
    
    FUNC_RETURN_SET(new_info);
    
done:
    if(FUNC_ERRORED) {
        /* cleanup */
        if(new_info && H5VL_pdc_info_free(new_info) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTFREE, NULL, "can't free fapl");
    }
    
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_info_copy() */

/*---------------------------------------------------------------------------*/
static herr_t
H5VL_pdc_info_cmp(int *cmp_value, const void *_info1, const void *_info2)
{
    const H5VL_pdc_info_t *info1 = (const H5VL_pdc_info_t *)_info1;
    const H5VL_pdc_info_t *info2 = (const H5VL_pdc_info_t *)_info2;
    
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    assert(info1);
    assert(info2);
    
    *cmp_value = memcmp(info1, info2, sizeof(H5VL_pdc_info_t));
    
    FUNC_LEAVE_VOL
}

/*---------------------------------------------------------------------------*/
static herr_t
H5VL_pdc_info_free(void *_info)
{
    H5VL_pdc_info_t   *info = (H5VL_pdc_info_t *)_info;
    
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    assert(info);
    
    /* Free the internal communicator and INFO object */
    if (MPI_COMM_NULL != info->comm)
        MPI_Comm_free(&info->comm);
    if (MPI_INFO_NULL != info->info)
        MPI_Info_free(&info->info);
    
    /* free the struct */
    free(info);
    
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_info_free() */

/*---------------------------------------------------------------------------*/
static H5VL_pdc_file_t *
H5VL__pdc_file_init(const char *name, unsigned flags, H5VL_pdc_info_t *info)
{
    H5VL_pdc_file_t *file = NULL;
    
    FUNC_ENTER_VOL(void *, NULL)
    
    /* allocate the file object that is returned to the user */
    if(NULL == (file = malloc(sizeof(H5VL_pdc_file_t))))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "can't allocate PDC file struct");
    memset(file, 0, sizeof(H5VL_pdc_file_t));
    file->fcpl_id = FAIL;
    file->fapl_id = FAIL;
    file->info = MPI_INFO_NULL;
    file->comm = MPI_COMM_NULL;
    
    /* Fill in fields of file we know */
    file->item.type = H5I_FILE;
    file->item.file = file;
    if(NULL == (file->file_name = strdup(name)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't copy file name");
    
    /* Duplicate communicator and Info object. */
    if(info) {
        if(MPI_SUCCESS != MPI_Comm_dup(info->comm, &file->comm))
            HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "Communicator duplicate failed");
        if((MPI_INFO_NULL != info->info)
           && (MPI_SUCCESS != MPI_Info_dup(info->info, &file->info)))
            HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "Info duplicate failed");
    } else {
        if(MPI_SUCCESS != MPI_Comm_dup(MPI_COMM_WORLD, &file->comm))
            HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "Communicator duplicate failed");
    }
    
    /* Obtain the process rank and size from the communicator attached to the
     * fapl ID */
    MPI_Comm_rank(file->comm, &file->my_rank);
    MPI_Comm_size(file->comm, &file->num_procs);
    
    file->nobj = 0;
    
    PDC_LIST_INIT(&file->ids);
    
    FUNC_RETURN_SET((void *)file);
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL__pdc_file_init() */

/*---------------------------------------------------------------------------*/
static herr_t
H5VL__pdc_file_close(H5VL_pdc_file_t *file)
{
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    assert(file);
    
    /* Free file data structures */
    if(file->file_name)
        free(file->file_name);
    if(file->comm)
        MPI_Comm_free(&file->comm);
    
    if(file->fapl_id != FAIL && H5Idec_ref(file->fapl_id) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close plist");
    if(file->fcpl_id != FAIL && H5Idec_ref(file->fcpl_id) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close plist");
    
    free(file);
    file = NULL;
    
    FUNC_LEAVE_VOL
} /* end H5VL__pdc_file_close() */

/*---------------------------------------------------------------------------*/
static H5VL_pdc_dset_t *
H5VL__pdc_dset_init(H5VL_pdc_item_t *item)
{
    H5VL_pdc_dset_t *dset = NULL;
    
    FUNC_ENTER_VOL(void *, NULL)
    
    /* Allocate the dataset object that is returned to the user */
    if(NULL == (dset = malloc(sizeof(H5VL_pdc_dset_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate PDC dataset struct");
    memset(dset, 0, sizeof(H5VL_pdc_dset_t));
    
    dset->obj.item.type = H5I_DATASET;
    dset->obj.item.file = item->file;
    dset->obj.reg_id_from = 0;
    dset->obj.reg_id_to = 0;
    dset->mapped = 0;
    dset->type_id = FAIL;
    dset->space_id = FAIL;
    dset->dcpl_id = FAIL;
    dset->dapl_id = FAIL;
    
    /* Set return value */
    FUNC_RETURN_SET((void *)dset);
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL__pdc_dset_init() */

/*---------------------------------------------------------------------------*/
static herr_t
H5VL__pdc_dset_free(H5VL_pdc_dset_t *dset)
{
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    if(dset->space_id != FAIL && H5Idec_ref(dset->space_id) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close space");
    if(dset->dcpl_id != FAIL && H5Idec_ref(dset->dcpl_id) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close plist");
    if(dset->dapl_id != FAIL && H5Idec_ref(dset->dapl_id) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "failed to close plist");
    
    H5_LIST_REMOVE(dset, entry);
    free(dset);
    dset = NULL;
    
    FUNC_LEAVE_VOL
} /* end H5VL__pdc_dset_free() */

/*---------------------------------------------------------------------------*/
static herr_t
H5VL__pdc_sel_to_recx_iov(hid_t space_id, size_t type_size, uint64_t *off)
{
    hid_t sel_iter_id;                  /* Selection iteration info */
    hbool_t sel_iter_init = FALSE;      /* Selection iteration info has been initialized */
    size_t nseq;
    size_t nelem;
    size_t len[H5VL_PDC_SEQ_LIST_LEN];
    
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    /* Initialize selection iterator  */
    if((sel_iter_id = H5Ssel_iter_create(space_id, type_size, 0)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
    sel_iter_init = TRUE;       /* Selection iteration info has been initialized */
    
    /* Generate sequences from the file space until finished */
    do {
        /* Get the sequences of bytes */
        if(H5Ssel_iter_get_seq_list(sel_iter_id, (size_t)H5VL_PDC_SEQ_LIST_LEN, (size_t)-1, &nseq, &nelem, off, len) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed");
    } while(nseq == H5VL_PDC_SEQ_LIST_LEN);
    
done:
    /* Release selection iterator */
    if(sel_iter_init && H5Ssel_iter_close(sel_iter_id) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
    FUNC_LEAVE_VOL
} /* end H5VL__pdc_sel_to_recx_iov() */

/*---------------------------------------------------------------------------*/
void *H5VL_pdc_file_create(const char *name, unsigned flags,
                           hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pdc_info_t *info;
    H5VL_pdc_file_t *file = NULL;
    pdcid_t cont_prop;
    
    FUNC_ENTER_VOL(void *, NULL)
    
    assert(name);
    
    /* Get information from the FAPL */
    if(H5Pget_vol_info(fapl_id, (void **)&info) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get PDC info struct");
    
    /* Initialize file */
    if(NULL == (file = H5VL__pdc_file_init(name, flags, info)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't init PDC file struct");
    
    if((file->fcpl_id = H5Pcopy(fcpl_id)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fcpl");
    
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl");
    
    if((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id)) <= 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't create container property");
    if((file->cont_id = PDCcont_create_col(name, cont_prop)) <= 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't create container");
    
    if((PDCprop_close(cont_prop)) < 0)
       HGOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't close container property");
    
    /* Free info */
    if(info && H5VL_pdc_info_free(info) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info");
    
    FUNC_RETURN_SET((void *)file);
    
done:
    if(FUNC_ERRORED) {
        /* Free info */
        if(info)
            if(H5VL_pdc_info_free(info) < 0)
                HDONE_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info");
        
        /* Close file */
        if(file < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file");
    } /* end if */
    
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_file_create() */

/*---------------------------------------------------------------------------*/
void *H5VL_pdc_file_open(const char *name, unsigned flags,
                         hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pdc_info_t *info;
    H5VL_pdc_file_t *file = NULL;

    FUNC_ENTER_VOL(void *, NULL)
    
    /* Get information from the FAPL */
    if(H5Pget_vol_info(fapl_id, (void **)&info) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't get PDC info struct");
    
    /* Initialize file */
    if(NULL == (file = H5VL__pdc_file_init(name, flags, info)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't init PDC file struct");
    
    if((file->fapl_id = H5Pcopy(fapl_id)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, NULL, "failed to copy fapl");
        
    if((file->cont_id = PDCcont_open(name, pdc_id)) <= 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "failed to create container");
    
    /* Free info */
    if(info && H5VL_pdc_info_free(info) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info");
    
    FUNC_RETURN_SET((void *)file);
    
done:
    if(FUNC_ERRORED) {
        /* Free info */
        if(info)
            if(H5VL_pdc_info_free(info) < 0)
                HGOTO_ERROR(H5E_VOL, H5E_CANTFREE, NULL, "can't free connector info");
        if(file < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close file");
    } /* end if */
    
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_file_open() */

/*---------------------------------------------------------------------------*/
herr_t H5VL_pdc_file_close(void *_file, hid_t dxpl_id, void **req)
{
    H5VL_pdc_file_t *file = (H5VL_pdc_file_t *)_file;
    H5VL_pdc_dset_t *dset = NULL;
    perr_t ret;
    
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    assert(file);
    
    while(!H5_LIST_IS_EMPTY(&file->ids)) {
        H5_LIST_GET_FIRST(dset, &file->ids);
        H5_LIST_REMOVE(dset, entry);
        if(H5VL__pdc_dset_free(dset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "failed to free dataset");
    }
    
    if((ret = PDCcont_close(file->cont_id)) < 0 )
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "failed to close container");
        
    if((ret = PDC_close(pdc_id)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "failed to close PDC");
    
    /* Close the file */
    if(H5VL__pdc_file_close(file) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "failed to close file");
        
done:
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_file_close() */

/*---------------------------------------------------------------------------*/
static void *
H5VL_pdc_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_pdc_item_t *o = (H5VL_pdc_item_t *)obj;
    H5VL_pdc_dset_t *dset = NULL;
    pdcid_t obj_prop, obj_id;
    int ndim;
    hsize_t dims[H5S_MAX_RANK];
    
    FUNC_ENTER_VOL(void *, NULL)
    
    if(!obj)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "parent object is NULL");
    if(!loc_params)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");
    if(!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset name is NULL");
                
    /* Init dataset */
    if(NULL == (dset = H5VL__pdc_dset_init(o)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't init PDC dataset struct");
    
    /* Finish setting up dataset struct */
    if((dset->type_id = H5Tcopy(type_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy datatype");
    if((dset->space_id = H5Scopy(space_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dataspace");
    if(H5Sselect_all(dset->space_id) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, NULL, "can't change selection");
    if((dset->dcpl_id = H5Pcopy(dcpl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dcpl");
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dapl");
    
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    if(H5T_INTEGER == H5Tget_class(type_id)) {
        PDCprop_set_obj_type(obj_prop, PDC_INT);
        dset->obj.type = PDC_INT;
    }
    else if(H5T_FLOAT == H5Tget_class(type_id)) {
        PDCprop_set_obj_type(obj_prop, PDC_FLOAT);
        dset->obj.type = PDC_FLOAT;
    }
    else {
        dset->obj.type = PDC_UNKNOWN;
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, NULL, "datatype is not supported yet");
    }
    
    /* Get dataspace extent */
    if((ndim = H5Sget_simple_extent_ndims(space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get number of dimensions");
    if(ndim != H5Sget_simple_extent_dims(space_id, dims, NULL))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get dimensions");
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    
    /* Create PDC object */
    obj_id = PDCobj_create_mpi(o->file->cont_id, name, obj_prop, 0, o->file->comm);
    
    dset->obj.obj_id = obj_id;
    strcpy(dset->obj.obj_name, name);
    o->file->nobj++;
    H5_LIST_INSERT_HEAD(&o->file->ids, dset, entry);
    
    if((PDCprop_close(obj_prop)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, NULL, "can't close object property");
    
    /* Set return value */
    FUNC_RETURN_SET((void *)dset);
    
done:
    FUNC_LEAVE_VOL
}

/*---------------------------------------------------------------------------*/
static void *
H5VL_pdc_dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
                      const char *name, hid_t dapl_id, hid_t dxpl_id,
                      void **req)
{
    H5VL_pdc_item_t *o = (H5VL_pdc_item_t *)obj;
    H5VL_pdc_dset_t *dset = NULL;
    struct PDC_obj_info *obj_info;
    
    FUNC_ENTER_VOL(void *, NULL)
    
    if(!obj)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "parent object is NULL");
    if(!loc_params)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "location parameters object is NULL");
    if(!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "dataset name is NULL");
    
    /* Init dataset */
    if(NULL == (dset = H5VL__pdc_dset_init(o)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't init PDC dataset struct");
    if((dset->dapl_id = H5Pcopy(dapl_id)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, NULL, "failed to copy dapl");
    
    strcpy(dset->obj.obj_name, name);
    dset->obj.obj_id = PDCobj_open(name, pdc_id);
    obj_info = PDCobj_get_info(name);
    dset->obj.type = obj_info->obj_pt->type;
    o->file->nobj++;
    H5_LIST_INSERT_HEAD(&o->file->ids, dset, entry);
    
    /* Set return value */
    FUNC_RETURN_SET((void *)dset);
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_dataset_open() */

/*---------------------------------------------------------------------------*/
herr_t H5VL_pdc_dataset_write(void *_dset, hid_t mem_type_id,
                              hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf,
                              void **req)
{
    H5VL_pdc_dset_t *dset = (H5VL_pdc_dset_t *)_dset;
    uint64_t *offset;
    size_t type_size;
    int ndim;
    pdcid_t region_x, region_xx;
    hsize_t dims[H5S_MAX_RANK];
    perr_t ret;
    
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    /* Get memory dataspace object */
    if((ndim = H5Sget_simple_extent_ndims(mem_space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions");
    if(ndim != H5Sget_simple_extent_dims(mem_space_id, dims, NULL))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions");
            
    offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0] = 0;
    region_x = PDCregion_create(ndim, offset, dims);
    dset->obj.reg_id_from = region_x;
    
    type_size = H5Tget_size(mem_type_id);
    H5VL__pdc_sel_to_recx_iov(file_space_id, type_size, offset);
    
    region_xx = PDCregion_create(ndim, offset, dims);
    dset->obj.reg_id_to = region_xx;
    free(offset);
    
    dset->mapped = 1;
    
    if(PDC_INT == dset->obj.type)
        PDCbuf_obj_map((void *)buf, PDC_INT, region_x, dset->obj.obj_id, region_xx);
    else if(PDC_FLOAT == dset->obj.type)
        PDCbuf_obj_map((void *)buf, PDC_FLOAT, region_x, dset->obj.obj_id, region_xx);
    else
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "data type not supported");
        
    if((ret = PDCreg_obtain_lock(dset->obj.obj_id, region_xx, WRITE, NOBLOCK)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't obtain lock");

    if((ret = PDCreg_release_lock(dset->obj.obj_id, region_xx, WRITE)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't release lock");
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_dataset_write() */

/*---------------------------------------------------------------------------*/
herr_t H5VL_pdc_dataset_read(void *_dset, hid_t mem_type_id,
                             hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf,
                             void **req)
{
    H5VL_pdc_dset_t *dset = (H5VL_pdc_dset_t *)_dset;
    size_t type_size;
    uint64_t *offset;
    int ndim;
    pdcid_t region_x, region_xx;
    hsize_t dims[H5S_MAX_RANK];
    perr_t ret;
    
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    /* Get memory dataspace object */
    if((ndim = H5Sget_simple_extent_ndims(mem_space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions");
    if(ndim != H5Sget_simple_extent_dims(mem_space_id, dims, NULL))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions");
            
    offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0] = 0;
    region_x = PDCregion_create(ndim, offset, dims);
    dset->obj.reg_id_from = region_x;

    type_size = H5Tget_size(mem_type_id);
    H5VL__pdc_sel_to_recx_iov(file_space_id, type_size, offset);
    
    region_xx = PDCregion_create(ndim, offset, dims);
    dset->obj.reg_id_to = region_xx;
    free(offset);
    
    dset->mapped = 1;
    if(PDC_INT == dset->obj.type)
        PDCbuf_obj_map((void *)buf, PDC_INT, region_x, dset->obj.obj_id, region_xx);
    else if(PDC_FLOAT == dset->obj.type)
        PDCbuf_obj_map((void *)buf, PDC_FLOAT, region_x, dset->obj.obj_id, region_xx);
    else
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "data type not supported");
        
    if((ret = PDCreg_obtain_lock(dset->obj.obj_id, region_xx, READ, NOBLOCK)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't obtain lock");
    
    if((ret = PDCreg_release_lock(dset->obj.obj_id, region_xx, READ)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't release lock");
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_dataset_read() */

/*---------------------------------------------------------------------------*/
herr_t H5VL_pdc_dataset_close(void *_dset, hid_t dxpl_id, void **req)
{
    H5VL_pdc_dset_t *dset = (H5VL_pdc_dset_t *)_dset;
    perr_t ret;
    
    FUNC_ENTER_VOL(herr_t, SUCCEED)
    
    assert(dset);
    
    if(dset->mapped == 1) {
        if((ret = PDCbuf_obj_unmap(dset->obj.obj_id, dset->obj.reg_id_to)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't unmap object");
    }
    
    if((ret = PDCobj_close(dset->obj.obj_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close object");
    
    if(dset->obj.reg_id_from != 0) {
        if((ret = PDCregion_close(dset->obj.reg_id_from)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close region");
    }
    if(dset->obj.reg_id_to != 0) {
        if((ret = PDCregion_close(dset->obj.reg_id_to) < 0))
            HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close remote region");
    }
    
    if(dset->mapped == 1)
        H5_LIST_REMOVE(dset, entry);
    
    if(H5VL__pdc_dset_free(dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't free dataset");
    
done:
    FUNC_LEAVE_VOL
} /* end H5VL_pdc_dataset_close() */
