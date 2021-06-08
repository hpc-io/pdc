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

#ifndef PDC_CONT_H
#define PDC_CONT_H

#include "pdc_public.h"

typedef struct _pdc_id_info cont_handle;

/**************************/
/* Library Public Struct */
/**************************/
struct pdc_cont_info {
    char *   name;
    pdcid_t  local_id;
    uint64_t meta_id;
};

/*********************/
/* Public Prototypes */
/*********************/
/**
 * Create a container
 *
 * \param cont_name [IN]        Name of the container
 * \param cont_create_prop [IN] ID of container property
 *                              returned by PDCprop_create(PDC_CONT_CREATE)
 *
 * \return Container id on success/Zero on failure
 */
pdcid_t PDCcont_create(const char *cont_name, pdcid_t cont_create_prop);

/**
 * Create a container, used when all ranks are trying to create the same
   container
 *
 * \param cont_name [IN]        Name of the container
 * \param cont_prop_id [IN]     ID of container property
 *                              returned by PDCprop_create(PDC_CONT_CREATE)
 *
 * \return Container id on success/Zero on failure
 */
pdcid_t PDCcont_create_col(const char *cont_name, pdcid_t cont_prop_id);

/**
 * Open a container
 *
 * \param cont_name [IN]        Name of the container
 * \param pdc_id [IN]           ID of pdc
 *
 * \return Container id on success/Zero on failure
 */
pdcid_t PDCcont_open(const char *cont_name, pdcid_t pdc_id);

/**
 * Open a container collectively
 *
 * \param cont_name [IN]        Name of the container
 * \param pdc_id [IN]           ID of pdc
 *
 * \return Container id on success/Zero on failure
 */
pdcid_t PDCcont_open_col(const char *cont_name, pdcid_t pdc_id);

/**
 * Close a container
 *
 * \param cont_id [IN]          Container id, returned by
 *                              PDCcont_open(pdcid_t pdc_id, const char *cont_name)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_close(pdcid_t cont_id);

/**
 * Return a container property
 *
 * \param cont_name [IN]        Name of the container
 *
 * \return Container struct on success/NULL on failure
 */
struct pdc_cont_info *PDCcont_get_info(const char *cont_name);

/**
 * Persist a transient container
 *
 * \param cont_id [IN]          ID of the container, returned by
 *                              PDCcont_open(pdcid_t pdc_id, const char *cont_name)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_persist(pdcid_t cont_id);

/**
 * Set container lifetime
 *
 * \param cont_create_prop [IN] ID of container property, returned by
 *                              PDCprop_create(PDC_CONT_CREATE)
 * \param cont_lifetime [IN]    container lifetime (enum type), PDC_PERSIST or
 *                              PDC_TRANSIENT
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_cont_lifetime(pdcid_t cont_create_prop, pdc_lifetime_t cont_lifetime);

/**
 * Get container ID by its name
 *
 * \param cont_name [IN]        Container name
 * \param pdc_id [IN]           PDC ID
 *
 * \return Container ID
 */
pdcid_t PDCcont_get_id(const char *cont_name, pdcid_t pdc_id);

/**
 * Iterate over containers within a PDC
 *
 * \return Pointer to cont_handle struct/NULL on failure
 */
cont_handle *PDCcont_iter_start();

/**
 * Check if container handle is pointing to NULL
 *
 * \param chandle [IN]          Pointer to cont_handle struct,
 *                              returned by PDCcont_iter_start(pdcid_t pdc_id)
 *
 * \return 1 on success/0 on failure
 */
pbool_t PDCcont_iter_null(cont_handle *chandle);

/**
 * Move to the next container within a PDC
 *
 * \param chandle [IN]          Pointer to cont_handle struct, returned by
 *                              PDCcont_iter_start(pdcid_t pdc_id)
 *
 * \return Pointer to cont_handle struct/NULL on failure
 */
cont_handle *PDCcont_iter_next(cont_handle *chandle);

/**
 * Retrieve container information
 *
 * \param chandle [IN]          A cont_handle struct, returned by
 *                              PDCcont_iter_start(pdcid_t pdc_id)
 *
 * \return Pointer to a PDC_cont_info struct/NULL on failure
 */
struct pdc_cont_info *PDCcont_iter_get_info(cont_handle *chandle);

/**
 * ************
 *
 * \param cont_id [IN]          Container ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_del(pdcid_t cont_id);

/**
 * ***********
 *
 * \param cont_id [IN]          Container ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 * \param value_size [IN]       ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_put_tag(pdcid_t cont_id, char *tag_name, void *tag_value, psize_t value_size);

/**
 * ***********
 *
 * \param cont_id [IN]          Container ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 * \param value_size [IN]       ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_get_tag(pdcid_t cont_id, char *tag_name, void **tag_value, psize_t *value_size);

/**
 * Deleta a tag from a container
 *
 * \param cont_id [IN]          Container ID
 * \param tag_name [IN]         Metadta field name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_del_tag(pdcid_t cont_id, char *tag_name);

/**
 * ********
 *
 * \param cont_id [IN]          Container ID
 * \param nobj [IN]             ******
 * \param obj_ids [IN]          ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_put_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids);

/**
 * *********
 *
 * \param cont_id [IN]          Container ID
 * \param nobj [IN]             ******
 * \param obj_ids [IN]          ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_get_objids(pdcid_t cont_id, int *nobj, pdcid_t **obj_ids);

/**
 * **********
 *
 * \param cont_id [IN]          Container ID
 * \param nobj [IN]             ******
 * \param obj_ids [IN]          ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_del_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids);

#endif /* PDC_CONT_H */
