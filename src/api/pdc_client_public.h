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

#ifndef PDC_CLIENT_PUBLIC_H
#define PDC_CLIENT_PUBLIC_H

#include "pdc_query.h"

enum pdc_prop_name_t {PDC_OBJ_NAME, PDC_CONT_NAME, PDC_APP_NAME, PDC_USER_ID};

// Request structure for async read/write
typedef struct PDC_Request_t {
    int                      seq_id;
    int                      server_id;
    int                      n_client;
    int                      n_update;
    PDC_access_t             access_type;
    void                    *metadata;
    struct PDC_region_info  *region;
    void                    *buf;

    char                    *shm_base;
    char                     shm_addr[128];
    int                      shm_fd;
    int                      shm_size;

    int                      n_buf_arr;
    void                  ***buf_arr;
    int                     *buf_arr_idx;
    char                   **shm_base_arr;
    char                   **shm_addr_arr;
    int                     *shm_fd_arr;
    uint64_t               *shm_size_arr;

    void                   *storage_meta;

    struct PDC_Request_t      *prev;
    struct PDC_Request_t      *next;
} PDC_Request_t;

/**
 * Query an object based on a specific metadata (attribute) name and value
 *
 * \param cont_id [IN]          Container ID, 0 for all containers
 * \param prop_name [IN]        Metadta field name
 * \param prop_value [IN]       Metadta field value
 * \param out_ids[OUT]          Result object ids
 * \param n_out[OUT]            Number of results
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_prop_query(pdcid_t cont_id, enum pdc_prop_name_t prop_name, void *prop_value, 
                         pdcid_t **out_ids, size_t *n_out);

/* TODO: another routine to destroy query results */

/**
 * Send updated metadata (stored as property) to metadata server
 *
 * \param obj_id[IN]            Object ID
 * \param prop_id[IN]           Object property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_update(pdcid_t obj_id, pdcid_t prop_id);

/**
 * Create a tag with a specific name and value
 *
 * \param obj_id[IN]            Object ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 * \param value_len [IN]        Length of the metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_create(pdcid_t obj_id, char *tag_name, void *tag_value, size_t value_len);

/**
 * Delete a tag with a specific name and value
 *
 * \param obj_id[IN]            Object ID
 * \param tag_name [IN]         Metadta field name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_delete(pdcid_t obj_id, char *tag_name);

/**
 * Get the size of the value of a specific tag name
 *
 * \param obj_id[IN]            Object ID
 * \param tag_name [IN]         Metadta field name
 * \param value_len [IN]        Length of the metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_getinfo(pdcid_t obj_id, char *tag_name, size_t *value_len);

/**
 * **********
 *
 * \param obj_id[IN]            Object ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_get(pdcid_t obj_id, char *tag_name, void *tag_value);

/**
 * ***********
 *
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 * \param value_size [IN]       *********
 * \param n_res [IN]            *********
 * \param pdc_ids [IN]          *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_get_id(const char *tag_name, void *tag_value, int value_size, int *n_res, uint64_t **pdc_ids);

/**
 * ***********
 *
 * \param tag_name [IN]         Metadta field name
 * \param value_len [IN]        Metadta field value
 * \param value_size [IN]       *********
 * \param n_res [IN]            *********
 * \param obj_names [IN]        *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_get_name(const char *tag_name, void *tag_value, int value_size, int *n_res, char **obj_names);

/**
 * ***********
 *
 * \param obj_name [IN]         Object name
 * \param data [IN]             *********
 * \param size [IN]             *********
 * \param n_res [IN]            *********
 * \param cont_id [IN]          *********
 *
 * \return Non-negative on success/Negative on failure
 */
pdcid_t PDCobj_put_data(const char *obj_name, void *data, uint64_t size, pdcid_t cont_id);

/**
 * ***********
 *
 * \param obj_id [IN]           Object ID
 * \param data [IN]             *********
 * \param size [IN]             *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_get_data(pdcid_t obj_id, void **data, uint64_t *size);

/**
 * ***********
 *
 * \param obj_id [IN]           Object ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_del_data(pdcid_t obj_id);

/**
 * ***********
 *
 * \param obj_id [IN]           Object ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 * \param value_size [IN]       ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_put_tag(pdcid_t obj_id, char *tag_name, void *tag_value, size_t value_size);

/**
 * ***********
 *
 * \param obj_id [IN]           Object ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 * \param value_size [IN]       ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_mod_tag(pdcid_t obj_id, const char *tag_name, const void *tag_value, size_t value_size);

/**
 * ***********
 *
 * \param obj_id [IN]           Object ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 * \param value_size [IN]       ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_get_tag(pdcid_t obj_id, char *tag_name, void **tag_value, size_t *value_size);

/**
 * Delete a tag from the object
 *
 * \param obj_id [IN]           Object ID
 * \param tag_name [IN]         Metadta field name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_del_tag(pdcid_t obj_id, char *tag_name);

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
 * Get container ID by its name
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
perr_t PDCcont_put_tag(pdcid_t cont_id, char *tag_name, void *tag_value, size_t value_size);

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
perr_t PDCcont_get_tag(pdcid_t cont_id, char *tag_name, void **tag_value, size_t *value_size);

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

/**
 * ********
 *
 * \param sel [IN]              *********
 */
void PDCselection_free(pdcselection_t *sel);

/**
 * ********
 *
 * \param query [IN]            *********
 */
void PDCquery_free(pdcquery_t *query);

/**
 * ********
 *
 * \param query [IN]            *********
 */
void PDCquery_free_all(pdcquery_t *query);

/**
 * ********
 *
 * \param query [IN]            *********
 */
void PDCquery_print(pdcquery_t *query);

/**
 * ********
 *
 * \param region [IN]           *********
 */
void PDCregion_free(struct PDC_region_info *region);

/**
 * ********
 *
 * \param sel [IN]              *********
 */
void PDCselection_print(pdcselection_t *sel);

/**
 * ********
 *
 * \param sel [IN]              *********
 */
void PDCselection_print_all(pdcselection_t *sel);

#endif /* PDC_CLIENT_PUBLIC_H */
