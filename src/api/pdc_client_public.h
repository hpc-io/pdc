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


/*
 * Data Server related
 */

/**
 * Client request server to collectively read a region of an object
 *
 * \param server_id [IN]         Target local data server ID
 * \param n_client [IN]          Number of clients that send to read request to one data server
 * \param meta [IN]              Metadata 
 * \param region [IN]            Region
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Client_data_server_read(PDC_Request_t *request);
/**
 * Client request server to collectively write a region of an object
 *
 * \param server_id [IN]         Target local data server ID
 * \param n_client [IN]          Number of clients that send to read request to one data server
 * \param meta [IN]              Metadata 
 * \param region [IN]            Region
 * \param buf[IN]                User buffer to store the read 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_write(PDC_Request_t *request); 

/**
 * Client request server to check IO status of a previous IO request
 *
 * \param request[IN]            PDC IO request
 * \param region [IN]            Region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_write_check(PDC_Request_t *request, int *status);

perr_t PDC_Client_write_id(pdcid_t local_obj_id, struct PDC_region_info *region, void *buf);
/*
 * Send a checkpoint metadata to all servers
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_all_server_checkpoint();
/**
 * Request of PDC client to delete metadata by object name
 *
 * \param delete_name [IN]      Name to delete
 * \param obj_delete_prop [IN]  Id of the associated property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_delete_metadata(char *delete_name, pdcid_t obj_delete_prop);

/**
 * Request of PDC client to delete metadata by object id
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_id [IN]          Id of the container
 * \param obj_id [IN]           Id of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_delete_metadata_by_id(uint64_t obj_id);

/**
 * Request from PDC client to close server
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_close_all_server();


/**
 * Client request server to check IO status of a previous IO request
 *
 * \param request [IN]            IO request to data server 
 * \param completed [IN]          Will be set to 1 if request is completed, otherwise 0 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_test(PDC_Request_t *request, int *completed);

/**
 * Wait for a previous IO request to be completed by server, or exit after timeout
 *
 * \param request [IN]            IO request to data server 
 * \param max_wait_ms[IN]         Max wait time for the check 
 * \param check_interval_ms[IN]   Time between sending check requet to server
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_wait(PDC_Request_t *request, unsigned long max_wait_ms, unsigned long check_interval_ms);

/*
 * Delete a number of objects from a container
 *
 * \param nobj[IN]               Number of objects to be deleted
 * \param local_obj_ids[IN]      Object ids   (local id, not the global one from metadata server)
 * \param local_cont_id[IN]      Container id (local id, not the global one from metadata server)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_del_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id);

/*
 * Add a number of objects to a container
 *
 * \param nobj[IN]               Number of objects to be added
 * \param local_obj_ids[IN]      Object ids   (local id, not the global one from metadata server)
 * \param local_cont_id[IN]      Container id (local id, not the global one from metadata server)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_add_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id);

perr_t PDC_Client_create_cont_id_mpi(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id);

/*
 * Query and read a number of objects with their obj name
 *
 * \param nobj[IN]               Number of objects to be queried
 * \param obj_names[IN]          Object names
 * \param out_buf[OUT]           Object data buffer, allocated by PDC
 * \param out_buf_sizes[OUT]     Sizes of object data buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_name_read_entire_obj(int nobj, char **obj_names, 
                                             void ***out_buf, uint64_t *out_buf_sizes);


/*
 * Query an object based on a specific metadata (attribute) name and value  
 *
 * \param cont_id[IN]           Container ID, 0 for all containers
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

/*
 * Send updated metadata (stored as property) to metadata server
 *
 * \param obj_id[IN]             Object ID
 * \param prop_id[IN]            Object property 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_update(pdcid_t obj_id, pdcid_t prop_id);

/*
 * Create a tag with a specific name and value
 *
 * \param obj_id[IN]             Object ID
 * \param tag_name [IN]          Metadta field name
 * \param tag_value [IN]         Metadta field value
 * \param value_len [IN]         Length of the metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_create(pdcid_t obj_id, char *tag_name, void *tag_value, size_t value_len);
perr_t PDC_add_kvtag(pdcid_t obj_id, pdc_kvtag_t *kvtag);

/*
 * Delete a tag with a specific name and value
 *
 * \param obj_id[IN]             Object ID
 * \param tag_name [IN]          Metadta field name
 * \param tag_value [IN]         Metadta field value
 * \param value_len [IN]         Length of the metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_delete(pdcid_t obj_id, char *tag_name);

/*
 * Get the size of the value of a specific tag name
 *
 * \param obj_id[IN]             Object ID
 * \param tag_name [IN]          Metadta field name
 * \param value_len [OUT]        Length of the metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_getinfo(pdcid_t obj_id, char *tag_name, size_t *value_len);

/*
 * Get the value of a specific tag name
 *
 * \param obj_id[IN]             Object ID
 * \param tag_name [IN]          Metadta field name
 * \param tag_value [IN]         Metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_get(pdcid_t obj_id, char *tag_name, void *tag_value);
perr_t PDC_get_kvtag(pdcid_t obj_id, char *tag_name, pdc_kvtag_t **kvtag);

perr_t PDC_free_kvtag(pdc_kvtag_t **kvtag);


perr_t PDC_Client_add_tags_to_container(uint64_t cont_meta_id, char *tags);

perr_t PDC_query_name_timestep_agg(const char *obj_name, int time_step, void **out);

perr_t PDC_iwrite(void *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf);

perr_t PDC_wait(PDC_Request_t *request, unsigned long max_wait_ms, unsigned long check_interval_ms);

int PDC_get_nproc_per_node();




perr_t  PDCobj_get_id(const char *tag_name, void *tag_value, int value_size, int *n_res, uint64_t **pdc_ids); // TODO
perr_t  PDCobj_get_name(const char *tag_name, void *tag_value, int value_size, int *n_res, char **obj_names); // TODO
pdcid_t PDCobj_put_data(const char *obj_name, const void *data, uint64_t size, pdcid_t pdc_id, pdcid_t cont_id);
perr_t  PDCobj_get_data(pdcid_t obj_id, void **data, uint64_t *size);
perr_t  PDCobj_del_data(pdcid_t obj_id);
perr_t  PDCobj_put_tag(pdcid_t obj_id, const char *tag_name, const void *tag_value, uint64_t value_size);
perr_t  PDCobj_mod_tag(pdcid_t obj_id, const char *tag_name, const void *tag_value, uint64_t value_size);
perr_t  PDCobj_get_tag(pdcid_t obj_id, const char *tag_name, void **tag_value, uint64_t *value_size);
perr_t  PDCobj_del_tag(pdcid_t obj_id, const char *tag_name);


pdcid_t PDCcont_create(const char *cont_name, pdcid_t pdc_id);
perr_t  PDCcont_get_name(pdcid_t cont_id, char **cont_name);
pdcid_t PDCcont_get_id(const char *cont_name, pdcid_t pdc_id);
perr_t  PDCcont_del(pdcid_t cont_id);
perr_t  PDCcont_put_tag(pdcid_t cont_id, const char *tag_name, const void *tag_value, const uint64_t value_size);
perr_t  PDCcont_get_tag(pdcid_t cont_id, const char *tag_name, void **tag_value, uint64_t *value_size);
perr_t  PDCcont_del_tag(pdcid_t cont_id, const char *tag_name);

perr_t  PDCcont_put_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids);
perr_t  PDCcont_get_objids(pdcid_t cont_id, int *nobj, pdcid_t **obj_ids);
perr_t  PDCcont_del_objids(pdcid_t cont_id, const int nobj, const pdcid_t *obj_ids);

#endif
