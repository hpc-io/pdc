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

#ifndef PDC_CLIENT_CONNECT_H
#define PDC_CLIENT_CONNECT_H

#include "pdc_prop.h"
#include "pdc_obj_pkg.h"
#include "pdc_client_server_common.h"
#include "mercury.h"
#include "mercury_proc_string.h"
#include "mercury_request.h"

extern int                      pdc_server_num_g;
extern int                      pdc_client_mpi_rank_g;
extern int                      pdc_client_mpi_size_g;
extern pdc_server_selection_t   pdc_server_selection_g;
extern struct _pdc_server_info *pdc_server_info_g;

/****************************/
/* Library Private Typedefs */
/****************************/
struct _pdc_server_info {
    char      addr_string[ADDR_MAX];
    int       addr_valid;
    hg_addr_t addr;
};

struct _pdc_client_lookup_args {
    const char *obj_name;
    uint64_t    obj_id;
    uint32_t    server_id;
    uint32_t    client_id;
    int         ret;
    char *      ret_string;
    char *      client_addr;

    uint32_t      user_id;
    const char *  app_name;
    int           time_step;
    uint32_t      hash_value;
    const char *  tags;
    hg_request_t *request;
};

struct _pdc_client_transform_args {
    size_t                                 size;
    void *                                 data;
    void *                                 transform_result;
    struct _pdc_region_transform_ftn_info *this_transform;
    struct pdc_region_info *               region_info;
    int                                    type_extent;
    int                                    transform_state;
    int                                    ret;
    hg_bulk_t                              local_bulk_handle;
};

struct _pdc_metadata_query_args {
    pdc_metadata_t *data;
};

struct _pdc_container_query_args {
    uint64_t cont_id;
};

struct _pdc_buf_map_args {
    int32_t ret;
};

struct _pdc_region_lock_args {
    pbool_t *status;
    int      ret;
};

struct _pdc_get_kvtag_args {
    int          ret;
    pdc_kvtag_t *kvtag;
};

struct _pdc_query_result_list {
    uint32_t  ndim;
    int       query_id;
    uint64_t  nhits;
    uint64_t *coords;
    void *    data;
    void **   data_arr;
    uint64_t *data_arr_size;
    uint64_t  recv_data_nhits;

    struct _pdc_query_result_list *prev;
    struct _pdc_query_result_list *next;
};

#define PDC_CLIENT_DATA_SERVER() ((pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g)

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * Request from client to get address of the server
 *
 * \return Number of servers on success/Negative on failure
 */
perr_t PDC_Client_read_server_addr_from_file();

/**
 * Client request of an obj id by sending object name
 *
 * \param obj_name [IN]         Name of the object
 * \param cont_id[IN]           Container ID (obtained from metadata server)
 * \param obj_create_prop [IN]  ID of the object property
 * \param meta_id [OUT]         Pointer to medadata id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_name_recv_id(const char *obj_name, uint64_t cont_id, pdcid_t obj_create_prop,
                                    pdcid_t *meta_id);

/**
 * Apply a map from buffer to an object
 *
 * \param local_region_id [IN]  ID of local region
 * \param remote_obj_id [IN]    ID of remote object
 * \param ndim [IN]             The offset of local region
 * \param local_dims [IN]       The dimension of local region
 * \param local_offset [IN]     The offset of local region
 * \param local_type [IN]       The data type of local data
 * \param local_data [IN]       The starting address of local data
 * \param remote_type [IN]      The data type of remoate data
 * \param local_region [IN]     Local region information
 * \param remote_region [IN]    Reomote region information
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_buf_map(pdcid_t local_region_id, pdcid_t remote_obj_id, size_t ndim, uint64_t *local_dims,
                          uint64_t *local_offset, pdc_var_type_t local_type, void *local_data,
                          pdc_var_type_t remote_type, struct pdc_region_info *local_region,
                          struct pdc_region_info *remote_region);

/**
 * Client request for buffer to object unmap
 *
 * \param remote_obj_id [IN]     ID of remote object
 * \param remote_reg_id [IN]     ID of remote region
 * \param reginfo [IN]           Remote region information
 * \param data_type [IN]         The data type of remote region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_buf_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id, struct pdc_region_info *reginfo,
                            pdc_var_type_t data_type);

/**
 * Request of PDC client to get region lock
 *
 * \param obj_id [IN]           ID of the metadata
 * \param region_info [IN]      Pointer to pdc_region_info struct
 * \param access_type [IN]      Access type (enum)
 * \param lock_mode [IN]        Lock mode
 * \param obtained [OUT]        Lock granted or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_region_lock(struct _pdc_obj_info *object_info, struct pdc_region_info *region_info,
                              pdc_access_t access_type, pdc_lock_mode_t lock_mode, pdc_var_type_t data_type,
                              pbool_t *obtained);

/**
 * Request of PDC client to get region release
 *
 * \param obj_id [IN]           ID of the metadata
 * \param region_info [IN]      Pointer to pdc_region_info struct
 * \param access_type [IN]      Access type (enum)
 * \param released [OUT]        Lock released or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_region_release(struct _pdc_obj_info *object_info, struct pdc_region_info *region_info,
                                 pdc_access_t access_type, pdc_var_type_t data_type, pbool_t *released);

/**
 * PDC client initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_init();

/**
 * PDC client finalization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_finalize();

/**
 * PDC client direct (i.e. client-to-client comms) init
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_direct_init();

/**
 * Copy the metadata to local object property, internal function
 *
 * \param obj_name[IN]          Object name
 * \param obj_id[IN]            Object ID (global, obtained from metadata server)
 * \param cont_id[IN]           Container id (global, obtained from metadata server)
 * \param obj_info[IN]          Object property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_attach_metadata_to_local_obj(const char *obj_name, uint64_t obj_id, uint64_t cont_id,
                                               struct _pdc_obj_info *obj_info);

/**
 * ****************
 *
 * \param callback_info[IN]     Struct of callback information
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Client_recv_bulk_storage_meta_cb(const struct hg_cb_info *callback_info);

/**
 * ****************
 *
 * \param process_args[IN]      Struct of process agrs
 *
 * \return *******
 */
perr_t PDC_Client_recv_bulk_storage_meta(process_bulk_storage_meta_args_t *process_args);

/**
 * ****************
 *
 * \param nobj[IN]              Number of objects
 * \param obj_names[IN]         Object names
 * \param out_buf[IN]           Starting address of out buffer
 * \param out_buf_sizes[IN]     The size of output buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_name_read_entire_obj_client(int nobj, char **obj_names, void ***out_buf,
                                                    uint64_t *out_buf_sizes);

/**
 * ****************
 *
 * \param nobj[IN]              Number of objects
 * \param req_start[IN]         ******
 * \param req_count[IN]
 * \param storage_start[IN]
 * \param storage_count[IN]
 * \param fp[IN]
 * \param file_offset[IN]
 * \param buf[IN]
 * \param total_read_bytes[IN]
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_read_overlap_regions(uint32_t ndim, uint64_t *req_start, uint64_t *req_count,
                                       uint64_t *storage_start, uint64_t *storage_count, FILE *fp,
                                       uint64_t file_offset, void *buf, size_t *total_read_bytes);

/**
 * Sync request send to server to write a region of object with users buffer
 * and wait for server's push notification
 *
 * \param meta [IN]             Metadata object pointer to the operating object
 * \param region [IN]           Region within the object to be read
 * \param buf [IN]              User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_write_wait_notify(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf);

/**
 * Sync request send to server to read a region of object with users buffer
 * and wait for server's push notification
 *
 * \param meta [IN]             Metadata object pointer to the operating object
 * \param region [IN]           Region within the object to be read
 * \param buf [IN]              User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_read_wait_notify(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf);

/**
 * Request of PDC client to add a tag to metadata
 *
 * \param obj_id [IN]           Object ID
 * \param new_tag [IN]          Pointer to a string storing new tag to be added
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_add_tag(pdcid_t obj_id, const char *tag);

/**
 * Request of PDC client to update metadata
 *
 * \param old [IN]              Pointer to pdc_metadata_t struct storing old infomation
 * \param new [IN]              Pointer to pdc_metadata_t struct storing updated infomation
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_update_metadata(pdc_metadata_t *old, pdc_metadata_t *new);

/**
 * PDC client query metadata by object name
 *
 * \param obj_name [IN]         Object name
 * \param out [OUT]             Pointer to pdc_metadata_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_metadata_name_only(const char *obj_name, pdc_metadata_t **out);

/*
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_tag(const char *tags, int *n_res, pdc_metadata_t ***out);

/**
 * PDC client query metadata from server for a certain time step
 *
 * \param obj_name [IN]         Object name
 * \param time_step [IN]        Time step
 * \param out [OUT]             Pointer to pdc_metadata_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_metadata_name_timestep(const char *obj_name, int time_step, pdc_metadata_t **out);

/**
 * PDC client query metadata from server for a certain time step
 *
 * \param obj_name [IN]         Object name
 * \param time_step [IN]        Time step
 * \param out [OUT]             Pointer to pdc_metadata_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_metadata_name_timestep_agg(const char *obj_name, int time_step, pdc_metadata_t **out);

/**
 * Listing all objects on the client
 *
 * \param n_res [OUT]           Number of objects
 * \param out [OUT]             Pointer to pdc_metadata_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_list_all(int *n_res, pdc_metadata_t ***out);

/**
 * ************
 *
 * \param is_list_all [IN]      **********
 * \param user_id [IN]          **********
 * \param app_name [IN]         **********
 * \param obj_name [IN]         **********
 * \param time_step_from [IN]   **********
 * \param time_step_to [IN]     **********
 * \param user_ndim [IN]        **********
 * \param tags [IN]             **********
 * \param n_res [IN]            **********
 * \param out [OUT]             **********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_partial_query(int is_list_all, int user_id, const char *app_name, const char *obj_name,
                         int time_step_from, int time_step_to, int ndim, const char *tags, int *n_res,
                         pdc_metadata_t ***out);

/**
 * Client request server to collectively write a region of an object
 * and wait for server's push notification
 *
 * \param meta [IN]             Metadata
 * \param region [IN]           Region
 * \param buf[IN]               User buffer to store the read
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_write_wait_notify(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf);

/**
 * Client request server to check IO status of a previous IO request
 *
 * \param server_id [IN]        Target local data server ID
 * \param n_client [IN]         Client ID
 * \param meta [IN]             Metadata
 * \param region [IN]           Region
 * \param status [IN]           Status
 * \param buf[IN]               User buffer to store the read data
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_read_check(int server_id, uint32_t client_id, pdc_metadata_t *meta,
                                         struct pdc_region_info *region, int *status, void *buf);

/**
 * Client request server to return container ID by sending container name
 *
 * \param cont_name [IN]        Container name
 * \param cont_meta_id [OUT]    Container metadata ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_container_name(const char *cont_name, uint64_t *cont_meta_id);

/**
 * Client requests server to return container ID by sending container name (used by MPI mode)
 *
 * \param cont_name [IN]        Container name
 * \param cont_meta_id [OUT]    Container metadata ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_container_name_col(const char *cont_name, uint64_t *cont_meta_id);

/**
 * Async request send to server to read a region and put it in users buffer
 *
 * \param meta [IN]             Metadata object pointer to the operating object
 * \param region [IN]           Region within the object to be read
 * \param request [IN]          Request structure to store the IO information
 * \param buf [IN]              User's buffer to store data
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_iread(pdc_metadata_t *meta, struct pdc_region_info *region, struct pdc_request *request,
                        void *buf);

/**
 * Sync request send to server to read a region and put it in users buffer
 *
 * \param meta [IN]             Metadata object pointer to the operating object
 * \param region [IN]           Region within the object to be read
 * \param buf [IN]              User's buffer to store data
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_read(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf);

/**
 * Sync request send to server to write a region of object with users buffer
 *
 * \param meta [IN]             Metadata object pointer to the operating object
 * \param region [IN]           Region within the object to be read
 * \param request [IN]          Request structure to store the IO information
 * \param buf [IN]              User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_iwrite(pdc_metadata_t *meta, struct pdc_region_info *region, struct pdc_request *request,
                         void *buf);

/**
 * Sync request send to server to write a region of object with users buffer
 *
 * \param meta [IN]             Metadata object pointer to the operating object
 * \param region [IN]           Region within the object to be read
 * \param buf [IN]              User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_write(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf);

/**
 * Generate container ID by server
 *
 * \param cont_name [IN]        Container name
 * \param cont_create_prop [IN] Container property ID
 * \param cont_id [OUT]         Generated container ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_create_cont_id(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id);

/**
 * Generate container ID by server (used by MPI mode)
 *
 * \param cont_name [IN]        Container name
 * \param cont_create_prop [IN] Container property ID
 * \param cont_id [OUT]         Generated container ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_create_cont_id_mpi(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id);

/**
 * Client sends query requests to server
 *
 * \param kvtag [IN]            *********
 * \param n_res [IN]            **********
 * \param pdc_ids [OUT]         *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_kvtag(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids);

/**
 * Client sends query requests to server (used by MPI mode)
 *
 * \param kvtag [IN]            *********
 * \param n_res [IN]            **********
 * \param pdc_ids [OUT]         *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_kvtag_col(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids);

/**
 * Client sends query requests to server (used by MPI mode)
 *
 * \param my_nobj [IN]          *********
 * \param n_my_obj_names [IN]   **********
 * \param out_buf [OUT]         *********
 * \param out_buf_sizes [OUT]   *********
 * \param cache_percent [IN]    *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_name_read_entire_obj_client_agg_cache_iter(int my_nobj, char **my_obj_names,
                                                                   void ***out_buf, size_t *out_buf_sizes,
                                                                   int cache_percent);

/**
 * ********
 *
 * \param query [IN]            *********
 * \param get_op [IN]           *********
 * \param nhits [IN]            *********
 * \param sel [IN]              *********
 * \param data [IN]             *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_send_data_query(pdc_query_t *query, pdc_query_get_op_t get_op, uint64_t *nhits,
                           pdc_selection_t *sel, void *data);

/**
 * ********
 *
 * \param callback_info [IN]    *********
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_recv_coords(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param obj_id [IN]           *********
 * \param sel [IN]              *********
 * \param data [IN]             *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_get_sel_data(pdcid_t obj_id, pdc_selection_t *sel, void *data);

/**
 * ********
 *
 * \param callback_info [IN]    *********
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_recv_read_coords_data(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param cp [IN]               Object property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_obj_prop_free(struct _pdc_obj_prop *cp);

/**
 * ********
 *
 * \param callback_info [IN]    *********
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param server_id [IN]        Server ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_lookup_server(int server_id);

/**
 * ********
 *
 * \param hg_context [IN]       *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_check_response(hg_context_t **hg_context);

/**
 * ********
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Client_work_done_cb();

/**
 * Client request server to collectively read a region of an object
 *
 * \param request [IN]          PDC request struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_read(struct pdc_request *request);

/**
 * *******
 *
 * \param request [IN]          Client request
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_write(struct pdc_request *request);

/**
 * Client request server to check IO status of a previous IO request
 *
 * \param request[IN]           PDC IO request
 * \param status [OUT]          Returned status
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_write_check(struct pdc_request *request, int *status);

/**
 * Client request server to check IO status of a previous IO request
 *
 * \param local_obj_id[IN]      **********
 * \param region [IN]           **********
 * \param buf [IN]              **********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_write_id(pdcid_t local_obj_id, struct pdc_region_info *region, void *buf);

/**
 * Send a checkpoint metadata to all servers
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_all_server_checkpoint();

/**
 * Request of PDC client to delete metadata by object name
 *
 * \param delete_name [IN]      Name to delete
 * \param obj_delete_prop [IN]  ID of the associated property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_delete_metadata(char *delete_name, pdcid_t obj_delete_prop);

/**
 * Request of PDC client to delete metadata by object id
 *
 * \param obj_id [IN]           Object ID
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
 * \param request [IN]          IO request to data server
 * \param completed [IN]        Will be set to 1 if request is completed, otherwise 0
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_test(struct pdc_request *request, int *completed);

/**
 * Wait for a previous IO request to be completed by server, or exit after timeout
 *
 * \param request [IN]          IO request to data server
 * \param max_wait_ms[IN]       Max wait time for the check
 * \param check_interval_ms[IN] Time between sending check requet to server
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_wait(struct pdc_request *request, unsigned long max_wait_ms,
                       unsigned long check_interval_ms);

/**
 * Wait for a previous IO request to be completed by server, or exit after timeout
 *
 * \param nobj[IN]              Number of objects to be deleted
 * \param local_obj_ids [IN]    Object ids   (local id, not the global one from metadata server)
 * \param local_cont_id [IN]    Container id (local id, not the global one from metadata server)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_del_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id);

/**
 * Wait for a previous IO request to be completed by server, or exit after timeout
 *
 * \param nobj [IN]             Number of objects to be added
 * \param local_obj_ids [IN]    Object ids   (local id, not the global one from metadata server)
 * \param local_cont_id [IN]    Container id (local id, not the global one from metadata server)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_add_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id);

/**
 * Wait for a previous IO request to be completed by server, or exit after timeout
 *
 * \param cont_name [IN]        ********
 * \param cont_create_prop [IN] ID of the associated property
 * \param cont_id [IN]          Container id (local id, not the global one from metadata server)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_create_cont_id_mpi(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id);

/**
 * Query and read a number of objects with their obj name
 *
 * \param nobj [IN]             Number of objects to be queried
 * \param obj_names [IN]        Object names
 * \param out_buf [OUT]         Object data buffer, allocated by PDC
 * \param out_buf_sizes [OUT]   Sizes of object data buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_name_read_entire_obj(int nobj, char **obj_names, void ***out_buf,
                                             uint64_t *out_buf_sizes);

/**
 * *********
 *
 * \param cont_meta_id [IN]     Container metadata ID
 * \param tags [IN]             *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_add_tags_to_container(uint64_t cont_meta_id, char *tags);

/**
 * *********
 *
 * \param obj_name [IN]         Object name
 * \param time_step [IN]        Time step
 * \param out [out]             ********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_query_name_timestep_agg(const char *obj_name, int time_step, void **out);

/**
 * *********
 *
 * \param meta [IN]             ******
 * \param region [IN]           ******
 * \param request [IN]          ******
 * \param buf [IN]              ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_iwrite(void *meta, struct pdc_region_info *region, struct pdc_request *request, void *buf);

/**
 * *********
 *
 * \param request [IN]          ******
 * \param max_wait_ms [IN]      ******
 * \param check_inter_ms [IN]   ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_wait(struct pdc_request *request, unsigned long max_wait_ms, unsigned long check_inter_ms);

/**
 * Get number of processes on each node
 *
 * \return Number of processes
 */
int PDC_get_nproc_per_node();

/**
 * *******
 *
 * \param kvtag [IN]             ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_free_kvtag(pdc_kvtag_t **kvtag);

perr_t PDC_Client_del_metadata(pdcid_t id, int is_cont);

#endif /* PDC_CLIENT_CONNECT_H */
