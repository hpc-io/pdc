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

#include "mercury.h"
#include "mercury_proc_string.h"

#include "pdc.h"
#include "pdc_prop.h"
#include "pdc_obj_pkg.h"
#include "pdc_client_server_common.h"


extern int pdc_server_num_g;

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

/* extern char pdc_server_tmp_dir_g[ADDR_MAX]; */
hg_return_t PDC_Client_work_done_cb(const struct hg_cb_info *callback_info);
hg_return_t PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info);

typedef struct pdc_server_info_t {
    char            addr_string[ADDR_MAX];
    int             addr_valid;
    hg_addr_t       addr;
    int             rpc_handle_valid;
    hg_handle_t     rpc_handle;
    int             client_test_handle_valid;
    hg_handle_t     client_test_handle;
    int             close_server_handle_valid;
    hg_handle_t     close_server_handle;
    /* int             name_marker_handle_valid; */
    /* hg_handle_t     name_marker_handle; */
    int             metadata_query_handle_valid;
    hg_handle_t     metadata_query_handle;
    int             metadata_delete_handle_valid;
    hg_handle_t     metadata_delete_handle;
    int             metadata_delete_by_id_handle_valid;
    hg_handle_t     metadata_delete_by_id_handle;
    int             metadata_add_tag_handle_valid;
    hg_handle_t     metadata_add_tag_handle;
    int             metadata_update_handle_valid;
    hg_handle_t     metadata_update_handle;
    int	            client_send_region_map_handle_valid;
    hg_handle_t     client_send_region_map_handle;
    int             client_send_region_unmap_handle_valid;
    hg_handle_t     client_send_region_unmap_handle;
    int             client_send_object_unmap_handle_valid;
    hg_handle_t     client_send_object_unmap_handle;
    int             region_lock_handle_valid;
    hg_handle_t     region_lock_handle;
    int             region_release_handle_valid;
    hg_handle_t     region_release_handle;
    int             query_partial_handle_valid;
    hg_handle_t     query_partial_handle;
    // Data server related
    int             data_server_read_handle_valid;
    hg_handle_t     data_server_read_handle;
    int             data_server_write_handle_valid;
    hg_handle_t     data_server_write_handle;
    int             data_server_read_check_handle_valid;
    hg_handle_t     data_server_read_check_handle;
    int             data_server_write_check_handle_valid;
    hg_handle_t     data_server_write_check_handle;
} pdc_server_info_t;

extern pdc_server_info_t *pdc_server_info_g;

// Request structure for async read/write
typedef struct PDC_Request_t {
    int                     server_id;
    int                     n_client;
    PDC_access_t            access_type;
    pdc_metadata_t          *metadata;
    struct PDC_region_info  *region;
    void                    *buf;

    struct PDC_Request_t      *prev;
    struct PDC_Request_t      *next;
} PDC_Request_t;

struct client_lookup_args {
    const char          *obj_name;
    uint64_t             obj_id;
    uint32_t             server_id;
    uint32_t             client_id;
    int                  ret;
    char                *ret_string;
    char                *client_addr;

    uint32_t             user_id;
    const char          *app_name;
    uint32_t             time_step;
    uint32_t             hash_value;
    const char          *tags;
};

typedef struct metadata_query_args_t {
    pdc_metadata_t *data;
} metadata_query_args_t;

/* typedef struct client_name_cache_t { */
/*     char                        name[ADDR_MAX]; */
/*     struct client_name_cache_t *prev; */
/*     struct client_name_cache_t *next; */
/* } client_name_cache_t; */

struct region_map_args {
	int         ret;
};

struct region_unmap_args {
    int         ret;
};

struct object_unmap_args {
    int         ret;
};

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
 * \param obj_create_prop [IN]  Id of the object property
 * \param meta_id [OUT]         Pointer to medadata id
 * \param meta_id [OUT]         Pointer to client id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_name_recv_id(const char *obj_name, pdcid_t obj_create_prop, pdcid_t *meta_id, int32_t *client_id);

/**
 * Listing all objects on the client
 *
 * \param n_res [OUT]           Number of objects
 * \param out [OUT]             Pointer to pdc_metadata_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_list_all(int *n_res, pdc_metadata_t ***out);

/*
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_partial_query(int is_list_all, int user_id, const char* app_name, const char* obj_name, int time_step_from, int time_step_to, int ndim, const char* tags, int *n_res, pdc_metadata_t ***out);

/** 
 * PDC client query metadata from server for a certain time step
 *
 * \param obj_name [IN]         Object name
 * \param time_step [IN]        Time step
 * \param out [OUT]             Pointer to pdc_metadata_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_metadata_name_timestep(const char *obj_name, uint32_t time_step, pdc_metadata_t **out);

/**
 * PDC client query metadata by object name
 *
 * \param obj_name [IN]         Object name
 * \param out [OUT]             Pointer to pdc_metadata_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_query_metadata_name_only(const char *obj_name, pdc_metadata_t **out);

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
 * Request of PDC client to add a tag to metadata
 *
 * \param old [IN]              Pointer to pdc_metadata_t struct storing old infomation
 * \param new_tag [IN]          Pointer to a string storing new tag to be added
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_add_tag(pdc_metadata_t *old, const char *new_tag);


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
 * Client request for object mapping
 *
 * \param local_obj_id [IN]     The origin object id
 * \param local_region_id [IN]  The origin region id
 * \param remote_obj_id [IN]    The target object id
 * \param remote_region_id [IN] The target region id
 * \param remote_client_id [IN] The target client id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_region_map(pdcid_t local_obj_id, pdcid_t local_region_id, pdcid_t remote_obj_id, pdcid_t remote_region_id, size_t ndim, uint64_t *local_dims, uint64_t *local_offset, uint64_t *local_size, PDC_var_type_t local_type, void *local_data, uint64_t *remote_dims, uint64_t *remote_offset, uint64_t *remote_size, PDC_var_type_t remote_type, int32_t remote_client_id, void *remote_data, struct PDC_region_info *remote_region);

/**
 * Client request for object unmapping
 *
 * \param local_obj_id [IN]      The origin object id
 * \param pdc_id [IN]            The pdc id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_object_unmap(pdcid_t local_obj_id);

/**
 * Client request for object unmapping
 *
 * \param local_obj_id [IN]      The origin object id
 * \param local_obj_id [IN]      The origin region id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_region_unmap(pdcid_t local_obj_id, pdcid_t local_reg_id);

/**
 * Request of PDC client to get region lock
 *
 * \param obj_id [IN]           Id of the metadata
 * \param region_info [IN]      Pointer to PDC_region_info struct
 * \param access_type [IN]      Access type (enum)
 * \param lock_mode [IN]        Lock mode
 * \param obtained [OUT]        Lock granted or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_obtain_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_lock_mode_t lock_mode, pbool_t *obtained);

/**
 * Request of PDC client to get region lock
 *
 * \param obj_id [IN]           Id of the metadata
 * \param region_info [IN]      Pointer to PDC_region_info struct
 * \param access_type [IN]      Access type (enum)
 * \param released [OUT]        Lock released or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_release_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, pbool_t *released);

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
 * Request from PDC client to close server
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_close_all_server();




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
perr_t PDC_Client_data_server_read(int server_id, int n_client, pdc_metadata_t *meta, struct PDC_region_info *region); 

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
perr_t PDC_Client_data_server_write(int server_id, int n_client, pdc_metadata_t *meta, struct PDC_region_info *region, void *buf); 

/**
 * Client request server to collectively write a region of an object
 * and wait for server's push notification
 *
 * \param server_id [IN]         Target local data server ID
 * \param n_client [IN]          Number of clients that send to read request to one data server
 * \param meta [IN]              Metadata 
 * \param region [IN]            Region
 * \param buf[IN]                User buffer to store the read 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_write_wait_notify(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf);


/**
 * Client request server to check IO status of a previous IO request
 *
 * \param server_id [IN]         Target local data server ID
 * \param n_client [IN]          Client ID
 * \param meta [IN]              Metadata 
 * \param region [IN]            Region
 * \param buf[IN]                User buffer to store the read data
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_read_check(int server_id, uint32_t client_id, pdc_metadata_t *meta, struct PDC_region_info *region, int *status, void *buf);


/**
 * Client request server to check IO status of a previous IO request
 *
 * \param server_id [IN]         Target local data server ID
 * \param n_client [IN]          Client ID
 * \param meta [IN]              Metadata 
 * \param region [IN]            Region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_data_server_write_check(int server_id, uint32_t client_id, pdc_metadata_t *meta, struct PDC_region_info *region, int *status);

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

/**
 * Async request send to server to read a region and put it in users buffer
 *
 * \param meta [IN]              Metadata object pointer to the operating object
 * \param region [IN]            Region within the object to be read
 * \param request [IN]           Request structure to store the IO information 
 * \param buf [IN]               User's buffer to store data
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_iread(pdc_metadata_t *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf);

/**
 * Sync request send to server to read a region and put it in users buffer
 *
 * \param meta [IN]              Metadata object pointer to the operating object
 * \param region [IN]            Region within the object to be read
 * \param buf [IN]               User's buffer to store data
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_read(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf);

/**
 * Sync request send to server to write a region of object with users buffer
 *
 * \param meta [IN]              Metadata object pointer to the operating object
 * \param region [IN]            Region within the object to be read
 * \param request [IN]           Request structure to store the IO information 
 * \param buf [IN]               User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_iwrite(pdc_metadata_t *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf);

/**
 * Sync request send to server to write a region of object with users buffer
 *
 * \param meta [IN]              Metadata object pointer to the operating object
 * \param region [IN]            Region within the object to be read
 * \param buf [IN]               User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_write(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf);

/**
 * Sync request send to server to write a region of object with users buffer
 * and wait for server's push notification
 *
 * \param meta [IN]              Metadata object pointer to the operating object
 * \param region [IN]            Region within the object to be read
 * \param buf [IN]               User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_write_wait_notify(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf);

/**
 * Sync request send to server to read a region of object with users buffer
 * and wait for server's push notification
 *
 * \param meta [IN]              Metadata object pointer to the operating object
 * \param region [IN]            Region within the object to be read
 * \param buf [IN]               User's data to be written to storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_read_wait_notify(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf);


#endif
