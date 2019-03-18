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
#include "mercury_request.h"

#include "pdc.h"
#include "pdc_prop.h"
#include "pdc_obj_pkg.h"
#include "pdc_client_server_common.h"
#include "pdc_client_public.h"


extern int pdc_server_num_g;

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

/* extern char pdc_server_tmp_dir_g[ADDR_MAX]; */
hg_return_t PDC_Client_work_done_cb();
hg_return_t PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info);

typedef struct pdc_server_info_t {
    char            addr_string[ADDR_MAX];
    int             addr_valid;
    hg_addr_t       addr;
} pdc_server_info_t;

extern pdc_server_info_t *pdc_server_info_g;

typedef struct pdc_client {
    pdcid_t         pdc;
    int             client_rank;
    int             client_port;
    char         *  client_addr;
/*  Some Mercury related info  */
    hg_class_t   *  hg_class;
    hg_context_t *  hg_context;
    hg_addr_t       hg_addr;
    hg_handle_t     client_handle;
} pdc_client_t;

extern pdc_client_t *pdc_client_direct_channels;

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
    int                  time_step;
    uint32_t             hash_value;
    const char          *tags;

    hg_request_t        *request;
};

typedef struct metadata_query_args_t {
    pdc_metadata_t *data;
} metadata_query_args_t;

typedef struct container_query_args_t {
    uint64_t cont_id;
} container_query_args_t;


/* typedef struct client_name_cache_t { */
/*     char                        name[ADDR_MAX]; */
/*     struct client_name_cache_t *prev; */
/*     struct client_name_cache_t *next; */
/* } client_name_cache_t; */

struct buf_map_args {
    int32_t     ret;
};

struct region_map_args {
	int         ret;
};

struct region_unmap_args {
    int         ret;
};

struct object_unmap_args {
    int32_t     ret;
};

struct region_lock_args {
    pbool_t     *status;
    int         ret;
};

struct region_release_args {
    pbool_t     *status;
    int         ret;
};

typedef struct pdc_get_kvtag_args_t{
    int ret;
    pdc_kvtag_t *kvtag;
} pdc_get_kvtag_args_t;

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
 * \param obj_create_prop [IN]  Id of the object property
 * \param meta_id [OUT]         Pointer to medadata id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_name_recv_id(const char *obj_name, uint64_t cont_id, pdcid_t obj_create_prop, pdcid_t *meta_id);
/**
 * Apply a map from buffer to object
 *
 * \param buf [IN]              Memory address
 * \param region_info [IN]      Pointer to PDC_region_info struct
 * \param access_type [IN]      Access type (enum)
 * \param released [OUT]        Lock released or not
 *
 * \return Non-negative on success/Negative on failure
 */
//perr_t PDC_Client_buf_map(pdcid_t local_region_id, pdcid_t remote_obj_id, pdcid_t remote_region_id, size_t ndim, uint64_t *local_dims, uint64_t *local_offset, uint64_t *local_size, PDC_var_type_t local_type, void *local_data, uint64_t *remote_dims, uint64_t *remote_offset, uint64_t *remote_size, PDC_var_type_t remote_type, int32_t remote_client_id, void *remote_data, struct PDC_region_info *local_region, struct PDC_region_info *remote_region);
perr_t PDC_Client_buf_map(pdcid_t local_region_id, pdcid_t remote_obj_id, size_t ndim, uint64_t *local_dims, uint64_t *local_offset, uint64_t *local_size, PDC_var_type_t local_type, void *local_data, PDC_var_type_t remote_type, struct PDC_region_info *local_region, struct PDC_region_info *remote_region);

/**
 * Client request for object mapping
 *
 * \param local_obj_id [IN]     The origin object id
 * \param local_region_id [IN]  The origin region id
 * \param remote_obj_id [IN]    The target object id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_region_map(pdcid_t local_obj_id, pdcid_t local_region_id, pdcid_t remote_obj_id, size_t ndim, uint64_t *local_dims, uint64_t *local_offset, uint64_t *local_size, PDC_var_type_t local_type, void *local_data, uint64_t *remote_dims, uint64_t *remote_offset, uint64_t *remote_size, PDC_var_type_t remote_type, void *remote_data, struct PDC_region_info *local_region, struct PDC_region_info *remote_region);

/**
 * Client request for buffer to object unmapping
 *
 * \param remote_obj_id [IN]     The target object id
 * \param remote_reg_id [IN]     The target region id
 * \param reginfo [IN]           The target region info
 * \param data_type [IN]         The target data type
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_buf_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id, struct PDC_region_info *reginfo, PDC_var_type_t data_type);

/**
 * Client request for object to buffer unmapping
 *
 * \param remote_obj_id [IN]     The target object id
 * \param remote_reg_id [IN]     The target region id
 * \param reginfo [IN]           The target region info
 * \param data_type [IN]         The target data type
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_obj_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id, struct PDC_region_info *reginfo, PDC_var_type_t data_type);

/**
 * Client request for object unmapping
 *
 * \param local_obj_id [IN]      The origin object id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_object_unmap(pdcid_t local_obj_id);

/**
 * Client request for object unmapping
 *
 * \param local_obj_id [IN]      The origin object id
 * \param local_obj_id [IN]      The origin region id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_region_unmap(pdcid_t local_obj_id, pdcid_t local_reg_id, struct PDC_region_info *reginfo, PDC_var_type_t data_type);

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
//perr_t PDC_Client_obtain_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_lock_mode_t lock_mode, PDC_var_type_t data_type, pbool_t *obtained);
perr_t PDC_Client_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_lock_mode_t lock_mode, PDC_var_type_t data_type, pbool_t *obtained);

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
perr_t PDC_Client_release_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_var_type_t data_type, pbool_t *released);

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

/*
 * Copy the metadata to local object property, internal function
 *
 * \param obj_name[IN]           Object name
 * \param obj_id[IN]             Object id (global, obtained from metadata server)
 * \param cont_id[IN]            Container id (global, obtained from metadata server)
 * \param obj_prop[IN]           Object property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_attach_metadata_to_local_obj(char *obj_name, uint64_t obj_id, uint64_t cont_id, 
                                               struct PDC_obj_info *obj_info);


/* perr_t PDCbuf_destroy(void *buf); */
/* perr_t PDClocidx_destroy(void *loc_idx); */

hg_return_t PDC_Client_recv_bulk_storage_meta_cb(const struct hg_cb_info *callback_info);
perr_t PDC_Client_recv_bulk_storage_meta(process_bulk_storage_meta_args_t *process_args);
perr_t PDC_Client_query_name_read_entire_obj_client(int nobj, char **obj_names, void ***out_buf, 
                                             uint64_t *out_buf_sizes);

perr_t PDC_Client_read_overlap_regions(uint32_t ndim, uint64_t *req_start, uint64_t *req_count,
                                       uint64_t *storage_start, uint64_t *storage_count,
                                       FILE *fp, uint64_t file_offset, void *buf,  size_t *total_read_bytes);

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
perr_t PDC_Client_query_tag(const char* tags, int *n_res, pdc_metadata_t ***out);

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

/*
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_partial_query(int is_list_all, int user_id, const char* app_name, const char* obj_name, int time_step_from, int time_step_to, int ndim, const char* tags, int *n_res, pdc_metadata_t ***out);


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


perr_t PDC_Client_query_container_name(char *cont_name, uint64_t *cont_meta_id);
perr_t PDC_Client_query_container_name_col(const char *cont_name, uint64_t *cont_meta_id);
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
 * Generate container ID by server
 *
 * \param cont_name [IN]         Container name
 * \param cont_create_prop [IN]  Container property ID
 * \param cont_id [OUT]          Generated container ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_create_cont_id(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id);

perr_t PDC_Client_create_cont_id_mpi(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id);

perr_t PDC_Client_query_kvtag(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids); 
perr_t PDC_Client_query_kvtag_col(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids);

perr_t PDC_Client_query_name_read_entire_obj_client_agg_cache_iter(int my_nobj, char **my_obj_names, 
                                                                   void ***out_buf, size_t *out_buf_sizes, 
                                                                   int cache_percentage);

perr_t PDC_Client_query_container_name_col(const char *cont_name, uint64_t *cont_meta_id);
perr_t PDC_send_data_query(pdcquery_t *query, pdcquery_get_op_t get_op, uint64_t *nhits, pdcselection_t *sel, void *data);

typedef struct pdcquery_result_list_t {
    uint32_t ndim;
    int      query_id;
    uint64_t nhits;
    uint64_t *coords;
    void     *data;

    struct pdcquery_result_list_t *prev;
    struct pdcquery_result_list_t *next;
} pdcquery_result_list_t;

hg_return_t PDC_recv_coords(const struct hg_cb_info *callback_info);
void PDCselection_free(pdcselection_t *sel);

#endif
