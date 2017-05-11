#include "mercury.h"
#include "mercury_proc_string.h"

#include "pdc.h"
#include "pdc_prop.h"
#include "pdc_obj_pkg.h"
#include "pdc_client_server_common.h"

#ifndef PDC_CLIENT_CONNECT_H
#define PDC_CLIENT_CONNECT_H

extern int pdc_server_num_g;

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

extern char pdc_server_tmp_dir_g[ADDR_MAX];

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
    int             query_partial_handle_valid;
    hg_handle_t     query_partial_handle;
} pdc_server_info_t;

extern pdc_server_info_t *pdc_server_info_g;

struct client_lookup_args {
    const char          *obj_name;
    uint64_t             obj_id;
    uint32_t             server_id;
    int                  client_id;
    int                  ret;

    uint32_t             user_id;
    const char          *app_name;
    uint32_t             time_step;
    uint32_t             hash_value;
    const char          *tags;
};

typedef struct metadata_query_args_t {
    pdc_metadata_t *data;
} metadata_query_args_t;

typedef struct client_name_cache_t {
    char                        name[ADDR_MAX];
    struct client_name_cache_t *prev;
    struct client_name_cache_t *next;
} client_name_cache_t;

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
int PDC_Client_read_server_addr_from_file();

/**
 * Client request of an obj id by sending object name
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_id [IN]          Id of the container
 * \param obj_name [IN]         Name of the object
 * \param obj_create_prop [IN]  Id of the object property
 * \param meta_id [IN]          Pointer to medadata id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_name_recv_id(pdcid_t pdc_id, pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop, pdcid_t *meta_id);

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
perr_t PDC_Client_query_metadata_name_timestep(const char *obj_name, int time_step, pdc_metadata_t **out);

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
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_id [IN]          Id of the container
 * \param delete_name [IN]      Name to delete
 * \param obj_delete_prop [IN]  Id of the associated property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_delete_metadata(pdcid_t pdc_id, pdcid_t cont_id, char *delete_name, pdcid_t obj_delete_prop);

/**
 * Request of PDC client to delete metadata by object id
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_id [IN]          Id of the container
 * \param obj_id [IN]           Id of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_delete_metadata_by_id(pdcid_t pdc_id, pdcid_t cont_id, uint64_t obj_id);

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
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_region_map(pdcid_t local_obj_id, pdcid_t local_region_id, pdcid_t remote_obj_id, pdcid_t remote_region_id, size_t ndim, uint64_t *dims, uint64_t *local_offset, uint64_t *size, PDC_var_type_t local_type, void *local_data, uint64_t *remote_offset, PDC_var_type_t remote_type);
/**
 * Client request for object unmapping
 *
 * \param local_obj_id [IN]      The origin object id
 * \param pdc_id [IN]            The pdc id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_object_unmap(pdcid_t local_obj_id, pdcid_t pdc_id);

/**
 * Client request for object unmapping
 *
 * \param local_obj_id [IN]      The origin object id
 * \param local_obj_id [IN]      The origin region id
 * \param pdc_id [IN]            The pdc id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_send_region_unmap(pdcid_t local_obj_id, pdcid_t local_reg_id, pdcid_t pdc_id);

/**
 * Request of PDC client to get region lock
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_id [IN]          Id of the container
 * \param obj_id [IN]           Id of the metadata
 * \param region_info [IN]      Pointer to PDC_region_info_t struct
 * \param access_type [IN]      Access type (enum)
 * \param lock_mode [IN]        Lock mode
 * \param obtained [OUT]        Lock granted or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_obtain_region_lock(pdcid_t pdc_id, pdcid_t cont_id, pdcid_t meta_id, PDC_region_info_t *region_info, PDC_access_t access_type, PDC_lock_mode_t lock_mode, pbool_t *obtained);

/**
 * Request of PDC client to get region lock
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_id [IN]          Id of the container
 * \param obj_id [IN]           Id of the metadata
 * \param region_info [IN]      Pointer to PDC_region_info_t struct
 * \param access_type [IN]      Access type (enum)
 * \param released [OUT]        Lock released or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Client_release_region_lock(pdcid_t pdc_id, pdcid_t cont_id, pdcid_t meta_id, PDC_region_info_t *region_info, PDC_access_t access_type, pbool_t *released);

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

#endif
