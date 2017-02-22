#include "mercury.h"
#include "mercury_proc_string.h"

#include "pdc.h"
#include "pdc_prop.h"
#include "pdc_client_server_common.h"

#ifndef PDC_CLIENT_CONNECT_H
#define PDC_CLIENT_CONNECT_H


int PDC_Client_read_server_addr_from_file();
uint64_t PDC_Client_send_name_recv_id(pdcid_t pdc, pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop);
perr_t PDC_Client_query_metadata_name_timestep(const char *obj_name, int time_step, pdc_metadata_t **out);
perr_t PDC_Client_query_metadata_name_only(const char *obj_name, pdc_metadata_t **out);
perr_t PDC_Client_delete_metadata(pdcid_t pdc, pdcid_t cont_id, char *delete_name, pdcid_t obj_delete_prop);
perr_t PDC_Client_delete_metadata_by_id(pdcid_t pdc, pdcid_t cont_id, uint64_t obj_id);
perr_t PDC_Client_update_metadata(pdc_metadata_t *old, pdc_metadata_t *new);
perr_t PDC_Client_init();
perr_t PDC_Client_finalize();
perr_t PDC_Client_close_all_server();

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
    int             name_marker_handle_valid;
    hg_handle_t     name_marker_handle;
    int             metadata_query_handle_valid;
    hg_handle_t     metadata_query_handle;
    int             metadata_delete_handle_valid;
    hg_handle_t     metadata_delete_handle;
    int             metadata_delete_by_id_handle_valid;
    hg_handle_t     metadata_delete_by_id_handle;
    int             metadata_update_handle_valid;
    hg_handle_t     metadata_update_handle;
} pdc_server_info_t;

extern int pdc_server_num_g;
extern pdc_server_info_t *pdc_server_info_g;

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

extern char pdc_server_tmp_dir_g[ADDR_MAX];

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
#endif
