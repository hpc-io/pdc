#include "mercury.h"
#include "mercury_proc_string.h"

#include "pdc.h"
#include "pdc_prop.h"
#include "pdc_client_server_common.h"

#ifndef PDC_CLIENT_CONNECT_H
#define PDC_CLIENT_CONNECT_H


int PDC_Client_read_server_addr_from_file();
uint64_t PDC_Client_send_name_recv_id(const char *obj_name, pdcid_t property);
perr_t PDC_Client_init();
perr_t PDC_Client_finalize();
perr_t PDC_Client_close_all_server();

typedef struct pdc_server_info_t {
    char            addr_string[ADDR_MAX];
    int             addr_valid;
    hg_addr_t       addr;
    int             rpc_handle_valid;
    hg_handle_t     rpc_handle;
} pdc_server_info_t;

extern int pdc_server_num_g;
extern pdc_server_info_t *pdc_server_info_g;

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

struct client_lookup_args {
    const char          *obj_name;
    uint64_t             obj_id;
    int                  server_id;

    uint32_t             user_id;
    const char          *app_name;
    uint32_t             time_step;
    uint32_t             hash_value;
    const char          *tags;
};
#endif
