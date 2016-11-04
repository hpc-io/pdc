#include "mercury.h"
#include "mercury_proc_string.h"

#ifndef PDC_CLIENT_CONNECT_H
#define PDC_CLIENT_CONNECT_H

#define pdc_server_cfg_name "./server.cfg"
#define ADDR_MAX 64

int PDC_Client_read_server_addr_from_file();
uint64_t PDC_Client_send_name_recv_id(int server_id, int port, const char *obj_name);

typedef struct pdc_server_info_t {
    char     addr_string[ADDR_MAX];
    /* int  has_mercury_handle; */
    /* hg_handle_t handle; */
} pdc_server_info_t;

extern int pdc_server_num_g;
extern pdc_server_info_t *pdc_server_info;

extern uint64_t pdc_id_seq_g;

extern int pdc_client_mpi_rank_g;
extern int pdc_client_mpi_size_g;

struct client_lookup_args {
    hg_class_t          *hg_class;
    hg_context_t        *hg_context;
    hg_addr_t           hg_target_addr;
    hg_const_string_t   obj_name;
    hg_uint64_t         obj_id;
};
#endif
