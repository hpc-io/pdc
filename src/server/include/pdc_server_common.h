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

#ifndef PDC_SERVER_COMMON_H
#define PDC_SERVER_COMMON_H

#define PDC_ALLOC_BASE_NUM         64
#define PDC_SERVER_TASK_INIT_VALUE 1000

/* extern hg_class_t *hg_class_g; */
/* extern hg_thread_mutex_t pdc_client_connect_mutex_g; */
typedef struct server_lookup_args_t {
    int             server_id;
    uint32_t        client_id;
    int             ret_int;
    char *          ret_string;
    void *          void_buf;
    char *          server_addr;
    pdc_metadata_t *meta;
    region_list_t **region_lists;
    uint32_t        n_loc;
    hg_handle_t     rpc_handle;
} server_lookup_args_t;

struct transfer_buf_map {
    hg_handle_t  handle;
    buf_map_in_t in;
};

typedef struct pdc_remote_server_info_t {
    char *    addr_string;
    int       addr_valid;
    hg_addr_t addr;
} pdc_remote_server_info_t;

typedef struct pdc_client_info_t {
    char      addr_string[ADDR_MAX];
    int       addr_valid;
    hg_addr_t addr;
} pdc_client_info_t;

#endif /* PDC_SERVER_COMMON_H */
