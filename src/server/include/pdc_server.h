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

#ifndef PDC_SERVER_H
#define PDC_SERVER_H

#include <time.h>

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"

#include "mercury_atomic.h"
#include "mercury_list.h"

#include "pdc_hash-table.h"

#include "pdc_client_server_common.h"
#include "pdc_server_common.h"
#include "pdc_server_metadata.h"
#include "pdc_server_data.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#ifdef ENABLE_FASTBIT
#include "iapi.h"
#endif

#ifdef ENABLE_MULTITHREAD
// Mercury multithread
#include "mercury_thread.h"
#include "mercury_thread_pool.h"
#include "mercury_thread_mutex.h"
#include "mercury_thread_condition.h"

/*****************************/
/* Library-private Variables */
/*****************************/
hg_thread_mutex_t hash_table_new_mutex_g;
hg_thread_mutex_t pdc_client_addr_mutex_g;
hg_thread_mutex_t pdc_metadata_hash_table_mutex_g;
hg_thread_mutex_t pdc_container_hash_table_mutex_g;
hg_thread_mutex_t pdc_time_mutex_g;
hg_thread_mutex_t pdc_bloom_time_mutex_g;
hg_thread_mutex_t n_metadata_mutex_g;
hg_thread_mutex_t gen_obj_id_mutex_g;
hg_thread_mutex_t total_mem_usage_mutex_g;
hg_thread_mutex_t data_read_list_mutex_g;
hg_thread_mutex_t data_write_list_mutex_g;
hg_thread_mutex_t region_struct_mutex_g;
hg_thread_mutex_t data_buf_map_mutex_g;
hg_thread_mutex_t data_buf_unmap_mutex_g;
hg_thread_mutex_t data_obj_map_mutex_g;
hg_thread_mutex_t insert_hash_table_mutex_g;
hg_thread_mutex_t lock_request_mutex_g;
hg_thread_mutex_t addr_valid_mutex_g;
hg_thread_mutex_t update_remote_server_addr_mutex_g;
hg_thread_mutex_t pdc_server_task_mutex_g;
#else
#define hg_thread_mutex_t int
hg_thread_mutex_t pdc_server_task_mutex_g;
#endif

extern int      n_bloom_total_g;
extern int      n_bloom_maybe_g;
extern double   server_bloom_check_time_g;
extern double   server_bloom_insert_time_g;
extern double   server_insert_time_g;
extern double   server_delete_time_g;
extern double   server_update_time_g;
extern double   server_hash_insert_time_g;
extern double   server_bloom_init_time_g;
extern uint32_t n_metadata_g;

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * ***********
 *
 * \param remote_server_id [IN] Remote server ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_lookup_server_id(int remote_server_id);

/**
 * ***********
 *
 * \param client_id [IN]        Client ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_lookup_client(uint32_t client_id);

/**
 * ***********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_set_close(void);

/**
 * ***********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_checkpoint();

/**
 * ***********
 *
 * \param filename [IN]         File name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_restart(char *filename);

/**
 * ***********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_get_client_addr(const struct hg_cb_info *callback_info);

/**
 * ***********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_work_done_cb(const struct hg_cb_info *callback_info);

/**
 * ***********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_s2s_send_work_done_cb(const struct hg_cb_info *callback_info);

/**
 * ***********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_s2s_recv_work_done_cb(const struct hg_cb_info *callback_info);

/**
 * ***********
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_checkpoint_cb();

/**
 * ***********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_recv_shm_cb(const struct hg_cb_info *callback_info);

#endif /* PDC_SERVER_H */
