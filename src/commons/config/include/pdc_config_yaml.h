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

/**
 * pdc_config_yaml.h
 *
 * Unified runtime configuration for PDC (see GitHub issue hpc-io/pdc#307).
 *
 * Historically PDC's runtime policy knobs (cache sizing, flush intervals,
 * lustre striping, debug flags, communication transport/host, ...) were
 * scattered across dozens of individual environment variables, read ad hoc
 * from many different source files. This module centralizes all of that
 * into a single struct, populated once at client/server startup from
 * (in increasing priority):
 *
 *   1. compiled-in defaults
 *   2. the legacy, individual environment variables (kept for backward
 *      compatibility; see each field below)
 *   3. a YAML configuration file, whose path is given by the single
 *      environment variable PDC_CONFIG
 *
 * This struct only holds parameters that can meaningfully change without
 * recompiling PDC. Decisions that change which code paths/libraries get
 * compiled (e.g. whether Lustre, RocksDB, multithreading, ... support is
 * built at all) remain ordinary CMake options (see CMakeLists.txt) and are
 * not duplicated here.
 */

#ifndef PDC_CONFIG_YAML_H
#define PDC_CONFIG_YAML_H

#include "pdc_public.h"
#include "pdc_logger.h"

#define PDC_CONFIG_ENV_NAME "PDC_CONFIG"
#define PDC_CONFIG_STR_LEN  1024

typedef enum {
    PDC_CLIENT_LOOKUP_LOCAL = 0, /* each client looks up only its node-local server at start time */
    PDC_CLIENT_LOOKUP_ALL,       /* each client looks up every server at start time */
    PDC_CLIENT_LOOKUP_DISABLED   /* clients look up servers lazily, on demand */
} PDC_client_lookup_mode_t;

typedef struct PDC_runtime_config {
    /* ---- general ---- */
    int  debug;                       /* general.debug                legacy: PDC_DEBUG   */
    char tmp_dir[PDC_CONFIG_STR_LEN]; /* general.tmp_dir               legacy: PDC_TMPDIR  */

    /* Logging mode: how verbose PDC is, and where the log lines go. Previously
     * hardcoded to LOG_LEVEL_INFO / stdout in pdc_logger.h with no way to change
     * it at runtime (setLogLevel()/setLogFile() existed but were never called
     * outside of one test). */
    PDC_LogLevel log_level; /* general.log_level  (error|warning|info|debug) legacy: PDC_LOG_LEVEL */
    char         log_file
        [PDC_CONFIG_STR_LEN]; /* general.log_file   ("stdout"|"stderr"|path)  legacy: PDC_LOG_FILE   */

    /* ---- communication ---- */
    char     transport[PDC_CONFIG_STR_LEN]; /* communication.transport  legacy: HG_TRANSPORT         */
    char     hostname[PDC_CONFIG_STR_LEN];  /* communication.hostname   legacy: HG_HOST (""=auto)    */
    int      base_port;                     /* communication.base_port  legacy: none (was hardcoded) */
    int      port_range;                    /* communication.port_range legacy: none (was hardcoded) */
    uint32_t drc_key;                       /* communication.drc_key    legacy: PDC_DRC_KEY          */

    /* ---- client ---- */
    int nclient_per_server; /* client.nclient_per_server legacy: PDC_NCLIENT_PER_SERVER */
    PDC_client_lookup_mode_t client_lookup_mode; /* client.lookup_mode        legacy: PDC_CLIENT_LOOKUP     */
    int                      cache_percentage;   /* client.cache_percentage   legacy: PDC_CACHE_PERCENTAGE */

    /* ---- server ---- */
    int  nthread;                      /* server.threads                legacy: PDC_SERVER_NTHREAD    */
    int  nclient_per_node;             /* server.nclient_per_node        legacy: NCLIENT                */
    int  server_lookup_on_demand;      /* server.lookup_on_demand        legacy: PDC_LOOKUP_ON_DEMAND   */
    int  sort_read;                    /* server.sort_read               legacy: PDC_SORT_READ          */
    char data_loc[PDC_CONFIG_STR_LEN]; /* server.data_loc              legacy: PDC_DATA_LOC           */

    /* ---- server.checkpoint ---- */
    int checkpoint_enabled;   /* server.checkpoint.enabled    legacy: PDC_DISABLE_CHECKPOINT (inverted) */
    int checkpoint_use_tmpfs; /* server.checkpoint.use_tmpfs  legacy: PDC_CHECKPOINT_TMPFS              */

    /* ---- server.cache ---- */
    long cache_max_size_gb; /* server.cache.max_size_gb        legacy: PDC_SERVER_CACHE_MAX_SIZE        */
    int  cache_idle_flush_time_sec; /* server.cache.idle_flush_time_sec legacy:
                                       PDC_SERVER_IDLE_CACHE_FLUSH_TIME */
    int cache_disable_flush; /* server.cache.disable_flush      legacy: PDC_SERVER_CACHE_NO_FLUSH        */

    /* ---- backend.burst_buffer ---- */
    char bb_loc[PDC_CONFIG_STR_LEN]; /* backend.burst_buffer.path            legacy: PDC_BB_LOC            */
    int  bb_write_percentage;        /* backend.burst_buffer.write_percentage legacy: PDC_BB_WRITE_PERCENT */

    /* ---- backend.lustre ---- */
    int lustre_stripe_size_mb; /* backend.lustre.stripe_size_mb  legacy: PDC_LUSTRE_STRIPE_SIZE */
    int lustre_ost_per_file;   /* backend.lustre.ost_per_file    legacy: PDC_NOST_PER_FILE       */

    /* ---- backend.rocksdb / backend.sqlite3 ---- */
    int use_rocksdb; /* backend.rocksdb.enabled  legacy: PDC_USE_ROCKSDB */
    int use_sqlite3; /* backend.sqlite3.enabled  legacy: PDC_USE_SQLITE3 */

    /* ---- optimization ---- */
    int data_sieving;      /* optimization.data_sieving          legacy: PDC_DATA_SIEVING      */
    int gen_histogram;     /* optimization.build_histogram       legacy: PDC_GEN_HIST          */
    int gen_fastbit_index; /* optimization.build_fastbit_index   legacy: PDC_GEN_FASTBIT_IDX   */
    int use_fastbit_index; /* optimization.use_fastbit_index     legacy: PDC_USE_FASTBIT_IDX   */

    /* true once PDC_config_load() has populated this struct */
    int loaded;
} PDC_runtime_config_t;

extern PDC_runtime_config_t pdc_config_g;

/**
 * Populate pdc_config_g from compiled-in defaults, legacy environment
 * variables, and (if PDC_CONFIG is set) the YAML file it points to.
 * Safe to call multiple times; after the first call it is a no-op.
 * Both PDC_Client_init() and the server's PDC_Server_get_env() call this
 * before reading any of the fields above.
 */
perr_t PDC_config_load(void);

#endif /* PDC_CONFIG_YAML_H */
