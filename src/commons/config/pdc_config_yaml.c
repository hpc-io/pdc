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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "pdc_config_yaml.h"
#include "pdc_logger.h"
#include "pdc_config.h"

#ifdef PDC_HAS_YAML_CONFIG
#include <libfyaml.h>
#endif

PDC_runtime_config_t pdc_config_g;

#ifdef PDC_HAS_YAML_CONFIG
static struct fy_node *fy_root_g = NULL;

static int
yaml_get_str(const char *path, char *out, size_t out_len)
{
    struct fy_node *n;
    const char *    s;

    if (fy_root_g == NULL)
        return 0;

    n = fy_node_by_path(fy_root_g, path, FY_NT, FYNWF_DONT_FOLLOW);
    if (n == NULL || !fy_node_is_scalar(n))
        return 0;

    s = fy_node_get_scalar0(n);
    if (s == NULL)
        return 0;

    strncpy(out, s, out_len - 1);
    out[out_len - 1] = '\0';
    return 1;
}
#else
static int
yaml_get_str(const char *path, char *out, size_t out_len)
{
    (void)path;
    (void)out;
    (void)out_len;
    return 0;
}
#endif

/* Accepts both "true"/"false" and plain integers, since a YAML config author
 * will naturally write booleans as `true`/`false` even for fields that are
 * stored internally as an int (to mirror the legacy env vars they replace). */
static int
yaml_get_long(const char *path, long *out)
{
    char buf[64];
    if (!yaml_get_str(path, buf, sizeof(buf)))
        return 0;
    if (strcasecmp(buf, "true") == 0) {
        *out = 1;
        return 1;
    }
    if (strcasecmp(buf, "false") == 0) {
        *out = 0;
        return 1;
    }
    *out = strtol(buf, NULL, 10);
    return 1;
}

/* Resolve one field as: yaml value > legacy env var > default. */
static void
resolve_long(const char *yaml_path, const char *legacy_env, long dflt, long *out)
{
    char *e;
    long  v;

    *out = dflt;

    e = legacy_env != NULL ? getenv(legacy_env) : NULL;
    if (e != NULL)
        *out = atol(e);

    if (yaml_get_long(yaml_path, &v))
        *out = v;
}

static void
resolve_str(const char *yaml_path, const char *legacy_env, const char *dflt, char *out, size_t out_len)
{
    char *e;

    strncpy(out, dflt, out_len - 1);
    out[out_len - 1] = '\0';

    e = legacy_env != NULL ? getenv(legacy_env) : NULL;
    if (e != NULL) {
        strncpy(out, e, out_len - 1);
        out[out_len - 1] = '\0';
    }

    yaml_get_str(yaml_path, out, out_len);
}

/* For legacy env vars whose mere *presence* toggles a feature on, regardless
 * of the value assigned to them (e.g. PDC_SORT_READ, PDC_LOOKUP_ON_DEMAND). */
static void
resolve_bool_presence(const char *yaml_path, const char *legacy_env, int dflt, int *out)
{
    long v;

    *out = dflt;

    if (legacy_env != NULL && getenv(legacy_env) != NULL)
        *out = 1;

    if (yaml_get_long(yaml_path, &v))
        *out = (v != 0);
}

/* For legacy env vars whose numeric *value* (atoi() != 0) decides the flag
 * (e.g. PDC_DEBUG, PDC_DATA_SIEVING). */
static void
resolve_bool_value(const char *yaml_path, const char *legacy_env, int dflt, int *out)
{
    char *e;
    long  v;

    *out = dflt;

    e = legacy_env != NULL ? getenv(legacy_env) : NULL;
    if (e != NULL)
        *out = (atoi(e) != 0);

    if (yaml_get_long(yaml_path, &v))
        *out = (v != 0);
}

static PDC_LogLevel
parse_log_level(const char *s, PDC_LogLevel dflt)
{
    if (s == NULL || s[0] == '\0')
        return dflt;
    if (strcasecmp(s, "error") == 0)
        return LOG_LEVEL_ERROR;
    if (strcasecmp(s, "warning") == 0)
        return LOG_LEVEL_WARNING;
    if (strcasecmp(s, "info") == 0)
        return LOG_LEVEL_INFO;
    if (strcasecmp(s, "debug") == 0)
        return LOG_LEVEL_DEBUG;
    return dflt;
}

perr_t
PDC_config_load(void)
{
    char default_cache_max_gb[32];
    char default_cache_flush_time[32];
    long tmp;
    int  disable_checkpoint;
    char log_level_str[32];
#ifdef PDC_HAS_YAML_CONFIG
    struct fy_document *fyd = NULL;
    const char *        path;
#endif

    if (pdc_config_g.loaded)
        return SUCCEED;

    memset(&pdc_config_g, 0, sizeof(pdc_config_g));

#ifdef PDC_HAS_YAML_CONFIG
    path = getenv(PDC_CONFIG_ENV_NAME);
    if (path != NULL) {
        fyd = fy_document_build_from_file(NULL, path);
        if (fyd == NULL)
            LOG_ERROR("%s is set to '%s' but it could not be parsed as YAML; "
                      "falling back to environment variables and defaults\n",
                      PDC_CONFIG_ENV_NAME, path);
        else
            fy_root_g = fy_document_root(fyd);
    }
#endif

    /* ---- general ---- */
    resolve_bool_value("/general/debug", "PDC_DEBUG", 0, &pdc_config_g.debug);
    resolve_str("/general/tmp_dir", "PDC_TMPDIR", "./pdc_tmp", pdc_config_g.tmp_dir,
                sizeof(pdc_config_g.tmp_dir));

    /* ---- general: logging mode (level + destination) ----
     * Previously hardcoded: logLevel defaulted to LOG_LEVEL_INFO in pdc_logger.h
     * and was never changed at runtime outside of a single test file. */
    strncpy(log_level_str, "info", sizeof(log_level_str) - 1);
    log_level_str[sizeof(log_level_str) - 1] = '\0';
    {
        char *e = getenv("PDC_LOG_LEVEL");
        if (e != NULL) {
            strncpy(log_level_str, e, sizeof(log_level_str) - 1);
            log_level_str[sizeof(log_level_str) - 1] = '\0';
        }
    }
    yaml_get_str("/general/log_level", log_level_str, sizeof(log_level_str));
    pdc_config_g.log_level = parse_log_level(log_level_str, LOG_LEVEL_INFO);

    resolve_str("/general/log_file", "PDC_LOG_FILE", "stdout", pdc_config_g.log_file,
                sizeof(pdc_config_g.log_file));

    setLogLevel(pdc_config_g.log_level);
    setLogFile(LOG_LEVEL_ERROR, pdc_config_g.log_file);
    setLogFile(LOG_LEVEL_WARNING, pdc_config_g.log_file);
    setLogFile(LOG_LEVEL_INFO, pdc_config_g.log_file);
    setLogFile(LOG_LEVEL_DEBUG, pdc_config_g.log_file);

    /* ---- communication ---- */
    resolve_str("/communication/transport", "HG_TRANSPORT", "ofi+tcp", pdc_config_g.transport,
                sizeof(pdc_config_g.transport));
    resolve_str("/communication/hostname", "HG_HOST", "", pdc_config_g.hostname,
                sizeof(pdc_config_g.hostname));
    resolve_long("/communication/base_port", NULL, 7000, &tmp);
    pdc_config_g.base_port = (int)tmp;
    resolve_long("/communication/port_range", NULL, 32, &tmp);
    pdc_config_g.port_range = (int)tmp;
    resolve_long("/communication/drc_key", "PDC_DRC_KEY", 0, &tmp);
    pdc_config_g.drc_key = (uint32_t)tmp;

    /* ---- client ---- */
    resolve_long("/client/nclient_per_server", "PDC_NCLIENT_PER_SERVER", 0, &tmp);
    pdc_config_g.nclient_per_server = (int)tmp;

    {
        char  lookup_mode[32] = "local";
        char *e               = getenv("PDC_CLIENT_LOOKUP");
        if (e != NULL) {
            strncpy(lookup_mode, e, sizeof(lookup_mode) - 1);
            lookup_mode[sizeof(lookup_mode) - 1] = '\0';
        }
        yaml_get_str("/client/lookup_mode", lookup_mode, sizeof(lookup_mode));

        if (strcasecmp(lookup_mode, "all") == 0)
            pdc_config_g.client_lookup_mode = PDC_CLIENT_LOOKUP_ALL;
        else if (strcasecmp(lookup_mode, "disabled") == 0)
            pdc_config_g.client_lookup_mode = PDC_CLIENT_LOOKUP_DISABLED;
        else
            pdc_config_g.client_lookup_mode = PDC_CLIENT_LOOKUP_LOCAL;
    }

    resolve_long("/client/cache_percentage", "PDC_CACHE_PERCENTAGE", 0, &tmp);
    pdc_config_g.cache_percentage = (int)tmp;

    /* ---- server ---- */
    resolve_long("/server/threads", "PDC_SERVER_NTHREAD", 4, &tmp);
    pdc_config_g.nthread = (int)tmp;
    resolve_long("/server/nclient_per_node", "NCLIENT", 31, &tmp);
    pdc_config_g.nclient_per_node = (int)tmp;
    resolve_bool_presence("/server/lookup_on_demand", "PDC_LOOKUP_ON_DEMAND", 0,
                          &pdc_config_g.server_lookup_on_demand);
    resolve_bool_presence("/server/sort_read", "PDC_SORT_READ", 0, &pdc_config_g.sort_read);
    resolve_str("/server/data_loc", "PDC_DATA_LOC", "", pdc_config_g.data_loc, sizeof(pdc_config_g.data_loc));

    /* ---- server.checkpoint ----
     * The legacy env var is a *disable* flag (PDC_DISABLE_CHECKPOINT=TRUE), while the
     * YAML key uses the more intuitive positive sense (enabled: true/false). */
    disable_checkpoint = 0;
    {
        char *e = getenv("PDC_DISABLE_CHECKPOINT");
        if (e != NULL && strcmp(e, "TRUE") == 0)
            disable_checkpoint = 1;
    }
    pdc_config_g.checkpoint_enabled = !disable_checkpoint;
    if (yaml_get_long("/server/checkpoint/enabled", &tmp))
        pdc_config_g.checkpoint_enabled = (tmp != 0);
    resolve_bool_value("/server/checkpoint/use_tmpfs", "PDC_CHECKPOINT_TMPFS", 0,
                       &pdc_config_g.checkpoint_use_tmpfs);

    /* ---- server.cache ----
     * Compiled-in defaults come from the CMake PDC_SERVER_CACHE_MAX_GB /
     * PDC_SERVER_IDLE_CACHE_FLUSH_TIME options so packagers can still bake in a
     * different baseline without touching a config file. */
#ifdef PDC_SERVER_CACHE_MAX_GB
    snprintf(default_cache_max_gb, sizeof(default_cache_max_gb), "%d", PDC_SERVER_CACHE_MAX_GB);
#else
    snprintf(default_cache_max_gb, sizeof(default_cache_max_gb), "%d", 32);
#endif
#ifdef PDC_SERVER_IDLE_CACHE_FLUSH_TIME
    snprintf(default_cache_flush_time, sizeof(default_cache_flush_time), "%d",
             PDC_SERVER_IDLE_CACHE_FLUSH_TIME);
#else
    snprintf(default_cache_flush_time, sizeof(default_cache_flush_time), "%d", 2);
#endif
    resolve_long("/server/cache/max_size_gb", "PDC_SERVER_CACHE_MAX_SIZE", atol(default_cache_max_gb),
                 &pdc_config_g.cache_max_size_gb);
    resolve_long("/server/cache/idle_flush_time_sec", "PDC_SERVER_IDLE_CACHE_FLUSH_TIME",
                 atol(default_cache_flush_time), &tmp);
    pdc_config_g.cache_idle_flush_time_sec = (int)tmp;
    resolve_bool_value("/server/cache/disable_flush", "PDC_SERVER_CACHE_NO_FLUSH", 0,
                       &pdc_config_g.cache_disable_flush);

    /* ---- backend.burst_buffer ---- */
    resolve_str("/backend/burst_buffer/path", "PDC_BB_LOC", "", pdc_config_g.bb_loc,
                sizeof(pdc_config_g.bb_loc));
    resolve_long("/backend/burst_buffer/write_percentage", "PDC_BB_WRITE_PERCENT", 0, &tmp);
    pdc_config_g.bb_write_percentage = (int)tmp;
    if (pdc_config_g.bb_write_percentage < 0 || pdc_config_g.bb_write_percentage > 100)
        pdc_config_g.bb_write_percentage = 0;

    /* ---- backend.lustre ---- */
    resolve_long("/backend/lustre/stripe_size_mb", "PDC_LUSTRE_STRIPE_SIZE", 16, &tmp);
    pdc_config_g.lustre_stripe_size_mb = (int)tmp;
    if (pdc_config_g.lustre_stripe_size_mb < 1 || pdc_config_g.lustre_stripe_size_mb > 128)
        pdc_config_g.lustre_stripe_size_mb = 16;
    resolve_long("/backend/lustre/ost_per_file", "PDC_NOST_PER_FILE", 0, &tmp);
    pdc_config_g.lustre_ost_per_file = (int)tmp;

    /* ---- backend.rocksdb / backend.sqlite3 ---- */
    {
        char *e                  = getenv("PDC_USE_ROCKSDB");
        pdc_config_g.use_rocksdb = (e != NULL && strcmp(e, "1") == 0);
        if (yaml_get_long("/backend/rocksdb/enabled", &tmp))
            pdc_config_g.use_rocksdb = (tmp != 0);
    }
    {
        char *e                  = getenv("PDC_USE_SQLITE3");
        pdc_config_g.use_sqlite3 = (e != NULL && strcmp(e, "1") == 0);
        if (yaml_get_long("/backend/sqlite3/enabled", &tmp))
            pdc_config_g.use_sqlite3 = (tmp != 0);
    }

    /* ---- optimization ---- */
    resolve_bool_value("/optimization/data_sieving", "PDC_DATA_SIEVING", 1, &pdc_config_g.data_sieving);
    resolve_bool_value("/optimization/build_histogram", "PDC_GEN_HIST", 0, &pdc_config_g.gen_histogram);
    resolve_bool_presence("/optimization/build_fastbit_index", "PDC_GEN_FASTBIT_IDX", 0,
                          &pdc_config_g.gen_fastbit_index);
    resolve_bool_presence("/optimization/use_fastbit_index", "PDC_USE_FASTBIT_IDX", 0,
                          &pdc_config_g.use_fastbit_index);

#ifdef PDC_HAS_YAML_CONFIG
    if (fyd != NULL)
        fy_document_destroy(fyd);
    fy_root_g = NULL;
#endif

    pdc_config_g.loaded = 1;
    return SUCCEED;
}
