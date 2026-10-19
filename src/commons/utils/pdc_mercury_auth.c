#include "pdc_mercury_auth.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pdc_private.h"

static pbool_t
PDC_is_cxi_mercury_transport(const char *hg_transport)
{
    return (hg_transport != NULL &&
            (strcmp(hg_transport, "ofi+cxi") == 0 || strcmp(hg_transport, "cxi") == 0));
}

static pbool_t
PDC_is_perlmutter_system(void)
{
    const char *nersc_host = getenv("NERSC_HOST");

    return (nersc_host != NULL && strcmp(nersc_host, "perlmutter") == 0);
}

static void
PDC_scope_env_set(pdc_scoped_env_entry_t *entry, const char *name, const char *value)
{
    const char *old_value = getenv(name);

    entry->name          = name;
    entry->had_old_value = (old_value != NULL) ? TRUE : FALSE;
    entry->old_value     = (old_value != NULL) ? strdup(old_value) : NULL;

    if (value != NULL)
        setenv(name, value, 1);
    else
        unsetenv(name);
}

static void
PDC_scope_env_restore(pdc_scoped_env_entry_t *entry)
{
    if (entry->had_old_value == TRUE)
        setenv(entry->name, entry->old_value, 1);
    else
        unsetenv(entry->name);

    free(entry->old_value);
    entry->old_value = NULL;
}

static perr_t
PDC_discover_perlmutter_cxi_auth(unsigned int *svc_id, unsigned int *vni)
{
    perr_t ret_value = FAIL;
    FILE * fp        = NULL;
    char   line[256];
    char   uid_pattern[64];

    unsigned int current_svc_id = 0;
    unsigned int current_vni    = 0;
    pbool_t      enabled        = FALSE;
    pbool_t      system_service = FALSE;
    pbool_t      restricted_mem = TRUE;
    pbool_t      member_match   = FALSE;

    if (svc_id == NULL || vni == NULL)
        PGOTO_ERROR(FAIL, "Invalid output pointers for CXI auth discovery\n");

    snprintf(uid_pattern, sizeof(uid_pattern), "uid=%u", (unsigned int)geteuid());

    fp = popen("cxi_service -v list 2>/dev/null", "r");
    if (fp == NULL)
        PGOTO_ERROR(FAIL, "Could not execute cxi_service\n");

    while (fgets(line, sizeof(line), fp) != NULL) {
        unsigned int parsed_value = 0;

        if (sscanf(line, " ID: %u", &parsed_value) == 1) {
            current_svc_id = parsed_value;
            current_vni    = 0;
            enabled        = FALSE;
            system_service = FALSE;
            restricted_mem = TRUE;
            member_match   = FALSE;
            continue;
        }

        if (strstr(line, "Enabled") != NULL) {
            enabled = (strstr(line, "Yes") != NULL) ? TRUE : FALSE;
            continue;
        }

        if (strstr(line, "System Service") != NULL) {
            system_service = (strstr(line, "Yes") != NULL) ? TRUE : FALSE;
            continue;
        }

        /* Must be checked before "Valid Members" -- "Restricted Members : No"
         * means the service has no member restriction at all, so its "Valid
         * Members" line reads "All uids/gids" instead of an explicit uid
         * list and never contains uid_pattern. */
        if (strstr(line, "Restricted Members") != NULL) {
            restricted_mem = (strstr(line, "Yes") != NULL) ? TRUE : FALSE;
            continue;
        }

        if (strstr(line, "Valid Members") != NULL) {
            member_match = (restricted_mem == FALSE || strstr(line, uid_pattern) != NULL) ? TRUE : FALSE;
            continue;
        }

        /* Real `cxi_service -v list` output has a plain "VNIs : <ids...>"
         * field directly under the service ID block (confirmed against
         * real Perlmutter output) -- not a "--->"-prefixed "Valid VNIs"
         * restriction sub-line like "Valid Members"/"Valid TCs" use. A
         * service can list multiple space-separated VNIs (e.g.
         * "VNIs : 1 10"); %u here only captures the first one, which is
         * sufficient since only one VNI is needed for SLINGSHOT_VNIS. */
        if (sscanf(line, " VNIs : %u", &parsed_value) == 1) {
            current_vni = parsed_value;
            if (enabled == TRUE && system_service == FALSE && member_match == TRUE) {
                *svc_id   = current_svc_id;
                *vni      = current_vni;
                ret_value = SUCCEED;
                break;
            }
        }
    }

done:
    if (fp != NULL)
        pclose(fp);

    return ret_value;
}

perr_t
PDC_scope_perlmutter_cxi_auth_env(const char *           hg_transport,
                                  pdc_scoped_env_entry_t envs[PDC_PERLMUTTER_CXI_ENV_COUNT], int *env_count,
                                  unsigned int *svc_id, unsigned int *vni, char *device_buf,
                                  size_t device_buf_len)
{
    perr_t       ret_value = SUCCEED;
    const char * nic_name  = getenv("HG_CXI_NIC");
    unsigned int local_svc_id;
    unsigned int local_vni;
    char         svc_buf[32];
    char         vni_buf[32];

    if (env_count == NULL)
        PGOTO_ERROR(FAIL, "Invalid env count pointer for CXI auth scoping\n");

    *env_count = 0;

    if (svc_id != NULL)
        *svc_id = 0;
    if (vni != NULL)
        *vni = 0;
    if (device_buf != NULL && device_buf_len > 0)
        device_buf[0] = '\0';

    if (PDC_is_perlmutter_system() == FALSE || PDC_is_cxi_mercury_transport(hg_transport) == FALSE)
        return SUCCEED;

    if (nic_name == NULL || nic_name[0] == '\0')
        nic_name = "cxi0";

    if (PDC_discover_perlmutter_cxi_auth(&local_svc_id, &local_vni) != SUCCEED)
        PGOTO_ERROR(FAIL, "Unable to discover Perlmutter CXI service/VNI\n");

    snprintf(svc_buf, sizeof(svc_buf), "%u", local_svc_id);
    snprintf(vni_buf, sizeof(vni_buf), "%u", local_vni);

    PDC_scope_env_set(&envs[(*env_count)++], "SLINGSHOT_VNIS", vni_buf);
    PDC_scope_env_set(&envs[(*env_count)++], "SLINGSHOT_SVC_IDS", svc_buf);
    PDC_scope_env_set(&envs[(*env_count)++], "SLINGSHOT_DEVICES", nic_name);

    if (svc_id != NULL)
        *svc_id = local_svc_id;
    if (vni != NULL)
        *vni = local_vni;
    if (device_buf != NULL && device_buf_len > 0) {
        snprintf(device_buf, device_buf_len, "%s", nic_name);
    }

done:
    if (ret_value != SUCCEED)
        PDC_restore_scoped_env(envs, *env_count);

    return ret_value;
}

void
PDC_restore_scoped_env(pdc_scoped_env_entry_t envs[PDC_PERLMUTTER_CXI_ENV_COUNT], int env_count)
{
    while (env_count > 0) {
        env_count--;
        PDC_scope_env_restore(&envs[env_count]);
    }
}
