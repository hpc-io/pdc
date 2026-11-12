#ifndef PDC_MERCURY_AUTH_H
#define PDC_MERCURY_AUTH_H

#include <stddef.h>

#include "pdc_public.h"

#define PDC_PERLMUTTER_CXI_ENV_COUNT 3

typedef struct pdc_scoped_env_entry_t {
    const char *name;
    char *      old_value;
    pbool_t     had_old_value;
} pdc_scoped_env_entry_t;

perr_t PDC_scope_perlmutter_cxi_auth_env(const char *           hg_transport,
                                         pdc_scoped_env_entry_t envs[PDC_PERLMUTTER_CXI_ENV_COUNT],
                                         int *env_count, unsigned int *svc_id, unsigned int *vni,
                                         char *device_buf, size_t device_buf_len);

void PDC_restore_scoped_env(pdc_scoped_env_entry_t envs[PDC_PERLMUTTER_CXI_ENV_COUNT], int env_count);

#endif
