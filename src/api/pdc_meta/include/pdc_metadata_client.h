#ifndef PDC_METADATA_CLIENT_H
#define PDC_METADATA_CLIENT_H

#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

typedef struct {
    int       n_res;
    uint64_t *out;
    char *    condition;
} separate_query_result_t;

size_t PDC_metadata_multi_condition_query(char *queryString, uint64_t **object_id_list, uint64_t *count);

#endif // PDC_METADATA_CLIENT_H