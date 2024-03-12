#ifndef PDC_METADATA_CLIENT_H
#define PDC_METADATA_CLIENT_H

#include "pdc_client_connect.h"

typedef struct PDC_metadata_query_pe_info {
    int client_rank;
    int client_size;
    int number_of_servers;
} PDC_metadata_query_pe_info;

size_t PDC_metadata_multi_condition_query(char *queryString, PDC_metadata_query_pe_info pe_info,
                                          uint64_t **object_id_list, uint64_t *count);

#endif // PDC_METADATA_CLIENT_H