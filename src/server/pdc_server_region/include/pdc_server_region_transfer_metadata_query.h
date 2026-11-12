#ifndef PDC_SERVER_REGION_TRANSFER_METADATA_QUERY_H
#define PDC_SERVER_REGION_TRANSFER_METADATA_QUERY_H

#include "bulki.h"
#include "bulki_serde.h"

perr_t   transfer_request_metadata_query_init(int pdc_server_size_input, char *checkpoint);
perr_t   transfer_request_metadata_query_init_bulki(int pdc_server_size_input, BULKI *checkpoint_bulki);
perr_t   transfer_request_metadata_query_finalize();
perr_t   transfer_request_metadata_query_checkpoint(char **checkpoint, uint64_t *checkpoint_size);
perr_t   transfer_request_metadata_query_checkpoint_bulki(BULKI **checkpoint_bulki);
perr_t   transfer_request_metadata_query_lookup_query_buf(uint64_t query_id, char **buf_ptr);
uint64_t transfer_request_metadata_query_parse(int32_t n_objs, char *buf, uint8_t partition_type,
                                               uint64_t *total_buf_size_ptr);

#endif
