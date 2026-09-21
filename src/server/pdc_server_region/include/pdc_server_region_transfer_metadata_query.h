#ifndef PDC_SERVER_REGION_TRANSFER_METADATA_QUERY_H
#define PDC_SERVER_REGION_TRANSFER_METADATA_QUERY_H

#include "bulki.h"
#include "bulki_serde.h"
#include "pdc_public.h"

perr_t transfer_request_metadata_query_init(int pdc_server_size_input, char *checkpoint);
perr_t transfer_request_metadata_query_init_bulki(int pdc_server_size_input, BULKI *checkpoint_bulki);
/**
 * Elastic restart install: init for n_new, load checkpointed objects, remap each
 * region's data_server_id with (id % n_new), and rebuild load counters from
 * region extents (unit=1). checkpoint_bulki may be NULL for empty init.
 */
perr_t   transfer_request_metadata_query_init_elastic_bulki(int n_new, BULKI *checkpoint_bulki);
perr_t   transfer_request_metadata_query_finalize();
perr_t   transfer_request_metadata_query_checkpoint(char **checkpoint, uint64_t *checkpoint_size);
perr_t   transfer_request_metadata_query_checkpoint_bulki(BULKI **checkpoint_bulki);
perr_t   transfer_request_metadata_query_lookup_query_buf(uint64_t query_id, char **buf_ptr);
uint64_t transfer_request_metadata_query_parse(int32_t n_objs, char *buf, uint8_t partition_type,
                                               uint64_t *total_buf_size_ptr);

#endif
