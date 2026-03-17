#ifndef PDC_SERVER_METADATA_PHT_H
#define PDC_SERVER_METADATA_PHT_H

#include "pdc_client_server_common.h"

#include "query_utils.h"
#include "timer_utils.h"
#include "pdc_set.h"
#include "pdc_hash.h"
#include "pdc_compare.h"
#include "pdc_hash_table.h"
#include "pdc_pht.h"

#define PHT_BUCKET_SIZE 1024

void   PDC_Server_metadata_pht_init(uint32_t num_server, uint32_t server_id);
perr_t PDC_Server_add_metadata();
/**
 * Create a bucket with the given prefix
 *
 * \param in [IN]               Input structure received from client
 * \param out [IN]              Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_create_bucket(metadata_create_bucket_in_t *in, metadata_create_bucket_out_t *out);
perr_t PDC_Server_metadata_key_add(metadata_key_add_in_t *in, metadata_key_add_out_t *out);

#endif /* PDC_SERVER_METADATA_PHT_H */