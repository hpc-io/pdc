#ifndef PDC_RPC_H
#define PDC_RPC_H

#include "pdc_config.h"

#define BULK_RPC_THRESHOLD 4096

// struct that contains the information of the RPC source
typedef struct {

} pdc_rpc_source_t;

// struct that contains the information of the RPC target
typedef struct {
    // uint32_t serverId;
} pdc_rpc_target_t;

// struct of the RPC message
typedef struct {

} pdc_rpc_msg_t;

pdc_rpc_source_t sender_init() {

}


#endif /* PDC_RPC_H */