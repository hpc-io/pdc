#ifndef PDC_RPC_H
#define PDC_RPC_H

#include "pdc_config.h"
#include "pdc_public.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#ifndef RPC_TYPE_MERCURY
#define RPC_TYPE_MERCURY 1
#endif

typedef enum {
    SUCCESS               = 200,
    BAD_REQUEST           = 400,
    UNAUTHORIZED          = 401,
    FORBIDDEN             = 403,
    NOT_FOUND             = 404,
    INTERNAL_SERVER_ERROR = 500
} RpcStatus;

typedef struct {
    uint32_t num_targets;
    char *   target_addresses[];
} RpcTargetInfo;
// a struct that contains the request information
typedef struct {
    uint32_t targetId;
    uint64_t requestId;
    char *   method;
    void *   payload;
    uint64_t timestamp;
} RpcRequest;

typedef struct {
    uint64_t requestId;
    int      status;
    void *   payload;
    uint64_t timestamp;
    // payload processing function. When this is NULL, payload is returned as is. `output` can be set to NULL
    // if not needed.
    perr_t (*processPayload)(void *payload, void *output);
} RpcResponse;

typedef struct {
    // TODO: `data` may not be needed. Avoid using it if possible.
    perr_t (*initialize)(void *info);
    perr_t (*prepareConnection)(void *conn_info, void **connection);
    perr_t (*createRpc)(void *connection, void **rpcHandle, RpcRequest *request);
    perr_t (*performRpc)(void *rpcHandle, RpcResponse *response);
    perr_t (*getResponse)(void *rpcHandle, RpcResponse *response);
    perr_t (*finalize)(void *info);
} RpcOriginImplementation;

typedef struct {
    perr_t (*registerRpc)(void *info);
    perr_t (*routeRpcCall)(const char *method, void *rpcInput, void **rpcOutput);
    perr_t (*finalize)(void *info);
} RpcTargetImplementation;

typedef struct {
    RpcTargetInfo           targetInfo;
    RpcOriginImplementation originImpl;
    RpcTargetImplementation targetImpl;
    void (*callMethod)(RpcRequest *request, RpcResponse *response);
} RpcInterface;

typedef struct {
    RpcTargetInfo *targetInfoArray;
    uint32_t       numTargets;
    void *         rpcClass;
    void *         rpcContext;
    void *         rpcHandle;
    RpcInterface * rpcInterface;
} RpcEngine;

extern RpcEngine *rpcEngine;
/**
 * @brief This function initializes the RPC engine with a series of actions including:
 * 1. Initialize the RPC engine
 * 2. Exchange server information, if needed.
 * 3. Prepare rpcClass and rpcContext, if necessary.
 * 4. create rpcHandle.
 * @param type [IN] The type of RPC engine to be initialized.
 * @return void
 */
void rpcEngineInit();
/**
 * @brief This function finalizes the RPC engine.
 * @param rpcEngine [IN] The RPC engine to be finalized.
 * @return void
 */
void rpcEngineFinalize(RpcEngine *rpcEngine);

/**
 * @brief This function returns the RPC engine.
 */
RpcEngine *getRpcEngine();

/**
 * @brief This function returns the RPC interface.
 */
RpcInterface *getRpcInterface();
#endif /* PDC_RPC_H */