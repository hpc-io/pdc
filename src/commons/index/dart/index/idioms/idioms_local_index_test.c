#include "idioms_local_index.h"
#include "dart_core.h"
#include "bulki.h"
#include "assert.h"

typedef enum { IDIOMS_INSERT = 1, IDIOMS_DELETE = 2, IDIOMS_QUERY = 3 } IDIOMS_OP_TYPE;

typedef struct {
    IDIOMS_t *idioms;
    void *    buffer_in;
    size_t    buffer_in_size;
    void *    buffer_out;
    size_t    buffer_out_size;
    int       id;
} dummy_server_t;

typedef struct {
    DART * dart;
    int    DART_ALPHABET_SIZE;
    int    num_servers;
    int    extra_tree_height;
    int    replication_factor;
    int    dart_insert_count;
    void * buffer_in;
    size_t buffer_in_size;
    void * buffer_out;
    size_t buffer_out_size;
    int    id;
} dummy_client_t;

dummy_server_t *servers;
dummy_client_t *clients;

void
init_servers(int num_servers)
{
    // create an array of dummy_server_t
    servers = (dummy_server_t *)malloc(num_servers * sizeof(dummy_server_t));
    // initialize each server, simulating the initialization of every single process
    for (int i = 0; i < num_servers; i++) {
        servers[i].id     = i;
        servers[i].idioms = IDIOMS_init(i, num_servers);
    }
}

void
init_clients(int num_clients, int num_servers)
{
    // create an array of dummy_client_t
    clients = (dummy_client_t *)malloc(num_clients * sizeof(dummy_client_t));
    // initialize each client, simulating the initialization of every single process
    for (int i = 0; i < num_clients; i++) {
        clients[i].id                 = i;
        clients[i].dart               = (DART *)calloc(1, sizeof(DART));
        clients[i].num_servers        = num_servers;
        clients[i].DART_ALPHABET_SIZE = 27;
        clients[i].extra_tree_height  = 0;
        clients[i].replication_factor = clients[i].replication_factor > 0 ? clients[i].replication_factor : 2;
        clients[i].replication_factor = 1; // pdc_server_num_g / 10;
    }

    // simulate the initialization of the DART space
    for (int i = 0; i < num_clients; i++) {
        dart_space_init(clients[i].dart, clients[i].num_servers);
    }
}

/**
 * message format:
 * {
 *   "message_type": "IDIOMS_query", // can be "IDIOMS_insert", "IDIOMS_delete", "IDIOMS_query"
 *   "key": "key1", // can be query string if IDIOMS_query, and can be key if IDIOMS_insert or IDIOMS_delete.
 *   "value": "value1", // optional field
 *   "id": "id1", // optional field
 * }
 */
void
client_generate_request(dummy_client_t *client, IDIOMS_OP_TYPE op_type, char *key, BULKI_Entity *value_ent,
                        uint64_t *id)
{

    BULKI_Entity *bentArr = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(bentArr,
                                     BULKI_ENTITY(&op_type, 1, PDC_INT8, PDC_CLS_ITEM));       // 0. op_type
    BULKI_ENTITY_append_BULKI_Entity(bentArr, BULKI_ENTITY(key, 1, PDC_STRING, PDC_CLS_ITEM)); // 1. key

    if (op_type == IDIOMS_INSERT || op_type == IDIOMS_DELETE) {
        BULKI_ENTITY_append_BULKI_Entity(bentArr, value_ent);                                     // 2. value
        BULKI_ENTITY_append_BULKI_Entity(bentArr, BULKI_ENTITY(id, 1, PDC_UINT64, PDC_CLS_ITEM)); // 3. id
    }
    client->buffer_out = BULKI_Entity_serialize(bentArr, &client->buffer_out_size);
}

void
get_server_info_cb(dart_server *server_ptr)
{
    uint32_t server_id                              = server_ptr->id;
    servers[server_id].idioms->index_record_count_g = 100;
}

int
client_select_server(dummy_client_t *client, char *attr_key, IDIOMS_OP_TYPE op_type,
                     index_hash_result_t **hash_result)
{
    // select a server based on the key
    dart_op_type_t           dart_op   = OP_INSERT;
    char *                   input_key = attr_key;
    get_server_info_callback gsi_cp    = NULL;
    if (op_type == IDIOMS_INSERT) {
        dart_op = OP_INSERT;
        gsi_cp  = get_server_info_cb;
    }
    else if (op_type == IDIOMS_QUERY) {
        dart_determine_query_token_by_key_query(attr_key, &input_key, &dart_op);
    }
    else if (op_type == IDIOMS_DELETE) {
        dart_op = OP_DELETE;
    }
    return DART_hash(client->dart, attr_key, dart_op, gsi_cp, hash_result);
}

void
sending_request_to_server(dummy_client_t *client, dummy_server_t *server)
{
    // memcpy to simulate the sending of the request to the server
    server->buffer_in_size = client->buffer_out_size;
    server->buffer_in      = (void *)malloc(server->buffer_in_size);
    memcpy(server->buffer_in, client->buffer_out, server->buffer_in_size);
    free(client->buffer_out);
}

void
get_response_from_server(dummy_client_t *client, dummy_server_t *server)
{
    // memcpy to simulate the receiving of the response from the server
    client->buffer_in_size = server->buffer_out_size;
    client->buffer_in      = (void *)malloc(client->buffer_in_size);
    memcpy(client->buffer_in, server->buffer_out, client->buffer_in_size);
    free(server->buffer_out);
}

perr_t
server_perform_query(dummy_server_t *server, char *query_str, uint64_t **object_id_list, uint64_t *count)
{
    IDIOMS_md_idx_record_t *idx_record = (IDIOMS_md_idx_record_t *)calloc(1, sizeof(IDIOMS_md_idx_record_t));
    idx_record->key                    = query_str;
    idioms_local_index_search(server->idioms, idx_record);
    *object_id_list = idx_record->obj_ids;
    *count          = idx_record->num_obj_ids;
    return SUCCEED;
}

perr_t
server_perform_insert(dummy_server_t *server, char *key, BULKI_Entity *value_ent, uint64_t id)
{
    // we assume that the count of value_ent is 1.
    IDIOMS_md_idx_record_t *idx_record = (IDIOMS_md_idx_record_t *)calloc(1, sizeof(IDIOMS_md_idx_record_t));
    idx_record->key                    = key;
    idx_record->value                  = value_ent->data;
    idx_record->value_len              = value_ent->size;
    idx_record->type                   = value_ent->pdc_type;
    idx_record->num_obj_ids            = 1;
    idx_record->obj_ids                = (uint64_t *)calloc(1, sizeof(uint64_t));
    idx_record->obj_ids[0]             = id;
    idioms_local_index_create(server->idioms, idx_record);
    return SUCCEED;
}

perr_t
server_perform_delete(dummy_server_t *server, char *key, BULKI_Entity *value_ent, uint64_t id)
{
    IDIOMS_md_idx_record_t *idx_record = (IDIOMS_md_idx_record_t *)calloc(1, sizeof(IDIOMS_md_idx_record_t));
    idx_record->key                    = key;
    idx_record->value                  = value_ent->data;
    idx_record->value_len              = value_ent->size;
    idx_record->type                   = value_ent->pdc_type;
    idx_record->num_obj_ids            = 1;
    idx_record->obj_ids                = (uint64_t *)calloc(1, sizeof(uint64_t));
    idx_record->obj_ids[0]             = id;
    idioms_local_index_delete(server->idioms, idx_record);
    return SUCCEED;
}

void
server_perform_operation(dummy_server_t *server)
{
    // printf("Perform operation on server %d\n", server->id);
    BULKI_Entity * resultBent  = empty_Bent_Array_Entity();
    BULKI_Entity * bentArr     = BULKI_Entity_deserialize(server->buffer_in);
    BULKI_Entity * opType_ent  = BULKI_ENTITY_get_BULKI_Entity(bentArr, 0);
    BULKI_Entity * key_ent     = BULKI_ENTITY_get_BULKI_Entity(bentArr, 1);
    char *         key         = (char *)key_ent->data;
    perr_t         result      = SUCCEED;
    IDIOMS_OP_TYPE opType      = *(int8_t *)opType_ent->data;
    uint64_t *     obj_id_list = NULL;
    uint64_t       count       = 0;
    if (opType == IDIOMS_QUERY) {
        result = server_perform_query(server, key, &obj_id_list, &count);
    }
    else {
        BULKI_Entity *value_ent = BULKI_ENTITY_get_BULKI_Entity(bentArr, 2);
        BULKI_Entity *id_ent    = BULKI_ENTITY_get_BULKI_Entity(bentArr, 3);
        uint64_t      id        = *(uint64_t *)id_ent->data;
        if (opType == IDIOMS_INSERT) {
            result = server_perform_insert(server, key, value_ent, id);
        }
        else if (opType == IDIOMS_DELETE) {
            result = server_perform_delete(server, key, value_ent, id);
        }
    }
    BULKI_ENTITY_append_BULKI_Entity(resultBent, BULKI_ENTITY(&result, 1, PDC_INT32, PDC_CLS_ITEM));
    if (count > 0) {
        BULKI_ENTITY_append_BULKI_Entity(resultBent,
                                         BULKI_ENTITY(obj_id_list, count, PDC_UINT64, PDC_CLS_ARRAY));
    }
    server->buffer_out = BULKI_Entity_serialize(resultBent, &server->buffer_out_size);
    free(server->buffer_in);
}

perr_t
client_parse_response(dummy_client_t *client, uint64_t **obj_id_list, uint64_t *count)
{
    BULKI_Entity *resultBent = BULKI_Entity_deserialize(client->buffer_in);
    int           result     = *(int *)BULKI_ENTITY_get_BULKI_Entity(resultBent, 0)->data;
    if (result == SUCCEED && obj_id_list != NULL && count != NULL) {
        BULKI_Entity *obj_id_bent = BULKI_ENTITY_get_BULKI_Entity(resultBent, 1);
        if (obj_id_bent != NULL && obj_id_bent->count > 0) {
            *obj_id_list = (uint64_t *)obj_id_bent->data;
            *count       = obj_id_bent->count;
        }
    }
    free(client->buffer_in);
    return result;
}

perr_t
client_insert_data(dummy_client_t *client, int id)
{
    char    key[64];
    char    value[64];
    char    int_key[64];
    int64_t int_value = (int64_t)id;
    sprintf(key, "str%03dstr", id, id);
    sprintf(int_key, "int%s", "key");
    sprintf(value, "str%03dstr", id, id, id);
    uint64_t u64_id = (uint64_t)id;
    perr_t   result = SUCCEED;
    // generate a request for each client
    index_hash_result_t *hash_result       = NULL;
    int                  num_selected_srvs = client_select_server(client, key, IDIOMS_INSERT, &hash_result);
    for (int s = 0; s < num_selected_srvs; s++) {
        // client insert string value
        BULKI_Entity *value_ent = BULKI_ENTITY(value, 1, PDC_STRING, PDC_CLS_ITEM);
        client_generate_request(client, IDIOMS_INSERT, key, value_ent, &u64_id);
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        result |= client_parse_response(client, NULL, NULL);

        // client insert numeric value
        BULKI_Entity *value_ent2 = BULKI_ENTITY(&int_value, 1, PDC_INT64, PDC_CLS_ITEM);
        client_generate_request(client, IDIOMS_INSERT, int_key, value_ent2, &u64_id);
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        result |= client_parse_response(client, NULL, NULL);
    }
    char *result_str = result == SUCCEED ? "SUCCEED" : "FAILED";
    printf("Insert result: %s\n", result_str);
    return result;
}

void
client_print_result(uint64_t *rst_ids, uint64_t rst_count)
{
    printf("Result count : %" PRIu64 " | ", rst_count);
    for (int i = 0; i < rst_count; i++) {
        printf("%lu ", rst_ids[i]);
    }
    printf("|\n");
}

uint64_t
client_perform_search(dummy_client_t *client, char *query, uint64_t **rst_ids)
{
    if (rst_ids == NULL) {
        return 0;
    }
    // generate a request for each client
    index_hash_result_t *hash_result       = NULL;
    int                  num_selected_srvs = client_select_server(client, query, IDIOMS_QUERY, &hash_result);
    *rst_ids                               = NULL;
    uint64_t rst_count                     = 0;
    for (int s = 0; s < num_selected_srvs; s++) {
        client_generate_request(client, IDIOMS_QUERY, query, NULL, NULL);
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        uint64_t *obj_id_list = NULL;
        uint64_t  count       = 0;
        perr_t    result      = client_parse_response(client, &obj_id_list, &count);
        if (result == SUCCEED && count > 0) {
            rst_count += count;
            if (*rst_ids == NULL) {
                *rst_ids = (uint64_t *)malloc(rst_count * sizeof(uint64_t));
            }
            else {
                *rst_ids = (uint64_t *)realloc(*rst_ids, rst_count * sizeof(uint64_t));
            }
            memcpy(*rst_ids + rst_count - count, obj_id_list, count * sizeof(uint64_t));
        }
    }
    client_print_result(*rst_ids, rst_count);
    return rst_count;
}

perr_t
client_delete_data(dummy_client_t *client, int id)
{
    char    key[64];
    char    value[64];
    char    int_key[64];
    int64_t int_value = (int64_t)id;
    sprintf(key, "str%03dstr", id, id);
    sprintf(int_key, "int%s", key);
    sprintf(value, "str%03dstr", id, id, id);
    uint64_t u64_id = (uint64_t)id;
    // generate a request for each client
    index_hash_result_t *hash_result       = NULL;
    int                  num_selected_srvs = client_select_server(client, key, IDIOMS_DELETE, &hash_result);
    perr_t               result            = SUCCEED;
    for (int s = 0; s < num_selected_srvs; s++) {
        // delete string value
        BULKI_Entity *value_ent = BULKI_ENTITY(value, 1, PDC_STRING, PDC_CLS_ITEM);
        client_generate_request(client, IDIOMS_DELETE, key, value_ent, &u64_id);
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        result |= client_parse_response(client, NULL, NULL);

        // delete numeric value
        BULKI_Entity *value_ent2 = BULKI_ENTITY(&int_value, 1, PDC_INT64, PDC_CLS_ITEM);
        client_generate_request(client, IDIOMS_DELETE, int_key, value_ent2, &u64_id);
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        result |= client_parse_response(client, NULL, NULL);
    }
    char *result_str = result == SUCCEED ? "SUCCEED" : "FAILED";
    printf("Delete result: %s\n", result_str);
    return result;
}

void
basic_test()
{
    perr_t insert_rst = client_insert_data(&clients[0], 10);
    assert(insert_rst == SUCCEED);

    char query[100];
    // exact query
    sprintf(query, "%d_%d=\"%d_%d_%d\"", 10, 10, 10, 10, 10);
    uint64_t *rst_ids   = NULL;
    uint64_t  rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 1);
    assert(rst_ids[0] == 10);

    // prefix search
    sprintf(query, "%d_%d=\"%d_*\"", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 1);
    assert(rst_ids[0] == 10);

    // suffix search
    sprintf(query, "%d_%d=\"*_%d\"", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 1);
    assert(rst_ids[0] == 10);

    // infix search
    sprintf(query, "%d_%d=\"*_%d_*\"", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 1);
    assert(rst_ids[0] == 10);

    // numeric exact query
    sprintf(query, "%d_%d=|%d|", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count >= 1);
    assert(rst_ids[0] == 10);

    // numeric range query
    sprintf(query, "%d_%d=%d|~|%d", 10, 10, 9, 11);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 1);
    assert(rst_ids[0] == 10);

    perr_t delete_rst = client_delete_data(&clients[0], 10);
    assert(delete_rst == SUCCEED);

    sprintf(query, "%d_%d=\"%d_%d\"", 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 0);
    assert(rst_ids == NULL);

    // prefix search
    sprintf(query, "%d_%d=\"%d_*\"", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 0);
    assert(rst_ids == NULL);

    // suffix search
    sprintf(query, "%d_%d=\"*_%d\"", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 0);
    assert(rst_ids == NULL);

    // infix search
    sprintf(query, "%d_%d=\"*_%d_*\"", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 0);
    assert(rst_ids == NULL);

    // numeric exact query
    sprintf(query, "%d_%d=|%d|", 10, 10, 10);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 0);
    assert(rst_ids == NULL);

    // numeric range query
    sprintf(query, "%d_%d=%d|~|%d", 10, 10, 9, 11);
    rst_ids   = NULL;
    rst_count = client_perform_search(&clients[0], query, &rst_ids);
    assert(rst_count == 0);
    assert(rst_ids == NULL);
}

int
compare_uint64(const void *a, const void *b)
{
    return (*(uint64_t *)a - *(uint64_t *)b);
}

int
validate_query_result(int world_rank, int nres, uint64_t *pdc_ids)
{
    int i;
    int query_series = world_rank % 6;
    int step_failed  = -1;
    switch (query_series) {
        case 0:
            if (nres != 1) {
                printf("fail to query kvtag [%s] with rank %d. Expect 1 result but got %d result\n",
                       "str109str=str109str", world_rank, nres);
                step_failed = 0;
            }
            if (pdc_ids[0] != 109) {
                printf("fail to query kvtag [%s] with rank %d. Expect 1 result which is 109, but got result "
                       "%" PRIu64 ".\n",
                       "str109str=str109str", world_rank, pdc_ids[0]);
                step_failed = 0;
            }
            break;
        case 1:
            if (nres != 10) {
                printf("fail to query kvtag [%s] with rank %d. Expect 10 Result, but got %d result.\n",
                       "str09*=str09*", world_rank, nres);
                step_failed = 1;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != i + 90) {
                    printf("fail to query kvtag [%s] with rank %d. The %d th result does not match. Expect "
                           "%d, but got %" PRIu64 "\n",
                           "str09*=str09*", world_rank, i, i + 90, pdc_ids[i]);
                    step_failed = 1;
                    break;
                }
            }
            break;
        case 2:
            if (nres != 10) {
                printf("fail to query kvtag [%s] with rank %d. Expect 10 result, but got %d result.\n",
                       "*09str=*09str", world_rank, nres);
                step_failed = 2;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != i * 10 + 9) {
                    printf("fail to query kvtag [%s] with rank %d. The $d th result does not match. Expect "
                           "%d, but got %" PRIu64 "\n",
                           "*09str=*09str", world_rank, i, i * 10 + 9, pdc_ids[i]);
                    step_failed = 2;
                    break;
                }
            }
            break;
        case 3:
            if (nres != 20) {
                printf("fail to query kvtag [%s] with rank %d. Expected 20 results, but got %d results\n",
                       "*09*=*09*", world_rank, nres);
                step_failed = 3;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            uint64_t expected[20] = {9,  90,  91,  92,  93,  94,  95,  96,  97,  98,
                                     99, 109, 209, 309, 409, 509, 609, 709, 809, 909};
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != expected[i]) {
                    printf("fail to query kvtag [%s] with rank %d. The %d th result does not match. Expect "
                           "%" PRIu64 ", but got %" PRIu64 " results.\n",
                           "*09*=*09*", world_rank, i, expected[i], pdc_ids[i]);
                    step_failed = 3;
                    break;
                }
            }
            break;
        case 4:
            if (nres != 1) {
                printf("fail to query kvtag [%s] with rank %d. Expected 1 result, but got %d results\n",
                       "intkey=109", world_rank, nres);
                step_failed = 4;
            }
            if (pdc_ids[0] != 109) {
                printf(
                    "fail to query kvtag [%s] with rank %d. Expected 1 result which is 109, but got %" PRIu64
                    "\n",
                    "intkey=109", world_rank, pdc_ids[0]);
                step_failed = 4;
            }
            break;
        case 5:
            if (nres != 10) {
                printf("fail to query kvtag [%s] with rank %d. Expected 10 results, but got %d results. \n",
                       "intkey=90|~|99", world_rank, nres);
                step_failed = 5;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != i + 90) {
                    printf("fail to query kvtag [%s] with rank %d. The %d th result does not match, expect "
                           "%d but got %" PRIu64 "\n",
                           "intkey=90|~|99", world_rank, i, i + 90, pdc_ids[i]);
                    step_failed = 5;
                    break;
                }
            }
            break;
        default:
            break;
    }
    return step_failed;
}

void
perform_loop_test(int num_clients, int num_servers)
{
    for (int i = 0; i < 1000; i++) {
        client_insert_data(&clients[i % num_clients], i);
    }

    for (int i = 0; i < 1000; i++) {
        int   client_rank  = i % num_clients;
        int   query_series = i % 6;
        char *query        = NULL;
        switch (query_series) {
            case 0:
                query = "str109str=\"str109str\"";
                break;
            case 1:
                query = "str09*=\"str09*\"";
                break;
            case 2:
                query = "*09str=\"*09str\"";
                break;
            case 3:
                query = "*09*=\"*09*\"";
                break;
            case 4:
                query = "intkey=109";
                break;
            case 5:
                query = "intkey=90|~|99";
                break;
            default:
                break;
        }

        uint64_t *rst_ids   = NULL;
        uint64_t  rst_count = client_perform_search(&clients[client_rank], query, &rst_ids);
        if (validate_query_result(client_rank, rst_count, rst_ids) == -1) {
            printf("query [%s] with rank %d succeed\n", query, client_rank);
        }
        else {
            printf("query [%s] with rank %d failed\n", query, client_rank);
        }
    }
}

int
main(int argc, char *argv[])
{
    // read number of servers from first console argument
    int num_servers = atoi(argv[1]);
    // read number of clients from second console argument
    int num_clients = atoi(argv[2]);

    // create an array of dummy_server_t
    init_servers(num_servers);

    // create an array of dummy_client_t
    init_clients(num_clients, num_servers);

    basic_test();
    perform_loop_test(num_clients, num_servers);

    // // insert data
    // for (int i = 0; i < num_clients; i++) {
    //     for (int id = 0; id < 1000; id++) {
    //         if (id % num_clients != i)
    //             continue;
    //         client_insert_data(&clients[i], id);
    //     }
    // }

    // // // search data
    // for (int i = 0; i < num_clients; i++) {
    //     for (int id = 0; id < 1000; id++) {
    //         if (id % num_clients != i)
    //             continue;
    //         client_perform_search(&clients[i], id);
    //     }
    // }

    // // // delete data
    // for (int i = 0; i < num_clients; i++) {
    //     for (int id = 0; id < 1000; id++) {
    //         if (id % num_clients != i)
    //             continue;
    //         client_delete_data(&clients[i], id);
    //     }
    // }

    // // // search data
    // for (int i = 0; i < num_clients; i++) {
    //     for (int id = 0; id < 1000; id++) {
    //         if (id % num_clients != i)
    //             continue;
    //         client_perform_search(&clients[i], id);
    //     }
    // }

    return 0;
}