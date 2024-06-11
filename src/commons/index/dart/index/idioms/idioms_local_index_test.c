#include "idioms_local_index.h"
#include "dart_core.h"
#include "bulki.h"

typedef enum { IDIOMS_INSERT = 1, IDIOMS_DELETE = 2, IDIOMS_QUERY = 3 } IDIOMS_OP_TYPE;

typedef struct {
    IDIOMS_t *idioms;
    void *    buffer;
    size_t    buffer_size;
} dummy_server_t;

typedef struct {
    DART * dart;
    int    DART_ALPHABET_SIZE;
    int    num_servers;
    int    extra_tree_height;
    int    replication_factor;
    int    dart_insert_count;
    void * buffer;
    size_t buffer_size;
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
        servers[i].idioms = IDIOMS_init(i, num_servers);
    }
}

void
init_clients(int num_clients)
{
    // create an array of dummy_client_t
    clients = (dummy_client_t *)malloc(num_clients * sizeof(dummy_client_t));
    // initialize each client, simulating the initialization of every single process
    for (int i = 0; i < num_clients; i++) {
        clients[i].dart               = (DART *)calloc(1, sizeof(DART));
        clients[i].num_servers        = 3;
        clients[i].DART_ALPHABET_SIZE = 27;
        clients[i].extra_tree_height  = 0;
        clients[i].replication_factor = 3; // pdc_server_num_g / 10;
        clients[i].replication_factor = clients[i].replication_factor > 0 ? clients[i].replication_factor : 2;
    }

    // simulate the initialization of the DART space
    for (int i = 0; i < num_clients; i++) {
        dart_space_init(clients[i].dart, clients[i].num_servers, clients[i].DART_ALPHABET_SIZE,
                        clients[i].extra_tree_height, clients[i].replication_factor, clients[i].num_servers);
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
    client->buffer_size = get_BULKI_Entity_size(bentArr);
    client->buffer      = BULKI_Entity_serialize(bentArr);
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
    server->buffer_size = client->buffer_size;
    server->buffer      = (void *)malloc(server->buffer_size);
    memcpy(server->buffer, client->buffer, server->buffer_size);
    free(client->buffer);
}

void
get_response_from_server(dummy_client_t *client, dummy_server_t *server)
{
    // memcpy to simulate the receiving of the response from the server
    client->buffer_size = server->buffer_size;
    client->buffer      = (void *)malloc(client->buffer_size);
    memcpy(client->buffer, server->buffer, client->buffer_size);
    free(server->buffer);
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
    BULKI_Entity * resultBent  = empty_Bent_Array_Entity();
    BULKI_Entity * bentArr     = BULKI_Entity_deserialize(server->buffer);
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
    server->buffer_size = get_BULKI_Entity_size(resultBent);
    server->buffer      = BULKI_Entity_serialize(resultBent);
}

perr_t
client_parse_response(dummy_client_t *client, uint64_t **obj_id_list, uint64_t *count)
{
    BULKI_Entity *resultBent = BULKI_Entity_deserialize(client->buffer);
    int           result     = *(int *)BULKI_ENTITY_get_BULKI_Entity(resultBent, 0)->data;
    if (result == SUCCEED && obj_id_list != NULL && count != NULL) {
        BULKI_Entity *obj_id_bent = BULKI_ENTITY_get_BULKI_Entity(resultBent, 1);
        if (obj_id_bent != NULL && obj_id_bent->count > 0) {
            *obj_id_list = (uint64_t *)obj_id_bent->data;
            *count       = obj_id_bent->count;
        }
    }
    free(client->buffer);
    return result;
}

void
client_insert_data(dummy_client_t *client, int id)
{
    char key[40];
    char value[40];
    sprintf(key, "%d_key_%d", id, id);
    sprintf(value, "%d_value_%d", id, id);
    uint64_t u64_id = (uint64_t)id;
    // generate a request for each client
    index_hash_result_t *hash_result       = NULL;
    int                  num_selected_srvs = client_select_server(client, key, IDIOMS_INSERT, &hash_result);
    for (int s = 0; s < num_selected_srvs; s++) {
        BULKI_Entity *value_ent = BULKI_ENTITY(value, 1, PDC_STRING, PDC_CLS_ITEM);
        client_generate_request(client, IDIOMS_INSERT, key, value_ent, &u64_id);
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        perr_t result     = client_parse_response(client, NULL, NULL);
        char * result_str = result == SUCCEED ? "SUCCEED" : "FAILED";
        printf("Insert result: %s\n", result_str);
    }
}

void
client_perform_search(dummy_client_t *client, int id)
{
    char query[40];
    // exact search
    sprintf(query, "%d_key_%d=%d_value_%d", id, id, id, id);
    // generate a request for each client
    index_hash_result_t *hash_result       = NULL;
    int                  num_selected_srvs = client_select_server(client, query, IDIOMS_QUERY, &hash_result);
    for (int s = 0; s < num_selected_srvs; s++) {
        client_generate_request(client, IDIOMS_QUERY, query, NULL, NULL);
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        uint64_t *obj_id_list = NULL;
        uint64_t  count       = 0;
        perr_t    result      = client_parse_response(client, &obj_id_list, &count);
        char *    result_str  = result == SUCCEED ? "SUCCEED" : "FAILED";
        printf("Search result: %s | ", result_str);
        for (int i = 0; i < count; i++) {
            printf("%lu ", ((uint64_t *)obj_id_list)[i]);
        }
        printf("|\n");
    }
}

void
client_delete_data(dummy_client_t *client, int id)
{
    char key[20];
    char value[20];
    sprintf(key, "%d_key_%d", id, id);
    sprintf(value, "%d_value_%d", id, id);
    uint64_t u64_id = (uint64_t)id;
    // generate a request for each client
    BULKI_Entity *value_ent = BULKI_ENTITY(value, 1, PDC_STRING, PDC_CLS_ITEM);
    client_generate_request(client, IDIOMS_DELETE, key, value_ent, &u64_id);
    index_hash_result_t *hash_result       = NULL;
    int                  num_selected_srvs = client_select_server(client, key, IDIOMS_DELETE, &hash_result);
    for (int s = 0; s < num_selected_srvs; s++) {
        sending_request_to_server(client, &servers[hash_result[s].server_id]);
        server_perform_operation(&servers[hash_result[s].server_id]);
        get_response_from_server(client, &servers[hash_result[s].server_id]);
        perr_t result     = client_parse_response(client, NULL, NULL);
        char * result_str = result == SUCCEED ? "SUCCEED" : "FAILED";
        printf("Delete result: %s\n", result_str);
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
    init_clients(num_clients);

    // insert data
    for (int i = 0; i < num_clients; i++) {
        for (int id = 10000; id < 20000; id++) {
            if (id % num_clients != i)
                continue;
            client_insert_data(&clients[i], id);
        }
    }

    // search data
    for (int i = 0; i < num_clients; i++) {
        for (int id = 10000; id < 20000; id++) {
            if (id % num_clients != i)
                continue;
            client_perform_search(&clients[i], id);
        }
    }

    // delete data
    for (int i = 0; i < num_clients; i++) {
        for (int id = 10000; id < 20000; id++) {
            if (id % num_clients != i)
                continue;
            client_delete_data(&clients[i], id);
        }
    }

    // search data
    for (int i = 0; i < num_clients; i++) {
        for (int id = 10000; id < 20000; id++) {
            if (id % num_clients != i)
                continue;
            client_perform_search(&clients[i], id);
        }
    }

    return 0;
}