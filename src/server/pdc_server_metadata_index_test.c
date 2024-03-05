#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include "pdc_server_metadata_index.h"

typedef struct {

} dummy_server_t;

void
delete_kv_from_index(char *kv, uint64_t obj_id)
{
    char *key   = NULL;
    char *value = NULL;
    if (kv && contains(kv, "=")) {
        key   = substring(kv, 0, indexOf(kv, '='));
        value = substring(kv, indexOf(kv, '=') + 1, strlen(kv));
    }
    else {
        printf("Invalid Key Value Pair!\n");
        return;
    }

    dart_perform_one_server_in_t  input;
    dart_perform_one_server_out_t output;

    input.obj_ref_type = REF_PRIMARY_ID;
    input.hash_algo    = DART_HASH;
    // Test delete Index
    input.op_type         = OP_DELETE;
    input.attr_val        = value;
    input.obj_primary_ref = obj_id;
    input.attr_dtype      = PDC_STRING;

    for (int i = 0; i < strlen(key); i++) {
        if (i == 0) {
            input.inserting_suffix = 0;
        }
        else {
            input.inserting_suffix = 1;
        }
        input.attr_key = substring(key, 0, strlen(key));
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
    }
}

void
insert_kv_to_index(char *kv, uint64_t obj_id)
{

    char *key   = NULL;
    char *value = NULL;
    if (kv && contains(kv, "=")) {
        key   = substring(kv, 0, indexOf(kv, '='));
        value = substring(kv, indexOf(kv, '=') + 1, strlen(kv));
    }
    else {
        printf("Invalid Key Value Pair!\n");
        return;
    }

    dart_perform_one_server_in_t  input;
    dart_perform_one_server_out_t output;

    input.obj_ref_type = REF_PRIMARY_ID;
    input.hash_algo    = DART_HASH;
    // Test Insert Index
    input.op_type         = OP_INSERT;
    input.attr_val        = value;
    input.obj_primary_ref = obj_id;
    input.attr_dtype      = PDC_STRING;

    for (int i = 0; i < strlen(key); i++) {
        if (i == 0) {
            input.inserting_suffix = 0;
        }
        else {
            input.inserting_suffix = 1;
        }
        input.attr_key = substring(key, 0, strlen(key));
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
    }
}

void
query_result_from_kvtag(char *key_value_query, int8_t op_type)
{
    dart_perform_one_server_in_t *input =
        (dart_perform_one_server_in_t *)calloc(1, sizeof(dart_perform_one_server_in_t));
    dart_perform_one_server_out_t *output =
        (dart_perform_one_server_out_t *)calloc(1, sizeof(dart_perform_one_server_out_t));
    uint64_t  n_obj_ids = 0;
    uint64_t *buf_ptr   = NULL;
    input->op_type      = op_type;
    input->attr_key     = key_value_query;
    assert(PDC_Server_dart_perform_one_server(input, output, &n_obj_ids, &buf_ptr) == SUCCEED);
    printf("Query Successful! %d Results: ", n_obj_ids);
    for (int i = 0; i < n_obj_ids; i++) {
        printf("%llu, ", buf_ptr[i]);
    }
    printf("\n\n");
}

void
test_PDC_Server_dart_perform_one_server()
{

    PDC_Server_dart_init(1, 0);

    char *kv = (char *)calloc(20, sizeof(char));

    for (int i = 0; i < 100; i++) {
        sprintf(kv, "key%03dkey=\"val%03dval\"", i, i);
        insert_kv_to_index(kv, 10000 + i);
    }

    insert_kv_to_index("0key=\"0val\"", 20000);
    insert_kv_to_index("key000key=\"val000val\"", 10000);
    insert_kv_to_index("key000key=\"val000val\"", 20000);
    insert_kv_to_index("key000key=\"val000val\"", 30000);

    printf("Index Insertion Successful!\n");

    // key000key val000val
    query_result_from_kvtag("key000key=\"val000val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("0key=\"0val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("key01*=\"val01*\"", OP_PREFIX_QUERY);
    query_result_from_kvtag("*3key=\"*3val\"", OP_SUFFIX_QUERY);
    query_result_from_kvtag("*9*=\"*9*\"", OP_INFIX_QUERY);

    for (int i = 0; i < 100; i++) {
        sprintf(kv, "key%03dkey=\"val%03dval\"", i, i);
        // printf("Deleting %s\n", kv);
        delete_kv_from_index(kv, 10000 + i);
    }

    delete_kv_from_index("0key=\"0val\"", 20000);
    delete_kv_from_index("key000key=\"val000val\"", 10000);
    delete_kv_from_index("key000key=\"val000val\"", 20000);
    delete_kv_from_index("key000key=\"val000val\"", 30000);

    printf("Index Deletion Successful!\n");

    query_result_from_kvtag("key000key=\"val000val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("0key=\"0val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("key01*=\"val01*\"", OP_PREFIX_QUERY);
    query_result_from_kvtag("*3key=\"*3val\"", OP_SUFFIX_QUERY);
    query_result_from_kvtag("*9*=\"*9*\"", OP_INFIX_QUERY);
}

int
main()
{
    test_PDC_Server_dart_perform_one_server();
    return 0;
}