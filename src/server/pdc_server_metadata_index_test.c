#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include "pdc_server_metadata_index.h"

void
insert_kv_to_index(char *key, char *value, uint64_t obj_id)
{
    dart_perform_one_server_in_t  input;
    dart_perform_one_server_out_t output;

    input.obj_ref_type = REF_PRIMARY_ID;
    input.hash_algo    = DART_HASH;
    // Test Insert Index
    input.op_type         = OP_INSERT;
    input.attr_val        = value;
    input.obj_primary_ref = obj_id;

    for (int i = 0; i < strlen(key); i++) {
        input.attr_key = substring(key, i, strlen(key));
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
        printf("Index Insertion Successful!\n");
    }
}

void
query_result_from_kvtag(char *key_value_query, int8_t op_type)
{
    dart_perform_one_server_in_t  input;
    dart_perform_one_server_out_t output;
    uint64_t                      n_obj_ids = 0;
    uint64_t *                    buf_ptr   = NULL;
    input.op_type                           = op_type;
    input.attr_key                          = key_value_query;
    assert(PDC_Server_dart_perform_one_server(&input, &output, &n_obj_ids, &buf_ptr) == SUCCEED);
    printf("Query Successful! Result: ");
    for (int i = 0; i < n_obj_ids; i++) {
        printf("%llu, ", buf_ptr[i]);
    }
    printf("\n");
}

void
test_PDC_Server_dart_perform_one_server()
{

    PDC_Server_dart_init();

    char *key = (char *)calloc(100, sizeof(char));
    char *val = (char *)calloc(100, sizeof(char));

    for (int i = 0; i < 100; i++) {
        sprintf(key, "key%03d", i);
        sprintf(val, "val%03d", i);
        printf("Inserting %s, %s\n", key, val);
        insert_kv_to_index(key, val, i);
    }

    insert_kv_to_index("key000key", "val000val", 10000);
    insert_kv_to_index("key000", "val000", 20000);
    insert_kv_to_index("000key", "000val", 30000);

    query_result_from_kvtag("key000key=val000val", OP_EXACT_QUERY);
    query_result_from_kvtag("key000*=val000*", OP_PREFIX_QUERY);
    query_result_from_kvtag("*000key=*000val", OP_SUFFIX_QUERY);
    query_result_from_kvtag("*000*=*000*", OP_INFIX_QUERY);
}

int
main()
{
    test_PDC_Server_dart_perform_one_server();
    return 0;
}