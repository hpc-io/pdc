#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include "pdc_server_metadata_index.h"
#include "idioms_local_index.h"
#include "pdc_logger.h"
#include "dart_core.h"

void
delete_kv_from_index(char *kv, uint64_t obj_id)
{
    char * key      = NULL;
    char * value    = NULL;
    int8_t kv_dtype = PDC_STRING;

    dart_perform_one_server_in_t  input;
    dart_perform_one_server_out_t output;

    if (kv && contains(kv, "=")) {
        key   = substring(kv, 0, indexOf(kv, '='));
        value = substring(kv, indexOf(kv, '=') + 1, strlen(kv));
        if (is_number_query(value)) {
            if (contains(value, ".")) {
                kv_dtype = PDC_DOUBLE;
            }
            else {
                kv_dtype = PDC_INT64;
            }
            get_number_from_string(value, kv_dtype, &(input.attr_val));
            input.attr_vsize = get_size_by_class_n_type(input.attr_val, 1, PDC_CLS_ITEM, kv_dtype);
        }
        else {
            input.attr_val   = stripQuotes(value);
            input.attr_vsize = strlen(input.attr_val) + 1;
        }
    }
    else {
        LOG_ERROR("Invalid Key Value Pair!\n");
        return;
    }

    input.obj_ref_type = REF_PRIMARY_ID;
    input.hash_algo    = DART_HASH;
    // Test delete Index
    input.op_type         = OP_DELETE;
    input.obj_primary_ref = obj_id;
    input.attr_vtype      = kv_dtype;

#ifndef PDC_DART_SFX_TREE
    input.inserting_suffix = 0;
    input.attr_key         = strdup(key);
    assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);

    input.inserting_suffix = 1;
    input.attr_key         = reverse_str(key);
    assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);

#else

    for (int i = 0; i < strlen(key); i++) {
        if (i == 0) {
            input.inserting_suffix = 0;
        }
        else {
            input.inserting_suffix = 1;
        }
        input.attr_key = substring(key, i, strlen(key));
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
    }

#endif
}

void
insert_kv_to_index(char *kv, uint64_t obj_id)
{

    char * key      = NULL;
    void * value    = NULL;
    int8_t kv_dtype = PDC_STRING;

    dart_perform_one_server_in_t  input;
    dart_perform_one_server_out_t output;

    LOG_DEBUG("Inserting %s\n", kv);
    if (kv && contains(kv, "=")) {
        key   = substring(kv, 0, indexOf(kv, '='));
        value = substring(kv, indexOf(kv, '=') + 1, strlen(kv));
        if (is_number_query(value)) {
            if (contains(value, ".")) {
                kv_dtype = PDC_DOUBLE;
            }
            else {
                kv_dtype = PDC_INT64;
            }
            get_number_from_string(value, kv_dtype, &(input.attr_val));
            input.attr_vsize = get_size_by_class_n_type(input.attr_val, 1, PDC_CLS_ITEM, kv_dtype);
        }
        else {
            input.attr_val   = stripQuotes(value);
            input.attr_vsize = strlen(input.attr_val) + 1;
        }
    }
    else {
        LOG_ERROR("Invalid Key Value Pair!\n");
        return;
    }

    input.obj_ref_type = REF_PRIMARY_ID;
    input.hash_algo    = DART_HASH;
    // Test Insert Index
    input.op_type = OP_INSERT;

    input.obj_primary_ref = obj_id;
    input.attr_vtype      = kv_dtype;

#ifndef PDC_DART_SFX_TREE
    input.inserting_suffix = 0;
    input.attr_key         = strdup(key);
    assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);

    input.inserting_suffix = 1;
    input.attr_key         = reverse_str(key);
    assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);

#else

    for (int i = 0; i < strlen(key); i++) {
        if (i == 0) {
            input.inserting_suffix = 0;
        }
        else {
            input.inserting_suffix = 1;
        }
        input.attr_key = substring(key, i, strlen(key));
        input.vnode_id = 1;
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
    }

#endif
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
    NLF_LOG_INFO("Query %s Successful!  %d Results: ", key_value_query, n_obj_ids);
    for (int i = 0; i < n_obj_ids; i++) {
        printf("%llu, ", buf_ptr[i]);
    }
    printf("\n\n");
}

void
test_PDC_Server_dart_perform_one_server()
{

    PDC_Server_metadata_index_init(1, 0);

    char *kv    = (char *)calloc(20, sizeof(char));
    char *numkv = (char *)calloc(20, sizeof(char));

    for (int i = 0; i < 1000; i++) {
        sprintf(kv, "key%03dkey=\"val%03dval\"", i, i);
        sprintf(numkv, "num%03dnum=%d", i, i);
        insert_kv_to_index(kv, 10000 + i);
        sprintf(numkv, "num%03dnum=%d", i, i);
        insert_kv_to_index(numkv, 10000 + i);
    }

    insert_kv_to_index("0key=\"0val\"", 20000);
    insert_kv_to_index("key000key=\"val000val\"", 10000);
    insert_kv_to_index("key000key=\"val000val\"", 20000);
    insert_kv_to_index("key000key=\"val000val\"", 30000);
    insert_kv_to_index("key433key=\"val433val\"", 30000);

    insert_kv_to_index("0num=0", 20000);
    insert_kv_to_index("num000num=0", 10000);
    insert_kv_to_index("num010num=2", 20000);
    insert_kv_to_index("num010num=3", 30000);
    insert_kv_to_index("num010num=5", 50000);
    insert_kv_to_index("num010num=6", 60000);
    insert_kv_to_index("num010num=7", 70000);
    insert_kv_to_index("num010num=9", 90000);

    insert_kv_to_index("num001num=0", 11000);
    insert_kv_to_index("num011num=2", 21000);
    insert_kv_to_index("num011num=3", 31000);
    insert_kv_to_index("num011num=5", 51000);
    insert_kv_to_index("num011num=6", 61000);
    insert_kv_to_index("num011num=7", 71000);
    insert_kv_to_index("num011num=9", 91000);

    insert_kv_to_index("num000num=0", 30000);
    insert_kv_to_index("num433num=433", 30000);

    LOG_INFO("Index Insertion Successful!\n");

    // key000key val000val
    query_result_from_kvtag("key000key=\"val000val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("0key=\"0val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("key01*=\"val01*\"", OP_PREFIX_QUERY);
    query_result_from_kvtag("*33key=\"*33val\"", OP_SUFFIX_QUERY);
    query_result_from_kvtag("*43*=\"*43*\"", OP_INFIX_QUERY);

    query_result_from_kvtag("num01*=5~", OP_RANGE_QUERY);
    query_result_from_kvtag("num000num=0", OP_EXACT_QUERY);
    query_result_from_kvtag("num01*=~5", OP_RANGE_QUERY);
    query_result_from_kvtag("0num=0", OP_EXACT_QUERY);
    query_result_from_kvtag("num01*=5~9", OP_RANGE_QUERY);
    query_result_from_kvtag("num01*=5|~|9", OP_RANGE_QUERY);

    LOG_INFO("Index Dumping...\n");
    // save the index to file
    metadata_index_dump("/workspaces/pdc/build/bin", 0);

    for (int i = 0; i < 1000; i++) {
        sprintf(kv, "key%03dkey=\"val%03dval\"", i, i);
        // LOG_DEBUG("Deleting %s\n", kv);
        delete_kv_from_index(kv, 10000 + i);
        sprintf(numkv, "num%03dnum=%d", i, i);
        delete_kv_from_index(numkv, 10000 + i);
    }

    delete_kv_from_index("0key=\"0val\"", 20000);
    delete_kv_from_index("key000key=\"val000val\"", 10000);
    delete_kv_from_index("key000key=\"val000val\"", 20000);
    delete_kv_from_index("key000key=\"val000val\"", 30000);
    delete_kv_from_index("key433key=\"val433val\"", 30000);

    delete_kv_from_index("0num=0", 20000);
    delete_kv_from_index("num000num=0", 10000);
    delete_kv_from_index("num010num=2", 20000);
    delete_kv_from_index("num010num=3", 30000);
    delete_kv_from_index("num010num=5", 50000);
    delete_kv_from_index("num010num=6", 60000);
    delete_kv_from_index("num010num=7", 70000);
    delete_kv_from_index("num010num=9", 90000);

    delete_kv_from_index("num001num=0", 11000);
    delete_kv_from_index("num011num=2", 21000);
    delete_kv_from_index("num011num=3", 31000);
    delete_kv_from_index("num011num=5", 51000);
    delete_kv_from_index("num011num=6", 61000);
    delete_kv_from_index("num011num=7", 71000);
    delete_kv_from_index("num011num=9", 91000);

    delete_kv_from_index("num000num=0", 30000);
    delete_kv_from_index("num433num=433", 30000);

    LOG_INFO("Index Deletion Successful!\n");

    query_result_from_kvtag("key000key=\"val000val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("0key=\"0val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("key01*=\"val01*\"", OP_PREFIX_QUERY);
    query_result_from_kvtag("*33key=\"*l\"", OP_SUFFIX_QUERY);
    query_result_from_kvtag("*43*=\"*43*\"", OP_INFIX_QUERY);

    query_result_from_kvtag("num01*=5~", OP_RANGE_QUERY);
    query_result_from_kvtag("num000num=0", OP_EXACT_QUERY);
    query_result_from_kvtag("num01*=~5", OP_RANGE_QUERY);
    query_result_from_kvtag("0num=0", OP_EXACT_QUERY);
    query_result_from_kvtag("num01*=5~9", OP_RANGE_QUERY);
    query_result_from_kvtag("num01*=5|~|9", OP_RANGE_QUERY);

    metadata_index_recover("/workspaces/pdc/build/bin", 1, 0);

    LOG_INFO("Index Recovery Done!\n");

    // key000key val000val
    query_result_from_kvtag("key000key=\"val000val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("0key=\"0val\"", OP_EXACT_QUERY);
    query_result_from_kvtag("key01*=\"val01*\"", OP_PREFIX_QUERY);
    query_result_from_kvtag("*33key=\"*33val\"", OP_SUFFIX_QUERY);
    query_result_from_kvtag("*43*=\"*43*\"", OP_INFIX_QUERY);

    query_result_from_kvtag("num01*=5~", OP_RANGE_QUERY);
    query_result_from_kvtag("num000num=0", OP_EXACT_QUERY);
    query_result_from_kvtag("num01*=~5", OP_RANGE_QUERY);
    query_result_from_kvtag("0num=0", OP_EXACT_QUERY);
    query_result_from_kvtag("num01*=5~9", OP_RANGE_QUERY);
    query_result_from_kvtag("num01*=5|~|9", OP_RANGE_QUERY);
}
int
init_default_logger()
{
    setLogFile(LOG_LEVEL_ERROR, "stderr");
    setLogFile(LOG_LEVEL_WARNING, "stdout");
    setLogFile(LOG_LEVEL_INFO, "stdout");
    setLogFile(LOG_LEVEL_DEBUG, "stdout");
    setLogLevel(LOG_LEVEL_DEBUG);
    return 0;
}

int
main()
{
    init_default_logger();
    test_PDC_Server_dart_perform_one_server();
    return 0;
}