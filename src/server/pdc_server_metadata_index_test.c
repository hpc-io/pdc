#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include "pdc_server_metadata_index.h"

void
test_PDC_Server_dart_perform_one_server()
{

    PDC_Server_dart_init();
    dart_perform_one_server_in_t  input;
    dart_perform_one_server_out_t output;
    uint64_t                      n_obj_ids = 0;
    uint64_t *                    buf_ptr   = NULL;

    input.obj_ref_type = REF_PRIMARY_ID;
    input.hash_algo    = DART_HASH;

    // Test Insert Index
    input.op_type         = OP_INSERT;
    input.attr_val        = "val000val";
    input.obj_primary_ref = 10000;

    for (int i = 0; i < strlen("key000key"); i++) {
        input.attr_key = substring("key000key", i, strlen("key000key"));
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
        printf("Index Insertion Successful!\n");
    }

    // Test Insert Index
    input.op_type         = OP_INSERT;
    input.attr_val        = "val000";
    input.obj_primary_ref = 20000;
    for (int i = 0; i < strlen("key000"); i++) {
        input.attr_key = substring("key000", i, strlen("key000"));
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
        printf("Index Insertion Successful!\n");
    }

    // Test Insert Index
    input.op_type         = OP_INSERT;
    input.attr_val        = "000val";
    input.obj_primary_ref = 30000;
    for (int i = 0; i < strlen("000key"); i++) {
        input.attr_key = substring("000key", i, strlen("000key"));
        assert(PDC_Server_dart_perform_one_server(&input, &output, NULL, NULL) == SUCCEED);
        printf("Index Insertion Successful!\n");
    }

    // Test Exact query
    input.op_type  = OP_EXACT_QUERY;
    input.attr_key = "key000key=val000val";
    // input.attr_val = "val000val";
    assert(PDC_Server_dart_perform_one_server(&input, &output, &n_obj_ids, &buf_ptr) == SUCCEED);
    assert(n_obj_ids == 1);
    assert(buf_ptr[0] == 10000);
    printf("Exact Query Successful!\n");

    // Test Prefix query
    input.op_type  = OP_PREFIX_QUERY;
    input.attr_key = "key000*=val000*";
    // input.attr_val = "val000*";
    assert(PDC_Server_dart_perform_one_server(&input, &output, &n_obj_ids, &buf_ptr) == SUCCEED);
    printf("Prefix Query Successful! ");
    assert(n_obj_ids == 2);
    printf(" result: %llu, %llu\n", buf_ptr[0], buf_ptr[1]);

    // Test Suffix query
    input.op_type  = OP_SUFFIX_QUERY;
    input.attr_key = "*000key=*000val";
    // input.attr_val = "*000val";
    assert(PDC_Server_dart_perform_one_server(&input, &output, &n_obj_ids, &buf_ptr) == SUCCEED);
    printf("Suffix Query Successful! ");
    assert(n_obj_ids == 2);
    printf(" result: %llu, %llu\n", buf_ptr[0], buf_ptr[1]);

    // Test Infix query
    input.op_type  = OP_INFIX_QUERY;
    input.attr_key = "*000*=*000*";
    // input.attr_val = "*000*";
    assert(PDC_Server_dart_perform_one_server(&input, &output, &n_obj_ids, &buf_ptr) == SUCCEED);
    printf("Infix Query Successful! ");
    assert(n_obj_ids == 3);
    printf(" result: %llu, %llu, %llu\n", buf_ptr[0], buf_ptr[1], buf_ptr[2]);
}

int
main()
{
    test_PDC_Server_dart_perform_one_server();
    return 0;
}