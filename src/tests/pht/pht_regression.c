#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc_server_metadata_pht.h"

#define CHECK(condition)                                                                                     \
    do {                                                                                                     \
        if (!(condition)) {                                                                                  \
            fprintf(stderr, "%s:%d: %s failed\n", __FILE__, __LINE__, #condition);                           \
            return 1;                                                                                        \
        }                                                                                                    \
    } while (0)

static int
compare_keys(void *a, void *b)
{
    return strcmp(a, b);
}

static int
check_ranges(void)
{
    DoublyLinkedList *list = dllist_init(string_hash, compare_keys);
    CHECK(list);
    for (int i = 0; i < 3; i++) {
        char  text[2] = {(char)('a' + i), '\0'};
        char *key = strdup(text), *value = strdup(text);
        Set * values = set_new(string_hash, string_equal);
        CHECK(key && value && values);
        set_register_free_function(values, free);
        CHECK(set_insert(values, value));
        CHECK(dllist_insert(list, key, values) == 0);
    }
    for (int include_start = 0; include_start < 2; include_start++) {
        for (int include_end = 0; include_end < 2; include_end++) {
            Set *result = dllist_search_range(list, "a", include_start, "c", include_end);
            CHECK(result && set_num_entries(result) == (unsigned)(1 + include_start + include_end));
            CHECK(set_query(result, "b"));
            CHECK(!!set_query(result, "a") == include_start);
            CHECK(!!set_query(result, "c") == include_end);
            set_free(result);
        }
    }
    Set *empty = dllist_search_range(list, "z", true, "zz", true);
    CHECK(empty && set_num_entries(empty) == 0);
    set_free(empty);
    CHECK(dllist_destroy(list) == 0);
    return 0;
}

int
main(void)
{
    metadata_check_prefix_in_t   query = {.prefix = "missing"};
    metadata_check_prefix_out_t  found;
    metadata_create_bucket_in_t  create = {.prefix = "#"};
    metadata_create_bucket_out_t created;
    metadata_key_add_out_t       added;
    unsigned char                bytes[] = {0, 0xff, 0x80, 0, 0x7f};
    metadata_key_add_in_t add = {.prefix = "#", .key = "binary", .value = bytes, .size = sizeof(bytes)};

    CHECK(check_ranges() == 0);
    CHECK(PDC_get_server_using_pht(0, 0) == UINT32_MAX);
    /* Every server owns one complete arc, including both boundary slots. */
    for (uint32_t servers = 1; servers <= 8; servers++) {
        unsigned int counts[8] = {0};
        uint64_t     ring      = (uint64_t)servers * RING_ARC_SIZE;
        for (uint64_t hash = 0; hash < ring; hash++) {
            uint32_t server = PDC_get_server_using_pht(hash, servers);
            CHECK(server < servers);
            CHECK(PDC_get_server_using_pht(hash + ring, servers) == server);
            counts[server]++;
        }
        for (uint32_t server = 0; server < servers; server++)
            CHECK(counts[server] == RING_ARC_SIZE);
    }
    CHECK(PDC_Server_check_prefix(&query, &found) == FAIL && found.ret == -1);
    PDC_Server_metadata_pht_init(4, 2);
    CHECK(PDC_Server_check_prefix(&query, &found) == SUCCEED);
    CHECK(!found.found && !found.leaf && found.server_id == 2 && found.ret == 1);
    CHECK(PDC_Server_metadata_key_add(&add, &added) == FAIL && added.ret == -1);
    CHECK(PDC_Server_create_bucket(&create, &created) == SUCCEED && created.ret == 1);
    CHECK(PDC_Server_create_bucket(&create, &created) == SUCCEED && created.ret == 1);
    query.prefix = "#";
    CHECK(PDC_Server_check_prefix(&query, &found) == SUCCEED && found.found && found.leaf);
    /* Freeing the RPC input must not invalidate the stored value. The repeated
     * insertion then compares against the server's retained copy under ASan. */
    add.value = malloc(sizeof(bytes));
    CHECK(add.value);
    memcpy(add.value, bytes, sizeof(bytes));
    CHECK(PDC_Server_metadata_key_add(&add, &added) == SUCCEED && added.ret == 1);
    free(add.value);
    add.value = bytes;
    CHECK(PDC_Server_metadata_key_add(&add, &added) == SUCCEED && added.ret == 1);
    char key[32];
    for (unsigned int i = 1; i < PHT_BUCKET_SIZE; i++) {
        snprintf(key, sizeof(key), "key%u", i);
        add.key = key;
        CHECK(PDC_Server_metadata_key_add(&add, &added) == SUCCEED && added.ret == 1);
    }
    add.key = "overflow";
    CHECK(PDC_Server_metadata_key_add(&add, &added) == FAIL && added.ret == -1);
    CHECK(PDC_Server_check_prefix(&query, &found) == SUCCEED && found.found && found.leaf);
    add.key  = "binary";
    bytes[0] = 42;
    CHECK(PDC_Server_metadata_key_add(&add, &added) == SUCCEED && added.ret == 1);
    add.prefix = "missing";
    CHECK(PDC_Server_metadata_key_add(&add, &added) == FAIL && added.ret == -1);
    PDC_Server_metadata_pht_finalize();
    CHECK(PDC_Server_check_prefix(&query, &found) == FAIL && found.ret == -1);
    puts("PHT collection and server regressions passed");
    return 0;
}
