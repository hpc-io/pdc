#include "dart_core.h"

int32_t request_count_g;

dart_server *all_servers;

dart_server
virtual_dart_retrieve_server_info_cb(uint32_t server_id)
{
    return all_servers[server_id];
}

int
main(int argc, char *argv[])
{
    DART  dart;
    char *query_str          = argv[1];
    int   num_server         = atoi(argv[2]);
    int   num_client         = num_server * 120;
    int   alphabet_size      = 27;
    int   extra_tree_height  = 0;
    int   replication_factor = num_server / 10;
    replication_factor       = replication_factor == 0 ? 1 : replication_factor;

    // Init all servers
    all_servers = (dart_server *)malloc(num_server * sizeof(dart_server));
    for (int i = 0; i < num_server; i++) {
        all_servers[i].id                 = i;
        all_servers[i].indexed_word_count = 0;
        all_servers[i].request_count      = 0;
    }

    dart_space_init(&dart, num_client, num_server, alphabet_size, extra_tree_height, replication_factor);

    println(
        "num_server: %d, num_client: %d, alphabet_size: %d, extra_tree_height: %d, replication_factor: %d",
        num_server, num_client, alphabet_size, extra_tree_height, replication_factor);
    println("DART: num_vnode: %lu", dart.num_vnode);

    index_hash_result_t *out;
    int                  array_len = DART_hash(&dart, query_str, OP_INSERT, NULL, &out);
    // print out in the same line
    printf("server ids for insert (%d): \n{", array_len);
    for (int i = 0; i < array_len; i++) {
        printf("  %d : %s,\n", out[i].server_id, out[i].key);
    }
    printf("\n}\n");

    free(out);

    array_len = DART_hash(&dart, query_str, OP_EXACT_QUERY, NULL, &out);
    printf("server ids for exact search (%d): \n{", array_len);
    for (int i = 0; i < array_len; i++) {
        printf("  %d : %s,\n", out[i].server_id, out[i].key);
    }
    printf("\n}\n");

    free(out);

    array_len = DART_hash(&dart, substring(query_str, 0, strlen(query_str) - 3), OP_PREFIX_QUERY, NULL, &out);
    printf("server ids for prefix search (%d): \n{", array_len);
    for (int i = 0; i < array_len; i++) {
        printf("  %d : %s,\n", out[i].server_id, out[i].key);
    }
    printf("\n}\n");

    free(out);

    array_len = DART_hash(&dart, substring(query_str, 3, strlen(query_str)), OP_SUFFIX_QUERY, NULL, &out);
    printf("server ids for suffix search (%d): \n{", array_len);
    for (int i = 0; i < array_len; i++) {
        printf("  %d : %s,\n", out[i].server_id, out[i].key);
    }
    printf("\n}\n");

    free(out);

    array_len = DART_hash(&dart, substring(query_str, 2, strlen(query_str) - 2), OP_INFIX_QUERY, NULL, &out);
    printf("server ids for infix search (%d): \n{", array_len);
    for (int i = 0; i < array_len; i++) {
        printf("  %d : %s,\n", out[i].server_id, out[i].key);
    }
    printf("\n}\n");

    return 0;
}