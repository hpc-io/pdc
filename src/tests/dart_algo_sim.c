#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include "string_utils.h"
#include "timer_utils.h"
#include "dart_core.h"

#define HASH_MD5    0
#define HASH_MURMUR 1
#define HASH_DART   2

typedef void (*insert_key_cb)(char *key, int prefix_len);
typedef int (*search_key_cb)(char *query_string, int prefix_len);

const int INPUT_RANDOM_STRING = 0;
const int INPUT_UUID          = 1;
const int INPUT_DICTIONARY    = 2;
const int INPUT_WIKI_KEYWORD  = 3;

DART dart_g;

int32_t request_count_g;

dart_server *all_servers;

dart_server
virtual_dart_retrieve_server_info_cb(uint32_t server_id)
{
    return all_servers[server_id];
}

// void
// md5_keyword_insert(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return;
//     int      len                              = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                          = md5_hash(len, key);
//     uint32_t server_id                        = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count = all_servers[server_id].indexed_word_count + 1;
// }

// int
// md5_keyword_search(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return 0;
//     int      len                         = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                     = md5_hash(len, key);
//     uint32_t server_id                   = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count = all_servers[server_id].request_count + 1;
// }

// void
// murmurhash_keyword_insert(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return;
//     int      len                              = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                          = murmur3_32(key, len, 1);
//     uint32_t server_id                        = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count = all_servers[server_id].indexed_word_count + 1;
// }

// int
// murmurhash_keyword_search(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return 0;
//     int      len                         = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                     = murmur3_32(key, len, 1);
//     uint32_t server_id                   = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count = all_servers[server_id].request_count + 1;
// }

// int
// djb2_hash_keyword_insert(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return;
//     int      len                              = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                          = djb2_hash(key, len);
//     uint32_t server_id                        = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count = all_servers[server_id].indexed_word_count + 1;
// }

// int
// djb2_hash_keyword_search(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return 0;
//     int      len                         = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                     = djb2_hash(key, len);
//     uint32_t server_id                   = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count = all_servers[server_id].request_count + 1;
// }

// int
// djb2_hash_keyword_insert_full(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return;
//     int      len                              = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                          = djb2_hash(key, strlen(key));
//     uint32_t server_id                        = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count = all_servers[server_id].indexed_word_count + 1;
// }

// int
// djb2_hash_keyword_search_full(char *key, int prefix_len)
// {
//     if (key == NULL)
//         return 0;
//     int      len                         = prefix_len == 0 ? strlen(key) : prefix_len;
//     uint32_t hashVal                     = djb2_hash(key, strlen(key));
//     uint32_t server_id                   = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count = all_servers[server_id].request_count + 1;
// }

void
DHT_INITIAL_keyword_insert(char *key, int prefix_len)
{
    if (key == NULL)
        return;
    index_hash_result_t *out;
    int                  arr_len = DHT_hash(&dart_g, 1, key, OP_INSERT, &out);
    int                  replica = 0;
    for (replica = 0; replica < arr_len; replica++) {
        all_servers[out[replica].server_id].indexed_word_count =
            all_servers[out[replica].server_id].indexed_word_count + 1;
    }
}

int
DHT_INITIAL_keyword_search(char *key, int prefix_len)
{
    if (key == NULL)
        return 0;
    index_hash_result_t *out;
    int                  arr_len = DHT_hash(&dart_g, 1, key, OP_INSERT, &out);
    int                  i       = 0;
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[out[i].server_id].request_count = all_servers[out[i].server_id].request_count + 1;
    }
    return 1;
}

void
DHT_FULL_keyword_insert(char *key, int prefix_len)
{
    if (key == NULL)
        return;
    index_hash_result_t *out;
    int                  arr_len = DHT_hash(&dart_g, strlen(key), key, OP_INSERT, &out);
    int                  replica = 0;
    for (replica = 0; replica < arr_len; replica++) {
        all_servers[out[replica].server_id].indexed_word_count =
            all_servers[out[replica].server_id].indexed_word_count + 1;
    }
}

int
DHT_FULL_keyword_search(char *key, int prefix_len)
{
    if (key == NULL)
        return 0;
    index_hash_result_t *out;
    int                  arr_len = DHT_hash(&dart_g, strlen(key), key, OP_INSERT, &out);
    int                  i       = 0;
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[out[i].server_id].request_count = all_servers[out[i].server_id].request_count + 1;
    }
    return 1;
}

void
dart_keyword_insert(char *key, int prefix_len)
{
    if (key == NULL)
        return;
    index_hash_result_t *out;
    int arr_len = DART_hash(&dart_g, key, OP_INSERT, virtual_dart_retrieve_server_info_cb, &out);
    int replica = 0;
    for (replica = 0; replica < arr_len; replica++) {
        all_servers[out[replica].server_id].indexed_word_count =
            all_servers[out[replica].server_id].indexed_word_count + 1;
    }
}

int
dart_keyword_search(char *key, int prefix_len)
{
    if (key == NULL)
        return 0;
    index_hash_result_t *out;
    int arr_len = DART_hash(&dart_g, key, OP_EXACT_QUERY, virtual_dart_retrieve_server_info_cb, &out);
    int i       = 0;
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[out[i].server_id].request_count = all_servers[out[i].server_id].request_count + 1;
    }
    return 1;
}

void
get_key_distribution(int num_server, char *algo)
{
    int    srv_cnt  = 0;
    double sqrt_sum = 0;
    double sum      = 0;
    double min      = 999999999999999.0;
    double max      = 0.0;
    for (srv_cnt = 0; srv_cnt < num_server; srv_cnt++) {
        dart_server server_abstract  = all_servers[srv_cnt];
        double      num_indexed_word = (double)server_abstract.indexed_word_count;
        if (min > num_indexed_word) {
            min = num_indexed_word;
        }
        if (max < num_indexed_word) {
            max = num_indexed_word;
        }
        println("[%s Key Distribution] The total number of indexed keys on server %d = %.0f", algo, srv_cnt,
                num_indexed_word);
        sum += (double)num_indexed_word;
        sqrt_sum += (double)((double)num_indexed_word * (double)num_indexed_word);
    }
    double maxRange = max - min;
    double mean     = sum / (double)num_server;
    double variance = sqrt_sum / num_server - mean * mean;
    double stddev   = sqrt(variance);

    println("[%s Key Distribution] STDDEV = %.3f and CV = %.3f for %d servers and %.0f keys in total.", algo,
            stddev, stddev / mean, num_server, sum);
}

void
get_request_distribution(int num_server, char *algo)
{
    int    srv_cnt  = 0;
    double sqrt_sum = 0;
    double sum      = 0;

    for (srv_cnt = 0; srv_cnt < num_server; srv_cnt++) {
        dart_server server_abstract = all_servers[srv_cnt];
        double      request_count   = (double)server_abstract.request_count;
        println("[%s Load Balance All] The total number of query requests on server %d = %.0f", algo, srv_cnt,
                request_count);
        sum += (double)request_count;
        sqrt_sum += (double)((double)request_count * (double)request_count);
    }
    double mean     = sum / (double)num_server;
    double variance = sqrt_sum / num_server - mean * mean;
    double stddev   = sqrt(variance);
    println("[%s Load Balance All] STDDEV = %.3f and CV = %.3f for %d servers and %.0f request in total.",
            algo, stddev, stddev / mean, num_server, sum);
}

void
reset_request_count(int num_server)
{
    int i = 0;
    for (i = 0; i < num_server; i++) {
        all_servers[i].request_count = 0;
    }
}

char **
gen_uuids(int count, int prefix_len, insert_key_cb insert_cb, search_key_cb search_cb)
{
    uuid_t out;
    int    c      = 0;
    char **result = (char **)calloc(count, sizeof(char *));
    for (c = 0; c < count; c++) {
        uuid_generate_random(out);
        result[c] = (char *)calloc(37, sizeof(char));
        uuid_unparse_lower(out, result[c]);

        if (insert_cb != NULL && search_cb != NULL) {
            insert_cb(result[c], prefix_len);

            int sch = 0;
            for (sch = 0; sch < 1; sch++) {
                search_cb(result[c], prefix_len);
            }
        }
    }
    return result;
}

char **
gen_random_strings_with_cb(int count, int minlen, int maxlen, int alphabet_size, int prefix_len,
                           insert_key_cb insert_cb, search_key_cb search_cb)
{

    int    c      = 0;
    int    i      = 0;
    char **result = (char **)calloc(count, sizeof(char *));
    for (c = 0; c < count; c++) {
        // int len = maxlen;//rand()%maxlen;
        int len   = (rand() % maxlen) + 1;
        len       = len < minlen ? minlen : len;
        char *str = (char *)calloc(len + 1, sizeof(len));
        for (i = 0; i < len; i++) {
            int randnum = rand();
            if (randnum < 0)
                randnum *= -1;
            char chr = (char)((randnum % alphabet_size) + 65);
            str[i]   = chr;
        }
        // printf("generated %s\n", str);
        result[c] = str;
        if (insert_cb != NULL && search_cb != NULL) {
            insert_cb(result[c], prefix_len);

            int sch = 0;
            for (sch = 0; sch < 1; sch++) {
                search_cb(result[c], prefix_len);
            }
        }
    }
    return result;
}

char **
read_words_from_text(const char *fileName, int *word_count, int **req_count, int prefix_len,
                     insert_key_cb insert_cb, search_key_cb search_cb)
{

    FILE *file = fopen(fileName, "r"); /* should check the result */
    if (file == NULL) {
        println("File not available\n");
        exit(4);
    }
    int lines_allocated = 128;
    int max_line_len    = 512;

    int   i;
    int   line_count = 0;
    char *line       = (char *)malloc(sizeof(char) * (2 * max_line_len));
    char *word       = (char *)malloc(sizeof(char) * (max_line_len));
    for (i = 0; 1; i++) {
        int j;
        if (fgets(line, max_line_len - 1, file) == NULL) {
            break;
        }
        /* Get rid of CR or LF at end of line */

        for (j = strlen(line) - 1; j >= 0 && (line[j] == '\n' || line[j] == '\r'); j--)
            ;
        line[j + 1] = '\0';
        int rc;
        sscanf(line, "%d %s", &rc, word);
        // (*req_count)[line_count]=rc;

        if (insert_cb != NULL && search_cb != NULL) {
            insert_cb(word, prefix_len);

            int sch = 0;
            for (sch = 0; sch < rc; sch++) {
                search_cb(word, prefix_len);
            }
        }
        line_count++;
    }
    free(line);
    free(word);
    *word_count = line_count;
    //*total_word_count = i;

    fclose(file);
    return NULL;
}

void
print_usage()
{
    println("dart_sim.exe <hash> <num_server> <input_type> <path_to_file> <alphabet_size> "
            "<replication_factor> <word_count> <prefix_len> ");
}

int
main(int argc, char **argv)
{

    if (argc < 9) {
        print_usage();
        exit(1);
    }

    int   hashalgo    = atoi(argv[1]);
    int   num_server  = atoi(argv[2]);
    int   INPUT_TYPE  = atoi(argv[3]);
    char *txtFilePath = argv[4];

    int alphabet_size, replication_factor, word_count, prefix_len;

    alphabet_size      = atoi(argv[5]);
    replication_factor = atoi(argv[6]);

    word_count             = atoi(argv[7]);
    prefix_len             = atoi(argv[8]);
    char **input_word_list = NULL;
    int *  req_count       = NULL;

    int i = 0;

    // Init all servers
    all_servers = (dart_server *)malloc(num_server * sizeof(dart_server));
    for (i = 0; i < num_server; i++) {
        all_servers[i].id                 = i;
        all_servers[i].indexed_word_count = 0;
        all_servers[i].request_count      = 0;
    }
    int   extra_tree_height = 0;
    char *algo_name         = "";
    if (hashalgo == HASH_MD5) {
        algo_name = "MD5";
    }
    else if (hashalgo == HASH_MURMUR) {
        algo_name = "MURMUR";
    }
    else if (hashalgo == HASH_DART) {
        algo_name = "DART";
    }

    println("HASH = %s", algo_name);

    void (*keyword_insert[])(char *, int) = {DHT_INITIAL_keyword_insert, DHT_FULL_keyword_insert,
                                             dart_keyword_insert};
    int (*keyword_search[])(char *, int)  = {DHT_INITIAL_keyword_search, DHT_FULL_keyword_search,
                                            dart_keyword_search};

    if (INPUT_TYPE == INPUT_DICTIONARY) {
        // Init dart space.
        alphabet_size = 29;
        dart_space_init(&dart_g, num_server, num_server, alphabet_size, extra_tree_height, replication_factor,
                        1024);
        read_words_from_text(txtFilePath, &word_count, &req_count, prefix_len, keyword_insert[hashalgo],
                             keyword_search[hashalgo]);
    }
    else if (INPUT_TYPE == INPUT_RANDOM_STRING) {
        alphabet_size = 129;
        dart_space_init(&dart_g, num_server, num_server, alphabet_size, extra_tree_height, replication_factor,
                        1024);
        gen_random_strings_with_cb(word_count, 6, 16, alphabet_size, prefix_len, keyword_insert[hashalgo],
                                   keyword_search[hashalgo]);
    }
    else if (INPUT_TYPE == INPUT_UUID) {
        alphabet_size = 37;
        dart_space_init(&dart_g, num_server, num_server, alphabet_size, extra_tree_height, replication_factor,
                        1024);
        gen_uuids(word_count, prefix_len, keyword_insert[hashalgo], keyword_search[hashalgo]);
    }
    else if (INPUT_TYPE == INPUT_WIKI_KEYWORD) {
        alphabet_size = 129;
        dart_space_init(&dart_g, num_server, num_server, alphabet_size, extra_tree_height, replication_factor,
                        1024);
        read_words_from_text(txtFilePath, &word_count, &req_count, prefix_len, keyword_insert[hashalgo],
                             keyword_search[hashalgo]);
    }

    get_key_distribution(num_server, algo_name);
    get_request_distribution(num_server, algo_name);
}
