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

// void md5_keyword_insert(char *key, int prefix_len){
//     if (key==NULL) return;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = md5_hash(len, key);
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count=all_servers[server_id].indexed_word_count+1;
// }

// int md5_keyword_search(char *key, int prefix_len){
//     if (key==NULL) return 0;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = md5_hash(len, key);
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count=all_servers[server_id].request_count+1;
// }

// void murmurhash_keyword_insert(char *key, int prefix_len){
//     if (key==NULL) return ;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = murmur3_32(key, len, 1);
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count=all_servers[server_id].indexed_word_count+1;
// }

// int murmurhash_keyword_search(char *key, int prefix_len){
//     if (key==NULL) return 0;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = murmur3_32(key, len, 1);
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count=all_servers[server_id].request_count+1;
// }

// int djb2_hash_keyword_insert(char *key, int prefix_len){
//     if (key==NULL) return ;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = djb2_hash(key, len);
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count=all_servers[server_id].indexed_word_count+1;
// }

// int djb2_hash_keyword_search(char *key, int prefix_len){
//     if (key==NULL) return 0;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = djb2_hash(key, len);
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count=all_servers[server_id].request_count+1;
// }

// int djb2_hash_keyword_insert_full(char *key, int prefix_len){
//     if (key==NULL) return ;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = djb2_hash(key, strlen(key));
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].indexed_word_count=all_servers[server_id].indexed_word_count+1;
// }

// int djb2_hash_keyword_search_full(char *key, int prefix_len){
//     if (key==NULL) return 0;
//     int len = prefix_len==0?strlen(key):prefix_len;
//     uint32_t hashVal = djb2_hash(key, strlen(key));
//     uint32_t server_id = hashVal % dart_g.num_server;
//     all_servers[server_id].request_count=all_servers[server_id].request_count+1;
// }

void
DHT_INITIAL_keyword_insert(char *key, int prefix_len)
{
    if (key == NULL)
        return;
    uint64_t *server_id;
    int       arr_len = DHT_hash(&dart_g, 1, key, OP_INSERT, &server_id);
    int       replica = 0;
    for (replica = 0; replica < arr_len; replica++) {
        all_servers[server_id[replica]].indexed_word_count =
            all_servers[server_id[replica]].indexed_word_count + 1;
    }
}

int
DHT_INITIAL_keyword_search(char *key, int prefix_len)
{
    if (key == NULL)
        return 0;
    uint64_t *server_id;
    int       arr_len = DHT_hash(&dart_g, 1, key, OP_INSERT, &server_id);
    int       i       = 0;
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[server_id[i]].request_count = all_servers[server_id[i]].request_count + 1;
    }
    return 1;
}

void
DHT_FULL_keyword_insert(char *key, int prefix_len)
{
    if (key == NULL)
        return;
    uint64_t *server_id;
    int       arr_len = DHT_hash(&dart_g, strlen(key), key, OP_INSERT, &server_id);
    int       replica = 0;
    for (replica = 0; replica < arr_len; replica++) {
        all_servers[server_id[replica]].indexed_word_count =
            all_servers[server_id[replica]].indexed_word_count + 1;
    }
}

int
DHT_FULL_keyword_search(char *key, int prefix_len)
{
    if (key == NULL)
        return 0;
    uint64_t *server_id;
    int       arr_len = DHT_hash(&dart_g, strlen(key), key, OP_INSERT, &server_id);
    int       i       = 0;
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[server_id[i]].request_count = all_servers[server_id[i]].request_count + 1;
    }
    return 1;
}

void
dart_keyword_insert(char *key, int prefix_len)
{
    if (key == NULL)
        return;
    uint64_t *server_id;
    int       arr_len = DART_hash(&dart_g, key, OP_INSERT, virtual_dart_retrieve_server_info_cb, &server_id);
    int       replica = 0;
    for (replica = 0; replica < arr_len; replica++) {
        all_servers[server_id[replica]].indexed_word_count =
            all_servers[server_id[replica]].indexed_word_count + 1;
    }
}

int
dart_keyword_search(char *key, int prefix_len)
{
    if (key == NULL)
        return 0;
    uint64_t *server_id;
    int arr_len = DART_hash(&dart_g, key, OP_EXACT_QUERY, virtual_dart_retrieve_server_info_cb, &server_id);
    int i       = 0;
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[server_id[i]].request_count = all_servers[server_id[i]].request_count + 1;
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
gen_random_strings(int count, int maxlen, int alphabet_size, int prefix_len, insert_key_cb insert_cb,
                   search_key_cb search_cb)
{

    int    c      = 0;
    int    i      = 0;
    char **result = (char **)calloc(count, sizeof(char *));
    for (c = 0; c < count; c++) {
        // int len = maxlen;//rand()%maxlen;
        int   len = rand() % maxlen;
        char *str = (char *)calloc(len, sizeof(len));
        for (i = 0; i < len - 1; i++) {
            int randnum = rand();
            if (randnum < 0)
                randnum *= -1;
            char chr = (char)((randnum % alphabet_size) + 65);
            str[i]   = chr;
        }
        str[len - 1] = '\0';
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

    int server_num = atoi(argv[1]);
    int al_size    = atoi(argv[2]);
    int rep_fct    = atoi(argv[3]);

    // int round = 1;
    // for (;round < 1024; round++) {

    int num_server = server_num;
    int i          = 0;
    // Init all servers
    all_servers = (dart_server *)malloc(num_server * sizeof(dart_server));
    for (i = 0; i < num_server; i++) {
        all_servers[i].id                 = i;
        all_servers[i].indexed_word_count = 0;
        all_servers[i].request_count      = 0;
    }

    int alphabet_size = al_size;

    dart_space_init(&dart_g, num_server, num_server, alphabet_size, 0, rep_fct);

    char *    key     = "abcabc";
    char *    rev_key = reverse_str(key);
    int       arr_len = 0;
    uint64_t *server_id;
    for (i = 0; i < 2; i++) {
        char *token = i == 0 ? key : rev_key;
        arr_len     = DART_hash(&dart_g, token, OP_INSERT, virtual_dart_retrieve_server_info_cb, &server_id);
        int replica = 0;
        for (replica = 0; replica < arr_len; replica++) {
            all_servers[server_id[replica]].indexed_word_count =
                all_servers[server_id[replica]].indexed_word_count + 1;
            printf("insert server id = %d for key %s\n", server_id[replica], token);
        }
    }

    arr_len = DART_hash(&dart_g, "abcabc", OP_EXACT_QUERY, NULL, &server_id);
    if (arr_len != 2)
        for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
            all_servers[server_id[i]].request_count = all_servers[server_id[i]].request_count + 1;
            printf("exact search server id = %d\n", server_id[i]);
        }

    arr_len = DART_hash(&dart_g, "abc", OP_PREFIX_QUERY, NULL, &server_id);
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[server_id[i]].request_count = all_servers[server_id[i]].request_count + 1;
        printf("prefix search server id = %d\n", server_id[i]);
    }

    arr_len = DART_hash(&dart_g, "cba", OP_SUFFIX_QUERY, NULL, &server_id);
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[server_id[i]].request_count = all_servers[server_id[i]].request_count + 1;
        printf("suffix search server id = %d\n", server_id[i]);
    }

    arr_len = DART_hash(&dart_g, "bc", OP_INFIX_QUERY, NULL, &server_id);
    for (i = 0; i < arr_len; i++) { // Perhaps we can use openmp for this loop?
        all_servers[server_id[i]].request_count = all_servers[server_id[i]].request_count + 1;
        printf("infix search server id = %d\n", server_id[i]);
    }
    // }
}