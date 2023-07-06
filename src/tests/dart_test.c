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

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

const int INPUT_RANDOM_STRING = 0;
const int INPUT_UUID          = 1;
const int INPUT_DICTIONARY    = 2;
const int INPUT_WIKI_KEYWORD  = 3;

void
print_usage()
{
    println("Usage: srun -n <client_num> ./dart_test <input_type> <alphabet_size> <replication_factor> "
            "<word_count> /path/to/txtfile\n");
}

char **
gen_uuids(int count)
{
    uuid_t out;
    int    c      = 0;
    char **result = (char **)calloc(count, sizeof(char *));
    for (c = 0; c < count; c++) {
        uuid_generate_random(out);
        result[c] = (char *)calloc(37, sizeof(char));
        uuid_unparse_lower(out, result[c]);
    }
    return result;
}

char **
gen_random_strings(int count, int maxlen, int alphabet_size)
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
            char c = (char)((randnum % alphabet_size) + 65);
            str[i] = c;
        }
        str[len - 1] = '\0';
        // printf("generated %s\n", str);
        result[c] = str;
    }
    return result;
}

char **
read_words_from_text(const char *fileName, int *word_count, int *total_word_count, int mpi_rank)
{

    FILE *file = fopen(fileName, "r"); /* should check the result */
    if (file == NULL) {
        println("File not available\n");
        exit(4);
    }
    int    lines_allocated = 128;
    int    max_line_len    = 512;
    char **words           = (char **)malloc(sizeof(char *) * lines_allocated);
    if (words == NULL) {
        fprintf(stderr, "Out of memory\n");
        exit(1);
    }
    int i;
    int line_count = 0;
    for (i = 0; 1; i++) {
        int j;

#ifdef ENABLE_MPI
        if (i % (mpi_rank + 1) != 0) {
            // char *trash_skip_buf=(char *)calloc(max_line_len, sizeof(char));
            char trash_skip_buf[512];
            if (fgets(trash_skip_buf, max_line_len - 1, file) == NULL) {
                // free(trash_skip_buf);
                break;
            }
            // println("skip '%s'", trash_skip_buf);
            // free(trash_skip_buf);
            // i++;
            continue;
        }
#endif
        if (i >= lines_allocated) {
            int new_size;
            new_size                = lines_allocated * 2;
            char **new_wordlist_ptr = (char **)realloc(words, sizeof(char *) * new_size);
            if (new_wordlist_ptr == NULL) {
                fprintf(stderr, "Out of memory\n");
                exit(3);
            }
            words           = new_wordlist_ptr;
            lines_allocated = new_size;
        }
        words[line_count] = (char *)malloc(sizeof(char) * max_line_len);
        if (words[line_count] == NULL) {
            fprintf(stderr, "out of memory\n");
            exit(4);
        }
        if (fgets(words[line_count], max_line_len - 1, file) == NULL) {
            break;
        }
        /* Get rid of CR or LF at end of line */
        for (j = strlen(words[line_count]) - 1;
             j >= 0 && (words[line_count][j] == '\n' || words[line_count][j] == '\r'); j--)
            ;
        words[line_count][j + 1] = '\0';
        line_count++;
    }
    *word_count       = line_count;
    *total_word_count = i;

    fclose(file);
    return words;
}

int
main(int argc, char **argv)
{

    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    // println("MPI enabled!\n");
    fflush(stdout);
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (argc < 6) {
        print_usage();
        exit(1);
    }

    random_seed(0);

    int INPUT_TYPE = INPUT_UUID;

    INPUT_TYPE             = atoi(argv[1]);
    int alphabet_size      = atoi(argv[2]);
    int replication_factor = atoi(argv[3]);

    int num_client = size;

    char **input_word_list  = NULL;
    int    total_word_count = 2048;

    // total_word_count = atoi(argv[4]);

    int word_count = total_word_count / size;

    const char *dict_filename = argv[5];

    int                    index_type = atoi(argv[6]);
    dart_hash_algo_t       hash_algo  = (dart_hash_algo_t)index_type;
    dart_object_ref_type_t ref_type   = REF_PRIMARY_ID;

    if (dict_filename == NULL) {
        printf("Usage: srun -n <client_num> ./dart_test <input_type> <alphabet_size> <replication_factor> "
               "<word_count> /path/to/txtfile \n");
        exit(1);
    }

    if (INPUT_TYPE == INPUT_DICTIONARY) {
        input_word_list = read_words_from_text(dict_filename, &word_count, &total_word_count, rank);
        alphabet_size   = 29;
    }
    else if (INPUT_TYPE == INPUT_RANDOM_STRING) {
        input_word_list = gen_random_strings(word_count, 16, alphabet_size);
        alphabet_size   = 129;
    }
    else if (INPUT_TYPE == INPUT_UUID) {
        input_word_list = gen_uuids(word_count);
        alphabet_size   = 37;
    }
    else if (INPUT_TYPE == INPUT_WIKI_KEYWORD) {
        input_word_list = read_words_from_text(dict_filename, &word_count, &total_word_count, rank);
        alphabet_size   = 129;
    }

    if (rank == 0) {
        println("word_count = %d", word_count);
    }

    pdcid_t pdc = PDC_init("pdc");

    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    pdcid_t cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    DART *dart_g               = get_dart_g();
    dart_g->replication_factor = replication_factor;

    int sid = 0;
    for (sid = 0; sid < dart_g->num_server; sid++) {
        server_lookup_connection(sid, 2);
    }
    // return 0;

    int round = 0;
    for (round = 0; round < 2; round++) {
        stopwatch_t timer;
        stopwatch_t detailed_timer;
        double      insert_throughput = 0;
        double      query_throughput  = 0;
        double      delete_throughput = 0;
        double      update_throughput = 0;
        int         i                 = 0;

// /* ===============  Setup Connection By Inserting and Deleting for one time ======================= */
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        for (i = 0; i < word_count; i++) {
            int data = i;
            PDC_Client_insert_obj_ref_into_dart(index_type, input_word_list[i], input_word_list[i], ref_type,
                                                (void *)data);
        }

        for (i = 0; i < word_count; i++) {
            int data = i;
            PDC_Client_delete_obj_ref_from_dart(index_type, input_word_list[i], input_word_list[i], ref_type,
                                                (void *)data);
        }

/* ===============  Insert testing ======================= */
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif
        timer_start(&timer);

        for (i = 0; i < word_count; i++) {
            timer_start(&detailed_timer);
            int data = i;
            PDC_Client_insert_obj_ref_into_dart(index_type, input_word_list[i], input_word_list[i], ref_type,
                                                (void *)data);
            timer_pause(&detailed_timer);
            if (round == 1)
                println("[Client_Side_Insert] Time to insert key %s for both prefix index and suffix index = "
                        "%d microseconds",
                        input_word_list[i], timer_delta_us(&detailed_timer));
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_pause(&timer);

        if (rank == 0) {

            insert_throughput = (double)((double)(total_word_count) / (double)timer_delta_ms(&timer));
            if (round == 1)
                println("[Client_Side_Insert_Throughput] %.3f ops/ms", insert_throughput);

            // int srv_cnt = 0;
            // double sqrt_sum = 0;
            // double sum = 0;

            // for (srv_cnt = 0; srv_cnt < dart_g->num_server; srv_cnt++){
            //     dart_server server_abstract = dart_retrieve_server_info_cb((uint32_t)srv_cnt);
            //     int64_t num_indexed_word = server_abstract.indexed_word_count/2;
            //     println("[DART Key Distribution] Server %d has %d words indexed", srv_cnt,
            //     num_indexed_word); sum += (double)num_indexed_word; sqrt_sum +=
            //     (double)((double)num_indexed_word * (double)num_indexed_word);
            // }
            // double mean = sum/(double)dart_g->num_server;
            // double variance = sqrt_sum/dart_g->num_server - mean * mean;
            // double stddev = sqrt(variance);
            // println("[DART Key Distribution] STDDEV = %.3f for %d servers and %d keys in total.", stddev,
            // dart_g->num_server, word_count * size);
        }

        // /* ===============  Exact Query testing ======================= */

        char **query_input_list = (char **)calloc(word_count, sizeof(char *));
        for (i = 0; i < word_count; i++) {
            query_input_list[i] = (char *)calloc((strlen(input_word_list[i]) + 1), sizeof(char));
            strcpy(query_input_list[i], input_word_list[i]);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_start(&timer);
        for (i = 0; i < word_count; i++) {
            timer_start(&detailed_timer);
            int   data = i;
            char *key  = query_input_list[i];
            char  query_str[100];
            sprintf(query_str, "%s=%s", key, key);
            uint64_t **out;
            int        rest_count = 0;
            PDC_Client_search_obj_ref_through_dart(hash_algo, query_str, ref_type, &rest_count, &out);
            timer_pause(&detailed_timer);
            if (round == 1)
                println("[Client_Side_Exact] Time to search '%s' and get %d results = %d microseconds for "
                        "rank %d",
                        query_str, rest_count, timer_delta_us(&detailed_timer), rank);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_pause(&timer);

        if (rank == 0) {
            query_throughput = (double)((double)(total_word_count) / (double)timer_delta_ms(&timer));
            if (round == 1)
                println("[Client_Side_Exact_Throughput] %.3f ops/ms", query_throughput);

            int srv_cnt = 0;
            for (srv_cnt = 0; srv_cnt < dart_g->num_server; srv_cnt++) {
                dart_server server_abstract = dart_retrieve_server_info_cb((uint32_t)srv_cnt);
                if (round == 1)
                    println("[DART Load Balance 1] Server %d has query requests = %d", srv_cnt,
                            server_abstract.request_count);
            }
        }

        /* ===============  Prefix Query testing ======================= */

        for (i = 0; i < word_count; i++) {
            query_input_list[i] = (char *)calloc((strlen(input_word_list[i]) + 1), sizeof(char));
            strcpy(query_input_list[i], input_word_list[i]);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_start(&timer);
        for (i = 0; i < word_count; i++) {
            timer_start(&detailed_timer);
            int   data = i;
            char *key  = query_input_list[i];
            if (strlen(key) > 4) {
                key[4] = '\0'; // trim to prefix of 4.
            }
            char query_str[80];
            sprintf(query_str, "%s*=%s*", key, key);
            uint64_t **out;
            int        rest_count = 0;
            PDC_Client_search_obj_ref_through_dart(hash_algo, query_str, ref_type, &rest_count, &out);
            timer_pause(&detailed_timer);
            if (round == 1)
                println("[Client_Side_Prefix] Time to search '%s' and get %d results = %d microseconds for "
                        "rank %d",
                        query_str, rest_count, timer_delta_us(&detailed_timer), rank);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_pause(&timer);

        if (rank == 0) {
            query_throughput = (double)((double)(total_word_count) / (double)timer_delta_ms(&timer));
            if (round == 1)
                println("[Client_Side_Prefix_Throughput] %.3f ops/ms", query_throughput);

            int srv_cnt = 0;
            for (srv_cnt = 0; srv_cnt < dart_g->num_server; srv_cnt++) {
                dart_server server_abstract = dart_retrieve_server_info_cb((uint32_t)srv_cnt);
                if (round == 1)
                    println("[DART Load Balance 2] Server %d has query requests = %d", srv_cnt,
                            server_abstract.request_count);
            }
        }

        /* ===============  Suffix Query testing ======================= */
        for (i = 0; i < word_count; i++) {
            query_input_list[i] = (char *)calloc((strlen(input_word_list[i]) + 1), sizeof(char));
            strcpy(query_input_list[i], input_word_list[i]);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_start(&timer);
        for (i = 0; i < word_count; i++) {
            timer_start(&detailed_timer);
            int   data    = i;
            char *key     = query_input_list[i];
            int   key_len = strlen(key);
            if (key_len > 4) {
                key = &key[key_len - 4]; // trim to suffix of 4.
            }
            char query_str[80];
            sprintf(query_str, "*%s=*%s", key, key);
            uint64_t **out;
            int        rest_count = 0;
            PDC_Client_search_obj_ref_through_dart(hash_algo, query_str, ref_type, &rest_count, &out);
            timer_pause(&detailed_timer);
            if (round == 1)
                println("[Client_Side_Suffix] Time to search '%s' and get %d results = %d microseconds for "
                        "rank %d",
                        query_str, rest_count, timer_delta_us(&detailed_timer), rank);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_pause(&timer);

        if (rank == 0) {
            query_throughput = (double)((double)(total_word_count) / (double)timer_delta_ms(&timer));
            if (round == 1)
                println("[Client_Side_Suffix_Throughput] %.3f ops/ms", query_throughput);

            int srv_cnt = 0;
            for (srv_cnt = 0; srv_cnt < dart_g->num_server; srv_cnt++) {
                dart_server server_abstract = dart_retrieve_server_info_cb((uint32_t)srv_cnt);
                if (round == 1)
                    println("[DART Load Balance 3] Server %d has query requests = %d", srv_cnt,
                            server_abstract.request_count);
            }
        }

        /* ===============  Infix Query testing ======================= */
        for (i = 0; i < word_count; i++) {
            query_input_list[i] = (char *)calloc((strlen(input_word_list[i]) + 1), sizeof(char));
            strcpy(query_input_list[i], input_word_list[i]);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_start(&timer);
        for (i = 0; i < word_count; i++) {
            timer_start(&detailed_timer);
            int   data = i;
            char *key  = query_input_list[i];
            // if (strlen(key)>4){
            //     key[4] = '\0'; //trim to prefix of 4.
            // }
            if (strlen(key) > 6) {
                key    = &(key[1]);
                key[4] = '\0'; // trim to prefix of 4.
            }
            char query_str[80];
            sprintf(query_str, "*%s*=*%s*", key, key);
            uint64_t **out;
            int        rest_count = 0;
            PDC_Client_search_obj_ref_through_dart(hash_algo, query_str, ref_type, &rest_count, &out);
            timer_pause(&detailed_timer);
            if (round == 1)
                println("[Client_Side_Infix] Time to search '%s' and get %d results = %d microseconds for "
                        "rank %d",
                        query_str, rest_count, timer_delta_us(&detailed_timer), rank);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_pause(&timer);

        if (rank == 0) {
            query_throughput = (double)((double)(total_word_count) / (double)timer_delta_ms(&timer));
            if (round == 1)
                println("[Client_Side_Infix_Throughput] %.3f ops/ms", query_throughput);

            int srv_cnt = 0;
            for (srv_cnt = 0; srv_cnt < dart_g->num_server; srv_cnt++) {
                dart_server server_abstract = dart_retrieve_server_info_cb((uint32_t)srv_cnt);
                if (round == 1)
                    println("[DART Load Balance 4] Server %d has query requests = %d", srv_cnt,
                            server_abstract.request_count);
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        if (rank == 0) {
            int    srv_cnt  = 0;
            double sqrt_sum = 0;
            double sum      = 0;
            for (srv_cnt = 0; srv_cnt < dart_g->num_server; srv_cnt++) {
                dart_server server_abstract = dart_retrieve_server_info_cb((uint32_t)srv_cnt);
                int64_t     num_request     = server_abstract.request_count;
                if (round == 1)
                    println("[DART Load Balance All] The total number of query requests on server %d = %d",
                            srv_cnt, num_request);
                sum += (double)num_request;
                sqrt_sum += (double)((double)num_request * (double)num_request);
            }
            double mean     = sum / (double)dart_g->num_server;
            double variance = sqrt_sum / dart_g->num_server - mean * mean;
            double stddev   = sqrt(variance);

            // double normalSum = (sum - (mean*(double)dart_g->num_server))/stddev;
            // double normalSqrtSum = sqrt_sum-2*sum*mean+((sum*sum)/(double)dart_g->num_server);
            // double normalMean = normalSum / (double)dart_g->num_server;
            // double normalVariance = normalSqrtSum/dart_g->num_server - normalMean * normalMean;
            // double normalStdDev = sqrt(normalVariance);
            if (round == 1)
                println(
                    "[DART DART Load Balance All] STDDEV = %.3f for %d servers and %.1f request in total.",
                    stddev, dart_g->num_server, sum);

            srv_cnt  = 0;
            sqrt_sum = 0;
            sum      = 0;

            for (srv_cnt = 0; srv_cnt < dart_g->num_server; srv_cnt++) {
                dart_server server_abstract  = dart_retrieve_server_info_cb((uint32_t)srv_cnt);
                int64_t     num_indexed_word = server_abstract.indexed_word_count / 2;
                if (round == 1)
                    println("[DART Key Distribution] Server %d has %d words indexed", srv_cnt,
                            num_indexed_word);
                sum += (double)num_indexed_word;
                sqrt_sum += (double)((double)num_indexed_word * (double)num_indexed_word);
            }
            mean     = sum / (double)dart_g->num_server;
            variance = sqrt_sum / dart_g->num_server - mean * mean;
            stddev   = sqrt(variance);

            // normalSum = (sum - (mean*(double)dart_g->num_server))/stddev;
            // normalSqrtSum = sqrt_sum-2*sum*mean+((sum*sum)/(double)dart_g->num_server);
            // normalMean = normalSum / (double)dart_g->num_server;
            // normalVariance = normalSqrtSum/dart_g->num_server - normalMean * normalMean;
            // normalStdDev = sqrt(normalVariance);

            if (round == 1)
                println("[DART Key Distribution] STDDEV = %.3f for %d servers and %d keys in total.", stddev,
                        dart_g->num_server, word_count * size);
        }

/* =========================== Delete From Index ========================= */
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif
        timer_start(&timer);

        for (i = 0; i < word_count; i++) {
            timer_start(&detailed_timer);
            int data = i;
            PDC_Client_delete_obj_ref_from_dart(hash_algo, input_word_list[i], input_word_list[i], ref_type,
                                                (void *)data);
            timer_pause(&detailed_timer);
            if (round == 1)
                println("[Client_Side_Delete] Time to delete key %s for both prefix index and suffix index = "
                        "%d microseconds",
                        input_word_list[i], timer_delta_us(&detailed_timer));
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        timer_pause(&timer);

        if (rank == 0) {

            delete_throughput = (double)((double)(total_word_count) / (double)timer_delta_ms(&timer));
            if (round == 1)
                println("[Client_Side_Delete_Throughput] %.3f ops/ms", delete_throughput);
        }

    } // end round loop

done:

    if (PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
