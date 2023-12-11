#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "string_utils.h"

#define MAX_LINE_LENGTH  1024
#define MAX_COLUMNS      8
#define MAX_ROWS_TO_READ 1000
#define DUPLICATE_ROWS   1000

int
assign_work_to_rank(int rank, int size, int nwork, int *my_count, int *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        printf("assign_work_to_rank(): Error with input!\n");
        return -1;
    }
    if (nwork < size) {
        if (rank < nwork)
            *my_count = 1;
        else
            *my_count = 0;
        (*my_start) = rank * (*my_count);
    }
    else {
        (*my_count) = nwork / size;
        (*my_start) = rank * (*my_count);

        // Last few ranks may have extra work
        if (rank >= size - nwork % size) {
            (*my_count)++;
            (*my_start) += (rank - (size - nwork % size));
        }
    }

    return 1;
}

void
print_usage(char *name)
{
    printf("%s n_obj n_round n_selectivity is_using_dart query_type comm_type\n", name);
    printf("Summary: This test will create n_obj objects, and add n_selectivity tags to each object. Then it "
           "will "
           "perform n_round collective queries against the tags, each query from each client should get "
           "a whole result set.\n");
    printf("Parameters:\n");
    printf("  n_obj: number of objects\n");
    printf("  n_round: number of rounds, it can be the total number of tags too, as each round will perform "
           "one query against one tag\n");
    printf("  n_selectivity: selectivity, on a 100 scale. \n");
    printf("  is_using_dart: 1 for using dart, 0 for not using dart\n");
    printf("  query_type: -1 for no query, 0 for exact, 1 for prefix, 2 for suffix, 3 for infix\n");
    printf("  comm_type: 0 for point-to-point, 1 for collective\n");
}

perr_t
prepare_container(pdcid_t *pdc, pdcid_t *cont_prop, pdcid_t *cont, pdcid_t *obj_prop, int my_rank)
{
    perr_t ret_value = FAIL;
    // create a pdc
    *pdc = PDCinit("pdc");

    // create a container property
    *cont_prop = PDCprop_create(PDC_CONT_CREATE, *pdc);
    if (*cont_prop <= 0) {
        printf("[Client %d] Fail to create container property @ line  %d!\n", my_rank, __LINE__);
        goto done;
    }
    // create a container
    *cont = PDCcont_create("c1", *cont_prop);
    if (*cont <= 0) {
        printf("[Client %d] Fail to create container @ line  %d!\n", my_rank, __LINE__);
        goto done;
    }

    // create an object property
    *obj_prop = PDCprop_create(PDC_OBJ_CREATE, *pdc);
    if (*obj_prop <= 0) {
        printf("[Client %d] Fail to create object property @ line  %d!\n", my_rank, __LINE__);
        goto done;
    }

    ret_value = SUCCEED;
done:
    return ret_value;
}

size_t
create_objects(pdcid_t **obj_ids, int my_csv_rows, int csv_expand_factor, pdcid_t cont, pdcid_t obj_prop,
               int my_rank)
{
    size_t  obj_created = 0;
    char    obj_name[128];
    int64_t timestamp = get_timestamp_us();

    *obj_ids = (pdcid_t *)calloc(my_csv_rows * csv_expand_factor, sizeof(pdcid_t));
    for (int i = 0; i < my_csv_rows; i++) {
        // create `csv_expansion_factor` data objects for each csv row.
        for (int obj_idx = 0; obj_idx < csv_expand_factor; obj_idx++) {
            sprintf(obj_name, "obj%" PRId64 "%d", timestamp, obj_created);
            (*obj_ids)[obj_created] = PDCobj_create(cont, obj_name, obj_prop);

            if ((*obj_ids)[obj_created] <= 0) {
                printf("[Client %d] Fail to create object @ line  %d!\n", my_rank, __LINE__);
                goto done;
            }

            obj_created++;
        }
    }
done:
    return obj_created;
}

// Function to split a line into tokens based on the delimiter
int
split_line(char *line, char delimiter, char **tokens, int max_tokens)
{
    int   count = 0;
    char *token;

    token = strtok(line, &delimiter);
    while (token != NULL && count < max_tokens) {
        strncpy(tokens[count], token, strlen(token) + 1);
        count++;
        token = strtok(NULL, &delimiter);
    }

    return count;
}

int
read_lines_to_buffer(const char *filename, char **buffer, int num_lines, size_t *buffer_size)
{
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Error opening file");
        return -1;
    }

    const size_t max_line_length = 1024;
    char         line[max_line_length];
    size_t       total_size = 0;

    // Calculate the total size required
    for (int i = 0; i < num_lines && fgets(line, max_line_length, file) != NULL; i++) {
        total_size += strlen(line);
    }

    // Allocate the buffer
    *buffer = (char *)calloc(total_size, sizeof(char) + 1);
    if (*buffer == NULL) {
        perror("Failed to allocate buffer");
        fclose(file);
        return -1;
    }

    // Reset file pointer to the beginning of the file
    fseek(file, 0, SEEK_SET);

    // Read lines into the buffer
    char *buf_ptr = *buffer;
    for (int i = 0; i < num_lines && fgets(buf_ptr, max_line_length, file) != NULL; i++) {
        size_t len = strlen(buf_ptr);
        buf_ptr += len; // Move pointer to the end of the read line
    }

    *buffer_size = total_size;
    fclose(file);

    return 0;
}

typedef int (*process_tag_of_one_object)(pid_t obj_id, char *attr_name, char *attr_value, int is_using_dart);

int
add_tag_to_one_object(pid_t obj_id, char *attr_name, char *attr_value, int is_using_dart)
{
    pdc_kvtag_t kvtag;
    kvtag.name  = strdup(attr_name);
    kvtag.value = (void *)strdup(attr_value);
    kvtag.type  = PDC_STRING;
    kvtag.size  = (strlen(kvtag.value) + 1) * sizeof(char);

    if (PDCobj_put_tag(obj_id, kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0) {
        printf("Fail to add tag to object %" PRIu64 "\n", obj_id);
        return -1;
    }
    if (is_using_dart) {
        PDC_Client_insert_obj_ref_into_dart(DART_HASH, kvtag.name, (char *)kvtag.value, REF_PRIMARY_ID,
                                            (uint64_t)obj_id);
    }
    return 0;
}

int
delete_tag_from_one_object(pid_t obj_id, char *attr_name, char *attr_value, int is_using_dart)
{
    pdc_kvtag_t kvtag;
    kvtag.name  = strdup(attr_name);
    kvtag.value = (void *)strdup(attr_value);
    kvtag.type  = PDC_STRING;
    kvtag.size  = (strlen(kvtag.value) + 1) * sizeof(char);

    PDCobj_del_tag(obj_id, kvtag.name);
    if (is_using_dart) {
        PDC_Client_delete_obj_ref_from_dart(DART_HASH, kvtag.name, (char *)kvtag.value, REF_PRIMARY_ID,
                                            (uint64_t)obj_id);
    }
    return 1;
}

int
csv_tags_on_objects(pdcid_t *obj_ids, char ***csv_data, char **csv_header, int num_columns, int my_csv_rows,
                    int csv_expand_factor, int is_using_dart, process_tag_of_one_object tag_processor)
{

    pdc_kvtag_t kvtag;
    size_t      tags_processed = 0;
    size_t      obj_idx        = 0;
    for (int i = 0; i < my_csv_rows; i++) {
        // take one row from csv_data.
        char **row = csv_data[i];

        for (int j = 0; j < csv_expand_factor; j++) {

            char new_iter_value[30];
            sprintf(new_iter_value, "Scan_Iter_%04d", j);
            char new_iter_tok[10];
            sprintf(new_iter_tok, "%04dt.tif", j);

            char extra_attr_name[100];
            char extra_attr_value[200];

            for (int col_idx = 0; col_idx < num_columns; col_idx++) {
                char *attr_name  = strdup(csv_header[col_idx]);
                char *attr_value = strdup(row[col_idx]);
                if (strstr(attr_value, "Scan_Iter_0000")) {
                    char *start = strstr(attr_value, "Scan_Iter_0000");
                    strncpy(start, new_iter_value, strlen(new_iter_value));
                    char *zerot = strstr(attr_value, "0000t.tif");
                    strncpy(zerot, new_iter_tok, strlen(new_iter_tok));

                    if (startsWith(attr_name, "Filepath")) {
                        sprintf(extra_attr_value, "%s", attr_value);
                        // remove the last 4 characters, namely '.tif'.
                        extra_attr_value[strlen(extra_attr_value) - 4] = '\0';
                    }
                    else if (startsWith(attr_name, "Filename")) {
                        sprintf(extra_attr_name, "%s", attr_value);
                        // remove the last 4 characters, namely '.tif'.
                        extra_attr_name[strlen(extra_attr_name) - 4] = '\0';
                    }
                }
                tag_processor(obj_ids[obj_idx], attr_name, attr_value, is_using_dart);
                tags_processed++;
            }
            // add one extra tag
            tag_processor(obj_ids[obj_idx], extra_attr_name, extra_attr_value, is_using_dart);
            tags_processed++;
            obj_idx++;
        }
    }
    return tags_processed;
}

int
read_csv_from_buffer(char *data, char ***csv_header, char ****csv_data, int *num_columns, int rows_to_read,
                     int my_rank, int proc_num)
{
    int my_csv_row_num = 0;
    // Allocate memory for the header and data
    *csv_header = (char **)calloc(MAX_COLUMNS, sizeof(char *));
    *csv_data   = (char ***)calloc(rows_to_read / proc_num + 1, sizeof(char **));
    for (int i = 0; i < rows_to_read / proc_num + 1; i++) {
        (*csv_data)[i] = (char **)calloc(MAX_COLUMNS, sizeof(char *));
    }

    // Read the header line
    char *line = strtok(data, "\n");
    if (line == NULL) {
        fprintf(stderr, "Error reading headers from CSV\n");
        return -1;
    }

    // Parse the header line
    char *header[MAX_COLUMNS];
    *num_columns = split_line(line, ',', *csv_header, MAX_COLUMNS);

    // Read and parse the data lines
    int data_line_count = 0;
    while (data_line_count < rows_to_read) {
        line = strtok(NULL, "\n");
        if (line == NULL) {
            fprintf(stderr, "Error reading data from CSV\n");
            // free(buffer);
            return -1;
        }
        if (data_line_count % proc_num == my_rank) {

            // Parse the data line
            char *tmp_data[MAX_COLUMNS];
            *num_columns = split_line(line, ',', tmp_data, MAX_COLUMNS);

            // Copy the data into the csv_data array
            for (int i = 0; i < *num_columns; i++) {
                (*csv_data)[my_csv_row_num][i] = strdup(tmp_data[i]);
            }
            my_csv_row_num++;
        }

        data_line_count++;
    }

    return my_csv_row_num;
}

int
perform_search(int is_using_dart, int query_type, int comm_type, int iter_round)
{
    perr_t    ret_value;
    int       nres    = 0;
    uint64_t *pdc_ids = NULL;
    // perform search
    char attr_name[100];
    char tag_value[200];
    snprintf(attr_name, 100,
             "Scan_Iter_%04d_CamA_ch0_CAM1_stack0000_488nm_0000000msec_0%3d511977msecAbs_000x_"
             "000y_%03dz_%04dt",
             iter_round, iter_round, iter_round, iter_round);
    snprintf(tag_value, 200,
             "/clusterfs/nvme2/Data/20221128_Korra_GaoGroupVisit/Data/20221213_OB_WT/V1_600um//"
             "Scan_Iter_%04d_CamA_ch0_CAM1_stack0000_488nm_0000000msec_0%3d511977msecAbs_000x_000y_%03dz_"
             "%04dt",
             iter_round, iter_round, iter_round, iter_round);

    pdc_kvtag_t kvtag;
    kvtag.name  = strdup(attr_name);
    kvtag.value = (void *)strdup(tag_value);
    kvtag.type  = PDC_STRING;
    kvtag.size  = (strlen(tag_value) + 1) * sizeof(char);

    query_gen_input_t  input;
    query_gen_output_t output;
    input.base_tag         = &kvtag;
    input.key_query_type   = query_type;
    input.value_query_type = query_type;
    input.affix_len        = 14;

    gen_query_key_value(&input, &output);

    if (is_using_dart) {
        char *query_string = gen_query_str(&output);
        ret_value          = (comm_type == 0)
                        ? PDC_Client_search_obj_ref_through_dart(DART_HASH, query_string, REF_PRIMARY_ID,
                                                                 &nres, &pdc_ids)
                        : PDC_Client_search_obj_ref_through_dart_mpi(DART_HASH, query_string, REF_PRIMARY_ID,
                                                                     &nres, &pdc_ids, MPI_COMM_WORLD);
    }
    else {
        kvtag.name  = output.key_query;
        kvtag.value = output.value_query;
        /* fprintf(stderr, "    Rank %d: key [%s] value [%s]\n", my_rank, kvtag.name,
         * kvtag.value); */
        ret_value = (comm_type == 0) ? PDC_Client_query_kvtag(&kvtag, &nres, &pdc_ids)
                                     : PDC_Client_query_kvtag_mpi(&kvtag, &nres, &pdc_ids, MPI_COMM_WORLD);
    }

    return nres;
}

int
main(int argc, char *argv[])
{
    pdcid_t     pdc, cont_prop, cont, obj_prop;
    pdcid_t *   obj_ids;
    int         n_obj, my_csv_rows, num_columns;
    int         proc_num, my_rank, i, v, iter, round, csv_expand_factor, is_using_dart, query_type, comm_type;
    double      stime, total_time;
    pdc_kvtag_t kvtag;
    uint64_t *  pdc_ids;
    int         nres, ntotal;
    int *       my_cnt_round;
    int *       total_cnt_round;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif

    if (argc < 8) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj             = atoi(argv[1]);
    round             = atoi(argv[2]);
    csv_expand_factor = atoi(argv[3]);
    is_using_dart     = atoi(argv[4]); // 0 for no index, 1 for using dart.
    query_type        = atoi(argv[5]); // 0 for exact, 1 for prefix, 2 for suffix, 3 for infix,
                                       // 4 for num_exact, 5 for num_range
    comm_type       = atoi(argv[6]);   // 0 for point-to-point, 1 for collective
    char *file_name = argv[7];

    int rows_to_read = n_obj / csv_expand_factor; // read 1 more row which is the header

    int bypass_query = query_type == -1 ? 1 : 0;
    // prepare container
    if (prepare_container(&pdc, &cont_prop, &cont, &obj_prop, my_rank) < 0) {
        println("fail to prepare container @ line %d", __LINE__);
        goto done;
    }

    // ********************** Read and Broadcast first few rows of CSV file **********************
    char * data      = NULL;
    size_t data_size = 0;

    if (my_rank == 0) {
        if (read_lines_to_buffer("your_file.csv", &data, rows_to_read + 1, &data_size) != 0) {
            fprintf(stderr, "Failed to read lines from the file\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    // Broadcast the buffer size first
    MPI_Bcast(&data_size, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);

    // Allocate memory for other ranks
    if (my_rank != 0) {
        data = (char *)malloc(data_size * sizeof(char));
        if (data == NULL) {
            fprintf(stderr, "Failed to allocate buffer\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    // Broadcast the data
    MPI_Bcast(data, data_size, MPI_CHAR, 0, MPI_COMM_WORLD);

    // ********************** Parse these rows of CSV file **********************
    // read the CSV file and parse into data
    char ** csv_header = (char **)calloc(MAX_COLUMNS, sizeof(char *));
    char ***csv_data   = NULL;
    my_csv_rows =
        read_csv_from_buffer(data, &csv_header, &csv_data, &num_columns, rows_to_read, my_rank, proc_num);

    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();

    size_t obj_created = create_objects(&obj_ids, my_csv_rows, csv_expand_factor, cont, obj_prop, my_rank);

    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;

    if (my_rank == 0) {
        println("[Object Creation] Rank %d/%d: Created %d objects, time: %.5f ms", my_rank, proc_num,
                obj_created, total_time * 1000.0);
    }

    // ********************** Add tags to objects **********************
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();

    size_t tags_added = csv_tags_on_objects(obj_ids, csv_data, csv_header, num_columns, my_csv_rows,
                                            csv_expand_factor, is_using_dart, add_tag_to_one_object);

    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;

    if (my_rank == 0) {
        println("[Tag Creation] Rank %d/%d: Added %d tags for %d objects, time: %.5f ms", my_rank, proc_num,
                tags_added, obj_created, total_time * 1000.0);
    }

    if (bypass_query) {
        if (my_rank == 0) {
            println("Rank %d: All queries are bypassed.", my_rank);
            report_avg_server_profiling_rst();
        }
        goto done;
    }

    // ********************** Perform queries **********************
    int iter_round = round;
    if (comm_type == 0 && is_using_dart == 0) {
        iter_round = 2;
    }

    for (comm_type = 1; comm_type >= 0; comm_type--) {
        for (query_type = 0; query_type < 4; query_type++) {
            perr_t ret_value;
            int    round_total = 0;
            for (iter = -1; iter < iter_round; iter++) { // -1 is for warm up
#ifdef ENABLE_MPI
                if (iter == 0) {
                    MPI_Barrier(MPI_COMM_WORLD);
                    stime = MPI_Wtime();
                }
#endif
                nres = perform_search(is_using_dart, query_type, comm_type, iter_round);
                round_total += nres;
            }

#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
            // MPI_Reduce(&round_total, &ntotal, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            total_time = MPI_Wtime() - stime;

            if (my_rank == 0) {
                char *query_type_str = "EXACT";
                if (query_type == 1)
                    query_type_str = "PREFIX";
                else if (query_type == 2)
                    query_type_str = "SUFFIX";
                else if (query_type == 3)
                    query_type_str = "INFIX";
                println("[%s Client %s Query with%sINDEX] %d rounds (%d) within %.5f ms",
                        comm_type == 0 ? "Single" : "Multi", query_type_str,
                        is_using_dart == 0 ? " NO " : " DART ", round, round_total, total_time * 1000.0);
            }
#endif
        } // end query type
    }     // end comm type

    if (my_rank == 0) {
        println("Rank %d: All queries are done.", my_rank);
        report_avg_server_profiling_rst();
    }

    // delete all tags
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();

    size_t tags_deleted = csv_tags_on_objects(obj_ids, csv_data, csv_header, num_columns, my_csv_rows,
                                              csv_expand_factor, is_using_dart, delete_tag_from_one_object);

    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
    if (my_rank == 0) {
        println("[TAG Deletion] Rank %d/%d: Deleted %d kvtag from %d objects, time: %.5f ms", my_rank,
                proc_num, tags_deleted, obj_created, total_time * 1000.0);
    }

done:
    // close a container
    if (PDCcont_close(cont) < 0) {
        if (my_rank == 0) {
            printf("fail to close container c1\n");
        }
    }
    else {
        if (my_rank == 0)
            printf("successfully close container c1\n");
    }

    // close an object property
    if (PDCprop_close(obj_prop) < 0) {
        if (my_rank == 0)
            printf("Fail to close property @ line %d\n", __LINE__);
    }
    else {
        if (my_rank == 0)
            printf("successfully close object property\n");
    }

    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        if (my_rank == 0)
            printf("Fail to close property @ line %d\n", __LINE__);
    }
    else {
        if (my_rank == 0)
            printf("successfully close container property\n");
    }

    // close pdc
    if (PDCclose(pdc) < 0) {
        if (my_rank == 0)
            printf("fail to close PDC\n");
    }

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}