/**
 * This is a JSON loader that reads the metadata from a JSON file.
 *
 * The JSON file is expected to have the following format:
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "cjson/cJSON.h"
#include <dirent.h>
#include <unistd.h>
#include "fs/fs_ops.h"
#include "string_utils.h"

#ifdef JMD_DEBUG
#include "meta_json/metadata_json_printer.h"
#else
#include "meta_json/metadata_json_importer.h"
#endif

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

#define MAX_JSON_FILE_SIZE 1000000

MD_JSON_PROCESSOR *md_json_processor;

typedef struct {
    int current_file_count;
    int processed_file_count;
    int mpi_size;
    int mpi_rank;
} meta_json_loader_args_t;

static void
initilize_md_json_processor()
{
#ifdef JMD_DEBUG
    md_json_processor = create_md_json_printer();
#else
    md_json_processor = create_md_json_importer();
#endif
}

static char *
read_json_file(const char *filename)
{
    FILE *fp;
    char *json_str;
    long  json_file_size;

    fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: cannot open file %s\n", filename);
        exit(1);
    }

    fseek(fp, 0L, SEEK_END);
    json_file_size = ftell(fp);
    rewind(fp);

    // if (json_file_size > MAX_JSON_FILE_SIZE) {
    //     fprintf(stderr, "Error: file %s is too large\n", filename);
    //     exit(1);
    // }

    json_str = (char *)malloc(json_file_size + 1);
    if (json_str == NULL) {
        fprintf(stderr, "Error: cannot allocate memory for json_str\n");
        exit(1);
    }

    size_t bytesRead = fread(json_str, 1, json_file_size, fp);
    if (bytesRead < json_file_size) {
        if (!feof(fp)) {
            fprintf(stderr, "Error: cannot read file %s\n", filename);
            fclose(fp);
            exit(1);
        }
    }
    fclose(fp);

    json_str[json_file_size] = '\0';

    return json_str;
}

// Function to print the properties array
void
parseProperties(cJSON *properties, MD_JSON_ARGS *md_json_args)
{
    cJSON *property = NULL;
    cJSON_ArrayForEach(property, properties)
    {
        cJSON *name  = cJSON_GetObjectItemCaseSensitive(property, "name");
        cJSON *value = cJSON_GetObjectItemCaseSensitive(property, "value");
        cJSON *class = cJSON_GetObjectItemCaseSensitive(property, "class");
        cJSON *type  = cJSON_GetObjectItemCaseSensitive(property, "type");

        md_json_processor->process_object_property(name, value, class, type, md_json_args);
    }
}

// Function to traverse and print the JSON structure
void
parseJSON(const char *jsonString)
{
    cJSON *json = cJSON_Parse(jsonString);
    if (json == NULL) {
        const char *error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL) {
            fprintf(stderr, "Error before: %s\n", error_ptr);
        }
        goto end;
    }

    initilize_md_json_processor();
    MD_JSON_ARGS *md_json_args = md_json_processor->init_processor();

    cJSON *dataset_name        = cJSON_GetObjectItemCaseSensitive(json, "dataset_name");
    cJSON *dataset_description = cJSON_GetObjectItemCaseSensitive(json, "dataset_description");
    cJSON *source_URL          = cJSON_GetObjectItemCaseSensitive(json, "source_URL");
    cJSON *collector           = cJSON_GetObjectItemCaseSensitive(json, "collector");
    cJSON *objects             = cJSON_GetObjectItemCaseSensitive(json, "objects");

    md_json_processor->process_json_header(dataset_name, dataset_description, source_URL, collector,
                                           md_json_args);

    cJSON *object = NULL;
    cJSON_ArrayForEach(object, objects)
    {
        cJSON *name       = cJSON_GetObjectItemCaseSensitive(object, "name");
        cJSON *type       = cJSON_GetObjectItemCaseSensitive(object, "type");
        cJSON *full_path  = cJSON_GetObjectItemCaseSensitive(object, "full_path");
        cJSON *properties = cJSON_GetObjectItemCaseSensitive(object, "properties");

        md_json_processor->process_object_base(name, type, full_path, md_json_args);
        parseProperties(properties, md_json_args);
    }
end:
    cJSON_Delete(json);
}

int
is_meta_json(const struct dirent *entry)
{
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        return 0;
    }
    if (entry->d_type == DT_DIR) {
        return 1;
    }
    if (endsWith(entry->d_name, ".json")) {
        return 1;
    }
    return 0;
}

int
scan_single_meta_json_file(char *full_filepath, void *args)
{
    meta_json_loader_args_t *mjargs = (meta_json_loader_args_t *)args;
    mjargs->current_file_count      = mjargs->current_file_count + 1;
    if (mjargs->current_file_count % mjargs->mpi_size != mjargs->mpi_rank) {
        return 0;
    }

    char *jsonString = read_json_file(full_filepath);
    if (jsonString == NULL) {
        return EXIT_FAILURE;
    }

    parseJSON(jsonString);
    free(jsonString);

    mjargs->processed_file_count += 1;
    return 0;
}

int
on_file(struct dirent *f_entry, const char *parent_path, void *arg)
{
    char *filepath = (char *)calloc(512, sizeof(char));
    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);
    scan_single_meta_json_file(filepath, arg);
    free(filepath);
    return 0;
}

int
on_dir(struct dirent *d_entry, const char *parent_path, void *arg)
{
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 0;
}

int
scan_files_in_dir(char *path, const int topk, void *args)
{
    collect_dir(path, is_meta_json, alphasort, ASC, topk, on_file, on_dir, args, NULL, NULL);
    return 0;
}

int
main(int argc, char **argv)
{
    int rst;
    int rank = 0, size = 1;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // check the current working directory
    char cwd[768];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        // printf("Current working dir: %s\n", cwd);
    }
    else {
        perror("getcwd() error");
        return 1;
    }

    if (argc != 3) {
        fprintf(stderr, "Usage: %s <json_file_dir> <num_files>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *INPUT_DIR = argv[1];
    int         topk      = atoi(argv[2]);
    char        full_filepath[1024];

    meta_json_loader_args_t *param = (meta_json_loader_args_t *)malloc(sizeof(meta_json_loader_args_t));
    param->current_file_count      = 0;
    param->processed_file_count    = 0;
    param->mpi_size                = size;
    param->mpi_rank                = rank;

    if (is_regular_file(INPUT_DIR)) {
        scan_single_meta_json_file((char *)INPUT_DIR, param);
        rst = 0;
    }
    else {
        rst = scan_files_in_dir((char *)INPUT_DIR, topk, param);
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    printf("Rank %d: Processed %d files\n", rank, param->processed_file_count);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return EXIT_SUCCESS;
}