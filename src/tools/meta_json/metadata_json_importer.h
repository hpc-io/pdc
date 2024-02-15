#ifndef METADATA_JSON_IMPORTER_H
#define METADATA_JSON_IMPORTER_H
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "cjson/cJSON.h"
#include "metadata_json_processor.h"

// Your code here

/**
 * @brief init_importer
 */
MD_JSON_ARGS *init_importer();

/**
 * @brief import_json_header
 * @param dataset_name
 * @param dataset_description
 * @param source_URL
 * @param collector
 * @param md_json_args -> here we should have the object ID stored in md_json_args
 * @return 0 if success, -1 if error
 */
int import_json_header(cJSON *dataset_name, cJSON *dataset_description, cJSON *source_URL, cJSON *collector,
                       MD_JSON_ARGS *md_json_args);

/**
 * @brief import_object_base
 * @param name
 * @param type
 * @param full_path
 * @param md_json_args -> here we should have the object ID in md_json_args already
 * @return 0 if success, -1 if error
 */
int import_object_base(cJSON *name, cJSON *type, cJSON *full_path, MD_JSON_ARGS *md_json_args);

/**
 * @brief import_object_property
 * @param name
 * @param type
 * @param cls
 * @param value
 * @param md_json_args -> here we should have the object ID in md_json_args already
 * @return 0 if success, -1 if error
 *
 */
int import_object_property(cJSON *name, cJSON *type, cJSON *cls, cJSON *value, MD_JSON_ARGS *md_json_args);

/**
 * @brief print_object_property_array
 * @param md_json_args
 * @return 0 if success, -1 if error
 */
int complete_one_json_file(MD_JSON_ARGS *md_json_args);

/**
 * @brief finalize_importer
 * @param md_json_args
 * @return 0 if success, -1 if error
 */
int finalize_importer(MD_JSON_ARGS *md_json_args);

/**
 * @brief create_md_json_importer
 * @return
 */
MD_JSON_PROCESSOR *create_md_json_importer();

#endif // METADATA_JSON_IMPORTER_H
