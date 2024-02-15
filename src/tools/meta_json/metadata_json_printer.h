#ifndef METADATA_JSON_PRINTER_H
#define METADATA_JSON_PRINTER_H

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "cjson/cJSON.h"
#include "metadata_json_processor.h"

/**
 * @brief init_printer
 */
MD_JSON_ARGS *init_printer();

/**
 * @brief print_json_header
 * @param dataset_name
 * @param dataset_description
 * @param source_URL
 * @param collector
 * @param md_json_args
 * @return 0 if success, -1 if error
 */
int print_json_header(cJSON *dataset_name, cJSON *dataset_description, cJSON *source_URL, cJSON *collector,
                      MD_JSON_ARGS *md_json_args);

/**
 * @brief print_object_base
 * @param name
 * @param type
 * @param full_path
 * @return 0 if success, -1 if error
 */
int print_object_base(cJSON *name, cJSON *type, cJSON *full_path, MD_JSON_ARGS *md_json_args);

/**
 * @brief print_object_property
 * @param name
 * @param type
 * @param cls
 * @param value
 * @param md_json_args
 * @return 0 if success, -1 if error
 *
 */
int print_object_property(cJSON *name, cJSON *type, cJSON *cls, cJSON *value, MD_JSON_ARGS *md_json_args);

/**
 * @brief print_object_property_array
 * @param md_json_args
 * @return 0 if success, -1 if error
 */
int complete_one_json_file(MD_JSON_ARGS *md_json_args);

/**
 * @brief finalize_printer
 * @param md_json_args
 * @return 0 if success, -1 if error
 */
int finalize_printer(MD_JSON_ARGS *md_json_args);

/**
 * @brief create_md_json_printer
 * @return
 */
MD_JSON_PROCESSOR *create_md_json_printer();

#endif // METADATA_JSON_PRINTER_H
