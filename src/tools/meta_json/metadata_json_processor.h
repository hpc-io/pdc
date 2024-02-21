#ifndef METADATA_JSON_PROCESSOR_H
#define METADATA_JSON_PROCESSOR_H

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include "cjson/cJSON.h"

typedef struct {
    void *   processor_args; // pdc_importer_args_t
    void *   loader_args;    // md_json_loader_args_t
    void *   arg1;           // unused
    void *   arg2;           // unused
    uint64_t total_obj_count;
    uint64_t total_prop_count;
} MD_JSON_ARGS;

typedef struct {

    int (*init_processor)(MD_JSON_ARGS *);

    int (*process_json_header)(cJSON *, cJSON *, cJSON *, cJSON *, MD_JSON_ARGS *);

    int (*process_object_base)(cJSON *, cJSON *, cJSON *, MD_JSON_ARGS *);

    int (*process_object_property)(cJSON *, cJSON *, cJSON *, cJSON *, MD_JSON_ARGS *);

    int (*complete_one_json_file)(MD_JSON_ARGS *);

    int (*finalize_processor)(MD_JSON_ARGS *);

} MD_JSON_PROCESSOR;

#endif // METADATA_JSON_PROCESSOR_H