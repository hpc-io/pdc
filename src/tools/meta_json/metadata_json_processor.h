#ifndef METADATA_JSON_PROCESSOR_H
#define METADATA_JSON_PROCESSOR_H

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "cjson/cJSON.h"

typedef struct {
    void *arg1;
    void *arg2;
    void *arg3;
    void *arg4;
} MD_JSON_ARGS;

typedef struct {

    MD_JSON_ARGS *(*init_processor)();

    int (*process_json_header)(cJSON *, cJSON *, cJSON *, cJSON *, MD_JSON_ARGS *);

    int (*process_object_base)(cJSON *, cJSON *, cJSON *, MD_JSON_ARGS *);

    int (*process_object_property)(cJSON *, cJSON *, cJSON *, cJSON *, MD_JSON_ARGS *);

    int (*finalize_processor)(MD_JSON_ARGS *);

} MD_JSON_PROCESSOR;

#endif // METADATA_JSON_PROCESSOR_H