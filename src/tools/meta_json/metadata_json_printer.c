#include "metadata_json_printer.h"
#include "pdc_logger.h"

int
init_printer(MD_JSON_ARGS *md_json_args)
{
    return EXIT_SUCCESS;
}

int
print_json_header(cJSON *dataset_name, cJSON *dataset_description, cJSON *source_URL, cJSON *collector,
                  MD_JSON_ARGS *md_json_args)
{
    LOG_JUST_PRINT("Dataset Name: %s\n", dataset_name->valuestring);
    LOG_JUST_PRINT("Dataset Description: %s\n", dataset_description->valuestring);
    LOG_JUST_PRINT("Source URL: %s\n", source_URL->valuestring);
    LOG_JUST_PRINT("Collector: %s\n\n", collector->valuestring);
    return 0;
}

int
print_object_base(cJSON *name, cJSON *type, cJSON *full_path, MD_JSON_ARGS *md_json_args)
{
    LOG_JUST_PRINT("  Object Name: %s\n", name->valuestring);
    LOG_JUST_PRINT("  Object Type: %s\n", type->valuestring);
    LOG_JUST_PRINT("  Object Full Path: %s\n\n", full_path->valuestring);
    return 0;
}

int
print_object_property(cJSON *name, cJSON *type, cJSON *cls, cJSON *value, MD_JSON_ARGS *md_json_args)
{
    LOG_JUST_PRINT("    Property Name: %s\n", name->valuestring);
    if (cJSON_IsString(value)) {
        LOG_JUST_PRINT("    Property Value: %s\n", value->valuestring);
    }
    else if (cJSON_IsNumber(value)) {
        LOG_JUST_PRINT("    Property Value: %f\n", value->valuedouble);
    }
    LOG_JUST_PRINT("    Property Class: %s\n", cls->valuestring);
    LOG_JUST_PRINT("    Property Type: %s\n\n", type->valuestring);
    return 0;
}

int
done_printing_one_json(MD_JSON_ARGS *md_json_args)
{
    return 0;
}

int
finalize_printer(MD_JSON_ARGS *md_json_args)
{
    return 0;
}

MD_JSON_PROCESSOR *
create_md_json_printer()
{
    MD_JSON_PROCESSOR *md_json_processor       = (MD_JSON_PROCESSOR *)malloc(sizeof(MD_JSON_PROCESSOR));
    md_json_processor->init_processor          = init_printer;
    md_json_processor->process_json_header     = print_json_header;
    md_json_processor->process_object_base     = print_object_base;
    md_json_processor->process_object_property = print_object_property;
    md_json_processor->complete_one_json_file  = done_printing_one_json;
    md_json_processor->finalize_processor      = finalize_printer;
    return md_json_processor;
}
