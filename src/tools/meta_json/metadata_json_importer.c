#include "metadata_json_importer.h"
#include "pdc.h"

typedef struct {
    pdcid_t obj_prop;
    pdcid_t pdc;
    pdcid_t cont_prop;
    pdcid_t cont;
    pdcid_t obj_id;
} pdc_importer_args_t;

/**
 * @brief init_importer
 */
MD_JSON_ARGS *
init_importer()
{
    pdc_importer_args_t *pdc_args = (pdc_importer_args_t *)malloc(sizeof(pdc_importer_args_t));

    MD_JSON_ARGS *md_json_args = (MD_JSON_ARGS *)malloc(sizeof(MD_JSON_ARGS));
    // initilize PDC related things, and store in the above structure.
    // create a pdc
    pdc_args->pdc = PDCinit("pdc");

    md_json_args->arg1 = pdc_args;

    return md_json_args;
}

/**
 * @brief import_json_header
 * @param dataset_name
 * @param dataset_description
 * @param source_URL
 * @param collector
 * @param md_json_args -> here we should have the object ID stored in md_json_args
 * @return 0 if success, -1 if error
 */
int
import_json_header(cJSON *dataset_name, cJSON *dataset_description, cJSON *source_URL, cJSON *collector,
                   MD_JSON_ARGS *md_json_args)
{
    // create object in PDC and store the object ID in md_json_args
    pdc_importer_args_t *pdc_args = (pdc_importer_args_t *)md_json_args->arg1;

    // create a container property
    pdc_args->cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_args->pdc);
    if (pdc_args->cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    pdc_args->cont = PDCcont_create(dataset_name->valuestring, pdc_args->cont_prop);
    if (pdc_args->cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // add tags to the container, based on the additional attributes in the JSON
    PDCcont_put_tag(pdc_args->cont, "dataset_description", (void *)dataset_description->valuestring,
                    PDC_STRING, strlen(dataset_description->valuestring) + 1);
    PDCcont_put_tag(pdc_args->cont, "source_URL", (void *)source_URL->valuestring, PDC_STRING,
                    strlen(source_URL->valuestring) + 1);
    PDCcont_put_tag(pdc_args->cont, "collector", (void *)collector->valuestring, PDC_STRING,
                    strlen(collector->valuestring) + 1);

    // create an object property
    pdc_args->obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_args->pdc);
    if (pdc_args->obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    return 0;
}

/**
 * @brief import_object_base
 * @param name
 * @param type
 * @param full_path
 * @param md_json_args -> here we should have the object ID in md_json_args already
 * @return 0 if success, -1 if error
 */
int
import_object_base(cJSON *name, cJSON *type, cJSON *full_path, MD_JSON_ARGS *md_json_args)
{
    pdc_importer_args_t *pdc_args = (pdc_importer_args_t *)md_json_args->arg1;

    // create object in PDC and store the object ID in md_json_args
    pdc_args->obj_id = PDCobj_create(pdc_args->cont, name->valuestring, pdc_args->obj_prop);

    if (PDCobj_put_tag(pdc_args->obj_id, "obj_full_path", (void *)full_path->valuestring, PDC_STRING,
                       strlen(full_path->valuestring) + 1) != SUCCEED) {
        printf("Fail to put tag!\n");
    }
    if (PDCobj_put_tag(pdc_args->obj_id, "obj_type", (void *)type->valuestring, PDC_STRING,
                       strlen(type->valuestring) + 1) != SUCCEED) {
        printf("Fail to put tag!\n");
    }

    return 0;
}

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
int
import_object_property(cJSON *name, cJSON *type, cJSON *cls, cJSON *value, MD_JSON_ARGS *md_json_args)
{
    pdc_importer_args_t *pdc_args = (pdc_importer_args_t *)md_json_args->arg1;

    // create object in PDC and store the object ID in md_json_args

    // bypass cls=array for now (singletone, array, etc.), just consider singleton
    if (strcmp(cls->valuestring, "singleton") != 0) {
        goto end;
    }
    pdc_var_type_t pdc_type            = PDC_UNKNOWN;
    void *         property_value      = NULL;
    size_t         property_value_size = 0;
    if (strcmp(type->valuestring, "int") == 0) {
        pdc_type            = PDC_INT;
        int64_t pval        = (int64_t)cJSON_GetNumberValue(value);
        property_value      = &pval;
        property_value_size = sizeof(int64_t);
    }
    else if (strcmp(type->valuestring, "float") == 0) {
        pdc_type            = PDC_FLOAT;
        double pval         = cJSON_GetNumberValue(value);
        property_value      = &pval;
        property_value_size = sizeof(double);
    }
    else if (strcmp(type->valuestring, "string") == 0) {
        pdc_type   = PDC_STRING;
        char *pval = cJSON_GetStringValue(value);
        if (pval != NULL) {
            property_value      = pval;
            property_value_size = strlen(pval) + 1;
        }
    }
    else {
        printf("Unknown type!\n");
        goto end;
    }

    if (PDCobj_put_tag(pdc_args->obj_id, name->valuestring, (void *)&property_value, pdc_type,
                       strlen(value->valuestring) + 1) != SUCCEED) {
        printf("Fail to add tag!\n");
    }

end:
    return 0;
}

/**
 * @brief finalize_importer
 * @param md_json_args
 * @return 0 if success, -1 if error
 */
int
finalize_importer(MD_JSON_ARGS *md_json_args)
{
    pdc_importer_args_t *pdc_args = (pdc_importer_args_t *)md_json_args->arg1;
    // finalize PDC related things
    // close a container
    if (PDCcont_close(pdc_args->cont) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(pdc_args->obj_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCprop_close(pdc_args->cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    // close pdc
    if (PDCclose(pdc_args->pdc) < 0)
        printf("fail to close PDC\n");

    return 0;
}

/**
 * @brief create_md_json_importer
 * @return
 */
MD_JSON_PROCESSOR *
create_md_json_importer()
{
    MD_JSON_PROCESSOR *md_json_processor       = (MD_JSON_PROCESSOR *)malloc(sizeof(MD_JSON_PROCESSOR));
    md_json_processor->init_processor          = init_importer;
    md_json_processor->process_json_header     = import_json_header;
    md_json_processor->process_object_base     = import_object_base;
    md_json_processor->process_object_property = import_object_property;
    md_json_processor->finalize_processor      = finalize_importer;
    return md_json_processor;
}
