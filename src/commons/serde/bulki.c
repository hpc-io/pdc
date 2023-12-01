#include "bulki.h"

size_t
get_BULKI_Entity_size(BULKI_Entity *bulk_entity)
{
    if (bulk_entity == NULL) {
        return 0;
    }
    size_t size = sizeof(int8_t) * 2 + sizeof(uint64_t) * 2;
    if (bulk_entity->pdc_class == PDC_CLS_ARRAY) {
        if (bulk_entity->pdc_type == PDC_BULKI) {
            BULKI *bulki_array = (BULKI *)bulk_entity->data;
            for (size_t i = 0; i < bulk_entity->count; i++) {
                size += get_BULKI_size(&bulki_array[i]);
            }
        }
        else if (bulk_entity->pdc_type == PDC_BULKI_ENT) {
            BULKI_Entity *bulki_entity_array = (BULKI_Entity *)bulk_entity->data;
            for (size_t i = 0; i < bulk_entity->count; i++) {
                size += get_BULKI_Entity_size(&bulki_entity_array[i]);
            }
        }
        else {
            size += get_size_by_class_n_type(bulk_entity->data, bulk_entity->count, bulk_entity->pdc_class,
                                             bulk_entity->pdc_type);
        }
    }
    else if (bulk_entity->pdc_class == PDC_CLS_ITEM) {
        if (bulk_entity->pdc_type == PDC_BULKI) {
            size += get_BULKI_size((BULKI *)bulk_entity->data);
        }
        else {
            size += get_size_by_class_n_type(bulk_entity->data, bulk_entity->count, bulk_entity->pdc_class,
                                             bulk_entity->pdc_type);
        }
    }
    bulk_entity->size = size;
    return size;
}

size_t
get_BULKI_size(BULKI *bulki)
{
    if (bulki == NULL) {
        return 0;
    }
    size_t size = sizeof(uint64_t) * 6; // totalSize + numKeys + headerSize + dataSize + offsets * 2;
    for (size_t i = 0; i < bulki->numKeys; i++) {
        size += get_BULKI_Entity_size(&bulki->header->keys[i]);
        size += get_BULKI_Entity_size(&bulki->data->values[i]);
    }
    bulki->totalSize = size;
    return size;
}

void
BULKI_Entity_print(BULKI_Entity *bulk_entity)
{
    if (bulk_entity == NULL) {
        printf("Error: bulki_entity is NULL\n");
        return;
    }
    printf("BULKI_Entity:\n");
    printf("pdc_class: %d\n", bulk_entity->pdc_class);
    printf("pdc_type: %d\n", bulk_entity->pdc_type);
    printf("count: %zu\n", bulk_entity->count);
    printf("size: %zu\n", bulk_entity->size);
    if (bulk_entity->pdc_class == PDC_CLS_ARRAY) {
        if (bulk_entity->pdc_type == PDC_BULKI) {
            BULKI *bulki_array = (BULKI *)bulk_entity->data;
            for (size_t i = 0; i < bulk_entity->count; i++) {
                printf("BULKI[%zu]:\n", i);
                BULKI_print(&bulki_array[i]);
            }
        }
        else if (bulk_entity->pdc_type == PDC_BULKI_ENT) {
            BULKI_Entity *bulki_entity_array = (BULKI_Entity *)bulk_entity->data;
            for (size_t i = 0; i < bulk_entity->count; i++) {
                printf("BULKI_Entity[%zu]:\n", i);
                BULKI_Entity_print(&bulki_entity_array[i]);
            }
        }
        else {
            printf("BULKI_Entity[%zu]:\n", bulk_entity->count);
            for (size_t i = 0; i < bulk_entity->count; i++) {
                printf("%s : ", DataTypeNames[bulk_entity->pdc_type]);
            }
            printf("\n");
        }
    }
    else if (bulk_entity->pdc_class == PDC_CLS_ITEM) {
        if (bulk_entity->pdc_type == PDC_BULKI) {
            printf("BULKI:\n");
            BULKI_print((BULKI *)bulk_entity->data);
        }
        else {
            printf("%s\n", DataTypeNames[bulk_entity->pdc_type]);
        }
    }
}

void
BULKI_print(BULKI *bulki)
{
    if (bulki == NULL) {
        printf("Error: bulki is NULL\n");
        return;
    }
    printf("BULKI:\n");
    printf("totalSize: %zu\n", bulki->totalSize);
    printf("numKeys: %zu\n", bulki->numKeys);
    printf("headerSize: %zu\n", bulki->header->headerSize);
    printf("dataSize: %zu\n", bulki->data->dataSize);
    for (size_t i = 0; i < bulki->numKeys; i++) {
        printf("key[%zu]:\n", i);
        BULKI_Entity_print(&bulki->header->keys[i]);
        printf("value[%zu]:\n", i);
        BULKI_Entity_print(&bulki->data->values[i]);
    }
}

BULKI_Entity *
empty_BULKI_Entity()
{
    BULKI_Entity *bulki_entity = (BULKI_Entity *)malloc(sizeof(BULKI_Entity));
    bulki_entity->pdc_type     = PDC_UNKNOWN;
    bulki_entity->pdc_class    = PDC_CLS_ITEM;
    bulki_entity->count        = 0;
    bulki_entity->data         = NULL;
    get_BULKI_Entity_size(bulki_entity);
    return bulki_entity;
}

BULKI_Entity *
BULKI_ENTITY_append_BULKI(BULKI_Entity *dest, BULKI *src)
{
    if (src == NULL || dest == NULL) {
        printf("Error: bulki is NULL\n");
        return NULL;
    }
    dest->pdc_type = PDC_BULKI;
    if (dest->count == 0) {
        dest->pdc_class = PDC_CLS_ITEM;
        dest->count     = 1;
        dest->data      = src;
    }
    else if (dest->count >= 1) {
        dest->pdc_class = PDC_CLS_ARRAY;
        dest->count     = dest->count + 1;
        dest->data      = realloc(dest->data, dest->count * sizeof(BULKI));
        memcpy(dest->data + (dest->count - 1) * sizeof(BULKI), src, sizeof(BULKI));
    }
    else {
        printf("Error: dest->count is %zu\n", dest->count);
        return NULL;
    }
    get_BULKI_Entity_size(dest);
    return dest;
}

BULKI_Entity *
BULKI_ENTITY_append_BULKI_Entity(BULKI_Entity *dest, BULKI_Entity *src)
{
    if (src == NULL || dest == NULL) {
        printf("Error: bulki is NULL\n");
        return NULL;
    }
    dest->pdc_type = PDC_BULKI_ENT;
    if (dest->count == 0) {
        dest->pdc_class = PDC_CLS_ITEM;
        dest->count     = 1;
        dest->data      = src;
    }
    else if (dest->count >= 1) {
        dest->pdc_class = PDC_CLS_ARRAY;
        dest->count     = dest->count + 1;
        dest->data      = realloc(dest->data, dest->count * sizeof(BULKI_Entity));
        memcpy(dest->data + (dest->count - 1) * sizeof(BULKI_Entity), src, sizeof(BULKI_Entity));
    }
    else {
        printf("Error: dest->count is %zu\n", dest->count);
        return NULL;
    }
    get_BULKI_Entity_size(dest);
    return dest;
}

BULKI_Entity *
BULKI_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class)
{
    if (pdc_type == PDC_BULKI_ENT && pdc_class == PDC_CLS_ITEM) {
        printf("Error: BULKI_Entity cannot be an single item in another BULKI_Entity\n");
        return NULL;
    }
    BULKI_Entity *bulki_entity = (BULKI_Entity *)malloc(sizeof(BULKI_Entity));
    bulki_entity->pdc_type     = pdc_type;
    bulki_entity->pdc_class    = pdc_class;
    bulki_entity->count        = (pdc_class == PDC_CLS_ITEM) ? 1 : count;
    size_t size                = get_size_by_class_n_type(data, count, pdc_class, pdc_type);
    bulki_entity->data         = malloc(size);
    memcpy(bulki_entity->data, data, size);
    get_BULKI_Entity_size(bulki_entity);
    return bulki_entity;
}

BULKI *
BULKI_init(int initial_field_count)
{
    BULKI *buiki              = calloc(1, sizeof(BULKI));
    buiki->numKeys            = 0;
    buiki->capacity           = initial_field_count;
    buiki->header             = calloc(1, sizeof(BULKI_Header));
    buiki->header->keys       = calloc(buiki->capacity, sizeof(BULKI_Entity));
    buiki->header->headerSize = 0;
    buiki->data               = calloc(1, sizeof(BULKI_Data));
    buiki->data->values       = calloc(buiki->capacity, sizeof(BULKI_Entity));
    buiki->data->dataSize     = 0;
    get_BULKI_size(buiki);
    return buiki;
}

int
BULKI_Entity_equal(BULKI_Entity *be1, BULKI_Entity *be2)
{
    return be1->pdc_type == be2->pdc_type && be1->pdc_class == be2->pdc_class && be1->count == be2->count &&
           be1->size == be2->size && be1->data == be2->data;
}

void
BULKI_add(BULKI *bulki, BULKI_Entity *key, BULKI_Entity *value)
{
    if (bulki == NULL || key == NULL || value == NULL) {
        printf("Error: bulki, key, or value is NULL\n");
        return;
    }
    if (bulki->numKeys >= bulki->capacity) {
        bulki->capacity *= 2;
        bulki->header->keys = realloc(bulki->header->keys, bulki->capacity * sizeof(BULKI_Entity));
        bulki->data->values = realloc(bulki->data->values, bulki->capacity * sizeof(BULKI_Entity));
    }
    memcpy(&bulki->header->keys[bulki->numKeys], key, sizeof(BULKI_Entity));
    // append bytes for type, size, and key
    bulki->header->headerSize += key->size;

    memcpy(&bulki->data->values[bulki->numKeys], value, sizeof(BULKI_Entity));
    // append bytes for class, type, size, and data
    bulki->data->dataSize += value->size;

    bulki->numKeys++;
    get_BULKI_size(bulki);
}

BULKI_Entity *
BULKI_delete(BULKI *bulki, BULKI_Entity *key)
{
    BULKI_Entity *value = NULL;
    for (size_t i = 0; i < bulki->numKeys; i++) {
        if (BULKI_Entity_equal(&bulki->header->keys[i], key)) {
            value = &bulki->data->values[i];
            bulki->header->headerSize -= key->size;
            bulki->data->dataSize -= value->size;
            bulki->numKeys--;
            memcpy(&bulki->header->keys[i], &bulki->header->keys[bulki->numKeys - 1], sizeof(BULKI_Entity));
            memcpy(&bulki->data->values[i], &bulki->data->values[bulki->numKeys - 1], sizeof(BULKI_Entity));
            break;
        }
    }
    get_BULKI_size(bulki);
    return value;
}

void
BULKI_Entity_free(BULKI_Entity *bulk_entity, int free_struct)
{
    if (bulk_entity != NULL) {
        if (bulk_entity->pdc_class == PDC_CLS_ARRAY) {
            if (bulk_entity->pdc_type == PDC_BULKI) {
                BULKI *bulki_array = (BULKI *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    BULKI_free(&bulki_array[i], 0);
                }
            }
            else if (bulk_entity->pdc_type == PDC_BULKI_ENT) {
                BULKI_Entity *bulki_entity_array = (BULKI_Entity *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    BULKI_Entity_free(&bulki_entity_array[i], 0);
                }
            }
        }
        else if (bulk_entity->pdc_class == PDC_CLS_ITEM) {
            if (bulk_entity->pdc_type == PDC_BULKI) {
                BULKI_free((BULKI *)bulk_entity->data, 0);
            }
        }
        free(bulk_entity->data);
        if (free_struct) {
            free(bulk_entity);
        }
    }
} // BULKI_Entity_free

void
BULKI_free(BULKI *bulki, int free_struct)
{
    if (bulki != NULL) {
        if (bulki->header != NULL) {
            if (bulki->header->keys != NULL) {
                for (size_t i = 0; i < bulki->numKeys; i++) {
                    BULKI_Entity_free(&bulki->header->keys[i], 0);
                }
                free(bulki->header->keys);
            }
            free(bulki->header);
        }
        if (bulki->data != NULL) {
            if (bulki->data->values != NULL) {
                for (size_t i = 0; i < bulki->numKeys; i++) {
                    BULKI_Entity_free(&bulki->data->values[i], 0);
                }
                free(bulki->data->values);
            }
            free(bulki->data);
        }
        if (free_struct) {
            free(bulki);
        }
    }
} // BULKI_free