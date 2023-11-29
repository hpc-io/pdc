#include "bulki.h"

size_t
get_BULKI_size(void *data, size_t count, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class)
{
    size_t bulki_size = 0;
    if (pdc_class != PDC_CLS_ITEM && pdc_class != PDC_CLS_ARRAY) {
        printf("Error: unsupported class %d\n", pdc_class);
        return 0;
    }
    if (count == 0) {
        printf("Error: array count cannot be 0\n");
        return 0;
    }

    if (pdc_class == PDC_CLS_ARRAY) {
        if (pdc_type == PDC_BULKI) { // BULKI array
            BULKI *bulki_array = (BULKI *)data;
            for (size_t i = 0; i < count; i++) {
                bulki_size = bulki_size + bulki_array[i].totalSize;
            }
        }
        else if (pdc_type == PDC_BULKI_ENT) { // BULKI_Entity array
            BULKI_Entity *bulki_entity_array = (BULKI_Entity *)data;
            for (size_t i = 0; i < count; i++) {
                bulki_size =
                    bulki_size + get_BULKI_size((bulki_entity_array[i]).data, (bulki_entity_array[i]).count,
                                                (bulki_entity_array[i]).pdc_type,
                                                (bulki_entity_array[i]).pdc_class);
            }
        }
        else { // all base types
            bulki_size = get_size_by_class_n_type(data, count, pdc_class, pdc_type);
        }
    }
    else if (pdc_class == PDC_CLS_ITEM) {
        if (pdc_type == PDC_BULKI) { // BULKI
            bulki_size = ((BULKI *)data)->totalSize;
        }
        else if (pdc_type == PDC_BULKI_ENT) { // BULKI_Entity
            bulki_size = get_BULKI_size(((BULKI_Entity *)data)->data, ((BULKI_Entity *)data)->count,
                                        ((BULKI_Entity *)data)->pdc_type, ((BULKI_Entity *)data)->pdc_class);
        }
        else { // all base types
            bulki_size = get_size_by_class_n_type(data, count, pdc_class, pdc_type);
        }
    }
    return bulki_size;
}

void
BULKI_print(void *data, size_t count, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class)
{
    if (pdc_class != PDC_CLS_ITEM && pdc_class != PDC_CLS_ARRAY) {
        printf("Error: unsupported class %d\n", pdc_class);
        return;
    }
    if (count == 0) {
        printf("Error: array count cannot be 0\n");
        return;
    }

    if (pdc_class == PDC_CLS_ARRAY) {
        if (pdc_type == PDC_BULKI) { // BULKI array
            BULKI *bulki_array = (BULKI *)data;
            for (size_t i = 0; i < count; i++) {
                printf("BULKI[%zu]:\n", i);
                BULKI_print(bulki_array[i].data, bulki_array[i].numKeys, PDC_BULKI_ENT, PDC_CLS_ARRAY);
            }
        }
        else if (pdc_type == PDC_BULKI_ENT) { // BULKI_Entity array
            BULKI_Entity *bulki_entity_array = (BULKI_Entity *)data;
            for (size_t i = 0; i < count; i++) {
                printf("BULKI_Entity[%zu]:\n", i);
                BULKI_print(bulki_entity_array[i].data, bulki_entity_array[i].count,
                            bulki_entity_array[i].pdc_type, bulki_entity_array[i].pdc_class);
            }
        }
        else { // all base types
            printf("BULKI_Entity[%zu]:\n", count);
            for (size_t i = 0; i < count; i++) {
                printf("%s ", DataTypeNames[pdc_type]);
            }
            printf("\n");
        }
    }
    else if (pdc_class == PDC_CLS_ITEM) {
        if (pdc_type == PDC_BULKI) { // BULKI
            printf("BULKI:\n");
            for (size_t i = 0; i < ((BULKI *)data)->numKeys; i++) {
                printf("key[%zu]:\n", i);
                BULKI_print(((BULKI *)data)->header->keys[i].data, ((BULKI *)data)->header->keys[i].count,
                            ((BULKI *)data)->header->keys[i].pdc_type,
                            ((BULKI *)data)->header->keys[i].pdc_class);
                BULKI_print(((BULKI *)data)->data->values[i].data, ((BULKI *)data)->data->values[i].count,
                            ((BULKI *)data)->data->values[i].pdc_type,
                            ((BULKI *)data)->data->values[i].pdc_class);
            }
        }
        else if (pdc_type == PDC_BULKI_ENT) { // BULKI_Entity
            printf("BULKI_Entity:\n");
            BULKI_print(((BULKI_Entity *)data)->data, ((BULKI_Entity *)data)->count,
                        ((BULKI_Entity *)data)->pdc_type, ((BULKI_Entity *)data)->pdc_class);
        } // all base types
        else {
            printf("%s\n", DataTypeNames[pdc_type]);
        }
    }
}

BULKI_Entity *
BULKI_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class)
{
    BULKI_Entity *bulki_entity = (BULKI_Entity *)malloc(sizeof(BULKI_Entity));
    bulki_entity->pdc_type     = pdc_type;
    bulki_entity->pdc_class    = pdc_class;
    bulki_entity->count        = (pdc_class == PDC_CLS_ITEM) ? 1 : count;
    bulki_entity->data         = data;
    bulki_entity->size         = get_BULKI_size(data, count, pdc_type, pdc_class);
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
    bulki->header->keys[bulki->numKeys] = *key;
    // append bytes for type, size, and key
    bulki->header->headerSize += (sizeof(uint8_t) + sizeof(uint64_t) + key->size);

    bulki->data->values[bulki->numKeys] = *value;
    // append bytes for class, type, size, and data
    bulki->data->dataSize += (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint64_t) + value->size);

    bulki->numKeys++;
    if (bulki->numKeys >= bulki->capacity) {
        bulki->capacity *= 2;
        bulki->header->keys = realloc(bulki->header->keys, bulki->capacity * sizeof(BULKI_Entity));
        bulki->data->values = realloc(bulki->data->values, bulki->capacity * sizeof(BULKI_Entity));
    }
    bulki->totalSize = bulki->header->headerSize + bulki->data->dataSize + sizeof(uint64_t) * 6;
}

BULKI_Entity *
BULKI_delete(BULKI *bulki, BULKI_Entity *key)
{
    BULKI_Entity *value = NULL;
    for (size_t i = 0; i < bulki->numKeys; i++) {
        if (BULKI_Entity_equal(&bulki->header->keys[i], key)) {
            value = &bulki->data->values[i];
            bulki->header->headerSize -= (sizeof(uint8_t) + sizeof(uint64_t) + key->size);
            bulki->data->dataSize -= (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint64_t) + value->size);
            bulki->numKeys--;
            bulki->header->keys[i] = bulki->header->keys[bulki->numKeys - 1];
            bulki->data->values[i] = bulki->data->values[bulki->numKeys - 1];
            break;
        }
    }
    return value;
}

void
free_BULKI_Entity(BULKI_Entity *bulk_entity)
{
    if (bulk_entity != NULL) {
        if (bulk_entity->pdc_class == PDC_CLS_ARRAY) {
            if (bulk_entity->pdc_type == PDC_BULKI) {
                BULKI *bulki_array = (BULKI *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    free_BULKI(&bulki_array[i]);
                }
            }
            else if (bulk_entity->pdc_type == PDC_BULKI_ENT) {
                BULKI_Entity *bulki_entity_array = (BULKI_Entity *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    free_BULKI_Entity(&bulki_entity_array[i]);
                }
            }
            else {
                free(bulk_entity->data);
            }
        }
        else if (bulk_entity->pdc_class == PDC_CLS_ITEM) {
            if (bulk_entity->pdc_type == PDC_BULKI) {
                free_BULKI((BULKI *)bulk_entity->data);
            }
            else if (bulk_entity->pdc_type == PDC_BULKI_ENT) {
                free_BULKI_Entity((BULKI_Entity *)bulk_entity->data);
            }
            else {
                free(bulk_entity->data);
            }
        }
        free(bulk_entity);
    }
} // free_BULKI_Entity

void
free_BULKI(BULKI *bulki)
{
    if (bulki != NULL) {
        if (bulki->header != NULL) {
            if (bulki->header->keys != NULL) {
                for (size_t i = 0; i < bulki->numKeys; i++) {
                    free_BULKI_Entity(&bulki->header->keys[i]);
                }
                free(bulki->header->keys);
            }
            free(bulki->header);
        }
        if (bulki->data != NULL) {
            if (bulki->data->values != NULL) {
                for (size_t i = 0; i < bulki->numKeys; i++) {
                    free_BULKI_Entity(&bulki->data->values[i]);
                }
                free(bulki->data->values);
            }
            free(bulki->data);
        }
        free(bulki);
    }
} // free_BULKI