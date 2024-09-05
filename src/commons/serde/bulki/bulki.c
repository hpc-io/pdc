#include "bulki.h"
#include "bulki_vle_util.h"

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
        if (bulk_entity->pdc_type == PDC_BULKI_ENT) {
            size += get_BULKI_Entity_size((BULKI_Entity *)bulk_entity->data);
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
    size_t size = sizeof(uint64_t) * 6; // totalSize, numKeys, headerSize, dataSize, headerOffset, dataOffset

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
empty_BULKI_Array_Entity()
{
    BULKI_Entity *bulki_entity = (BULKI_Entity *)calloc(1, sizeof(BULKI_Entity));
    bulki_entity->pdc_type     = PDC_BULKI;
    bulki_entity->pdc_class    = PDC_CLS_ARRAY;
    bulki_entity->count        = 0;
    bulki_entity->data         = NULL;
    get_BULKI_Entity_size(bulki_entity);
    return bulki_entity;
}

BULKI_Entity *
empty_Bent_Array_Entity()
{
    BULKI_Entity *bulki_entity = (BULKI_Entity *)calloc(1, sizeof(BULKI_Entity));
    bulki_entity->pdc_type     = PDC_BULKI_ENT;
    bulki_entity->pdc_class    = PDC_CLS_ARRAY;
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
    if (dest->pdc_class != PDC_CLS_ARRAY || dest->pdc_type != PDC_BULKI) {
        printf("Error: dest is not an array of BULKI structure\n");
        return NULL;
    }
    dest->count = dest->count + 1;
    dest->data  = realloc(dest->data, dest->count * sizeof(BULKI));
    memcpy(dest->data + (dest->count - 1) * sizeof(BULKI), src, sizeof(BULKI));
    get_BULKI_Entity_size(dest);
    return dest;
}

BULKI *
BULKI_ENTITY_get_BULKI(BULKI_Entity *bulki_entity, size_t idx)
{
    if (bulki_entity == NULL) {
        printf("Error: bulki_entity is NULL\n");
        return NULL;
    }
    if (bulki_entity->pdc_class != PDC_CLS_ARRAY || bulki_entity->pdc_type != PDC_BULKI) {
        printf("Error: bulki_entity is not an array of BULKI structure\n");
        return NULL;
    }
    if (idx >= bulki_entity->count) {
        printf("idx = %d, count = %d Warning: index for bulki_entity is out of bound\n", idx,
               bulki_entity->count);
        return NULL;
    }
    return &((BULKI *)bulki_entity->data)[idx];
}

BULKI_Entity *
BULKI_ENTITY_append_BULKI_Entity(BULKI_Entity *dest, BULKI_Entity *src)
{
    if (src == NULL || dest == NULL) {
        printf("Error: bulki is NULL\n");
        return NULL;
    }
    if (dest->pdc_class != PDC_CLS_ARRAY || dest->pdc_type != PDC_BULKI_ENT) {
        printf("Error: dest is not an array of BULKI_Entity structure\n");
        return NULL;
    }
    dest->count = dest->count + 1;
    dest->data  = realloc(dest->data, dest->count * sizeof(BULKI_Entity));
    memcpy(dest->data + (dest->count - 1) * sizeof(BULKI_Entity), src, sizeof(BULKI_Entity));
    get_BULKI_Entity_size(dest);
    return dest;
}

BULKI_Entity *
BULKI_ENTITY_get_BULKI_Entity(BULKI_Entity *bulki_entity, size_t idx)
{
    if (bulki_entity == NULL) {
        printf("Error: bulki_entity is NULL\n");
        return NULL;
    }
    if (bulki_entity->pdc_class != PDC_CLS_ARRAY || bulki_entity->pdc_type != PDC_BULKI_ENT) {
        printf("Error: bulki_entity is not an array of BULKI_Entity structure\n");
        return NULL;
    }
    if (idx >= bulki_entity->count) {
        // printf("idx = %d, count = %d Warning: index for bulki_entity is out of bound\n", idx,
        // bulki_entity->count);
        return NULL;
    }
    return &((BULKI_Entity *)bulki_entity->data)[idx];
}

BULKI_Entity *
BULKI_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class)
{
    if (pdc_type == PDC_BULKI_ENT && pdc_class == PDC_CLS_ITEM) {
        printf("Error: BULKI_Entity cannot be an single item in another BULKI_Entity\n");
        return NULL;
    }
    BULKI_Entity *bulki_entity = (BULKI_Entity *)calloc(1, sizeof(BULKI_Entity));
    bulki_entity->pdc_type     = pdc_type;
    bulki_entity->pdc_class    = pdc_class;
    bulki_entity->count        = (pdc_class == PDC_CLS_ITEM) ? 1 : count;
    size_t size                = get_size_by_class_n_type(data, count, pdc_class, pdc_type);
    if (pdc_type == PDC_BULKI) {
        size               = sizeof(BULKI) * bulki_entity->count;
        bulki_entity->data = data;
    }
    else if (pdc_type == PDC_BULKI_ENT) {
        size               = sizeof(BULKI_Entity) * bulki_entity->count;
        bulki_entity->data = data;
    }
    else {
        bulki_entity->data = calloc(1, size);
        memcpy(bulki_entity->data, data, size);
    }

    get_BULKI_Entity_size(bulki_entity);
    return bulki_entity;
}

BULKI_Entity *
BULKI_singleton_ENTITY(void *data, pdc_c_var_type_t pdc_type)
{
    return BULKI_ENTITY(data, 1, pdc_type, PDC_CLS_ITEM);
}

BULKI_Entity *
BULKI_array_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type)
{
    return BULKI_ENTITY(data, count, pdc_type, PDC_CLS_ARRAY);
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
    int meta_equal = be1->pdc_type == be2->pdc_type && be1->pdc_class == be2->pdc_class &&
                     be1->count == be2->count && be1->size == be2->size;
    if (!meta_equal) {
        // printf("Error: be1 and be2 are not equal in terms of metadata\n");
        return 0;
    }
    if (be1->pdc_class == PDC_CLS_ARRAY) {
        if (be1->pdc_type == PDC_BULKI) {
            BULKI *bulki_array1 = (BULKI *)be1->data;
            BULKI *bulki_array2 = (BULKI *)be2->data;
            for (size_t i = 0; i < be1->count; i++) {
                if (!BULKI_equal(&bulki_array1[i], &bulki_array2[i])) {
                    // printf("Error: be1 and be2 are not equal in terms of BULKI data in the array\n");
                    return 0;
                }
            }
        }
        else if (be1->pdc_type == PDC_BULKI_ENT) {
            BULKI_Entity *bulki_entity_array1 = (BULKI_Entity *)be1->data;
            BULKI_Entity *bulki_entity_array2 = (BULKI_Entity *)be2->data;
            for (size_t i = 0; i < be1->count; i++) {
                if (!BULKI_Entity_equal(&bulki_entity_array1[i], &bulki_entity_array2[i])) {
                    // printf("Error: be1 and be2 are not equal in terms of BULKI_Entity data in the
                    // array\n");
                    return 0;
                }
            }
        }
        else {
            if (memcmp(be1->data, be2->data, be1->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2) != 0) {
                // printf("Error: be1 and be2 are not equal in terms of base type data in the array\n");
                return 0;
            }
        }
    }
    else if (be1->pdc_class == PDC_CLS_ITEM) {
        if (be1->pdc_type == PDC_BULKI) {
            if (!BULKI_equal((BULKI *)be1->data, (BULKI *)be2->data)) {
                // printf("Error: be1 and be2 are not equal in terms of BULKI data\n");
                return 0;
            }
        }
        else {
            if (memcmp(be1->data, be2->data, be1->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2) != 0) {
                // printf("Error: be1 and be2 are not equal in terms of base type data\n");
                return 0;
            }
        }
    }
    return 1;
}

int
BULKI_equal(BULKI *bulki1, BULKI *bulki2)
{
    if (bulki1->numKeys != bulki2->numKeys || bulki1->totalSize != bulki2->totalSize ||
        bulki1->header->headerSize != bulki2->header->headerSize ||
        bulki1->data->dataSize != bulki2->data->dataSize) {
        printf("Error: bulki1 and bulki2 are not equal in terms of metadata\n");
        return 0;
    }
    for (size_t i = 0; i < bulki1->numKeys; i++) {
        if (!BULKI_Entity_equal(&bulki1->header->keys[i], &bulki2->header->keys[i]) ||
            !BULKI_Entity_equal(&bulki1->data->values[i], &bulki2->data->values[i])) {
            printf("Error: bulki1 and bulki2 are not equal in terms of data\n");
            return 0;
        }
    }
    return 1;
}

void
BULKI_put(BULKI *bulki, BULKI_Entity *key, BULKI_Entity *value)
{
    if (bulki == NULL || key == NULL || value == NULL) {
        printf("Error: bulki, key, or value is NULL\n");
        return;
    }
    // search for existing key
    BULKI_Entity *existing_value = BULKI_get(bulki, key);
    if (existing_value != NULL) {
        bulki->header->headerSize -= key->size;
        bulki->data->dataSize -= existing_value->size;
        memcpy(existing_value, value, sizeof(BULKI_Entity));
        bulki->header->headerSize += key->size;
        bulki->data->dataSize += value->size;
        get_BULKI_size(bulki);
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

BULKI_Entity_Iterator *
Bent_iterator_init(BULKI_Entity *array, void *filter, pdc_c_var_type_t filter_type)
{
    if (array == NULL || array->pdc_class != PDC_CLS_ARRAY) {
        printf("Error: not a proper array\n");
        return NULL;
    }
    BULKI_Entity_Iterator *iter = (BULKI_Entity_Iterator *)calloc(1, sizeof(BULKI_Entity_Iterator));
    iter->entity_array          = array;
    iter->total_size            = array->count;
    iter->current_idx           = 0;

    if (filter != NULL) {
        if (array->pdc_type == filter_type) {
            iter->bent_filter = filter;
            iter->filter_type = filter_type;
        } // if the filter is not appropriate, just ignore it.
    }
    return iter;
}

int
Bent_iterator_has_next_BULKI(BULKI_Entity_Iterator *it)
{
    if (it->bent_filter == NULL) {
        return it->current_idx < it->total_size;
    }
    else {
        while (it->current_idx < it->total_size) {
            if (it->filter_type == PDC_BULKI) {
                BULKI *array_content = (BULKI *)it->entity_array->data;
                BULKI *current_bulki = &(array_content[it->current_idx]);
                if (BULKI_equal(current_bulki, it->bent_filter)) {
                    return 1;
                }
            }
            it->current_idx++;
        }
    }
    return 0;
}

int
Bent_iterator_has_next_Bent(BULKI_Entity_Iterator *it)
{
    if (it->bent_filter == NULL) {
        return it->current_idx < it->total_size;
    }
    else {
        while (it->current_idx < it->total_size) {
            if (it->filter_type == PDC_BULKI_ENT) {
                BULKI_Entity *array_content  = (BULKI_Entity *)it->entity_array->data;
                BULKI_Entity *current_entity = &(array_content[it->current_idx]);
                if (BULKI_Entity_equal(current_entity, it->bent_filter)) {
                    return 1;
                }
            }
            it->current_idx++;
        }
    }
    return 0;
}

BULKI *
Bent_iterator_next_BULKI(BULKI_Entity_Iterator *it)
{
    if (it->current_idx < it->total_size) {
        if (it->entity_array->pdc_type == PDC_BULKI) {
            BULKI *array_content = (BULKI *)it->entity_array->data;
            return &(array_content[it->current_idx++]);
        }
    }
    return NULL;
}

BULKI_Entity *
Bent_iterator_next_Bent(BULKI_Entity_Iterator *it)
{
    if (it->current_idx < it->total_size) {
        if (it->entity_array->pdc_type == PDC_BULKI_ENT) {
            BULKI_Entity *array_content = (BULKI_Entity *)it->entity_array->data;
            return &(array_content[it->current_idx++]);
        }
    }
    return NULL;
}

BULKI_KV_Pair_Iterator *
BULKI_KV_Pair_iterator_init(BULKI *bulki)
{
    if (bulki == NULL) {
        printf("Error: bulki is NULL\n");
        return NULL;
    }
    BULKI_KV_Pair_Iterator *iter = (BULKI_KV_Pair_Iterator *)calloc(1, sizeof(BULKI_KV_Pair_Iterator));
    iter->bulki                  = bulki;
    iter->total_size             = bulki->numKeys;
    iter->current_idx            = 0;
    return iter;
}

int
BULKI_KV_Pair_iterator_has_next(BULKI_KV_Pair_Iterator *it)
{
    return it->current_idx < it->total_size;
}

BULKI_KV_Pair *
BULKI_KV_Pair_iterator_next(BULKI_KV_Pair_Iterator *it)
{
    if (it->current_idx < it->total_size) {
        BULKI_KV_Pair *pair = (BULKI_KV_Pair *)calloc(1, sizeof(BULKI_KV_Pair));
        pair->key           = it->bulki->header->keys[it->current_idx];
        pair->value         = it->bulki->data->values[it->current_idx];
        it->current_idx++;
        return pair;
    }
    return NULL;
}

BULKI_Entity *
BULKI_get(BULKI *bulki, BULKI_Entity *key)
{
    for (size_t i = 0; i < bulki->numKeys; i++) {
        if (BULKI_Entity_equal(&bulki->header->keys[i], key)) {
            return &bulki->data->values[i];
        }
    }
    return NULL;
}

void
BULKI_Entity_free(BULKI_Entity *bulk_entity, int free_struct)
{
    if (bulk_entity != NULL) {
        if (bulk_entity->pdc_class == PDC_CLS_ARRAY) { // if the entity is an array of BULKI or BULKI_Entity
            if (bulk_entity->pdc_type == PDC_BULKI && bulk_entity->data != NULL) {
                BULKI *bulki_array = (BULKI *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    BULKI_free(&bulki_array[i], 0);
                }
                printf("Freeing bulki_array 1\n");
                bulki_array = NULL;
            }
            else if (bulk_entity->pdc_type == PDC_BULKI_ENT && bulk_entity->data != NULL) {
                BULKI_Entity *bulki_entity_array = (BULKI_Entity *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    BULKI_Entity_free(&bulki_entity_array[i], 0);
                }
                printf("Freeing bulki_array 2\n");
                bulki_entity_array = NULL;
            }
        }
        else if (bulk_entity->pdc_class == PDC_CLS_ITEM) {
            if (bulk_entity->pdc_type == PDC_BULKI && bulk_entity->data != NULL) {
                BULKI_free((BULKI *)bulk_entity->data, 0);
                bulk_entity->data = NULL;
                printf("Freeing bulki_item 1\n");
            }
        }
        printf("Freeing bulk_entity\n");
        if (bulk_entity->data != NULL) {
            printf("bulki_entity->class: %d, bulki_entity->class: %d, bulki_entity->data: %p, bulki_entity: "
                   "%p\n",
                   bulk_entity->pdc_class, bulk_entity->pdc_type, bulk_entity->data, bulk_entity);
            free(bulk_entity->data);
            bulk_entity->data = NULL;
        }
        if (free_struct) {
            free(bulk_entity);
            bulk_entity = NULL;
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
                bulki->header->keys = NULL;
            }
            free(bulki->header);
            bulki->header = NULL;
        }
        if (bulki->data != NULL) {
            if (bulki->data->values != NULL) {
                for (size_t i = 0; i < bulki->numKeys; i++) {
                    BULKI_Entity_free(&bulki->data->values[i], 0);
                }
                free(bulki->data->values);
                bulki->data->values = NULL;
            }
            free(bulki->data);
            bulki->data = NULL;
        }
        if (free_struct) {
            free(bulki);
            bulki = NULL;
        }
    }
} // BULKI_free