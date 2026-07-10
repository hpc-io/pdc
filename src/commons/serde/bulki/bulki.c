#include "bulki.h"
#include "bulki_vle_util.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

/** Wire size of a BULKI_Entity header: pdc_class, pdc_type, count, size fields. */
#define BULKI_ENTITY_WIRE_HEADER_SIZE ((size_t)(sizeof(int8_t) * 2 + sizeof(uint64_t) * 2))
/** Wire size of a BULKI map header: totalSize, numKeys, headerSize, dataSize, offsets. */
#define BULKI_WIRE_META_SIZE ((size_t)(sizeof(uint64_t) * 6))

static size_t        bulki_entity_wire_size(BULKI_Entity *entity);
static size_t        bulki_wire_size(BULKI *bulki);
static void          bulki_refresh_total_size(BULKI *bulki);
static void          bulki_entity_array_ensure_capacity(BULKI_Entity *dest, size_t element_size);
static BULKI_Entity *empty_array_entity(pdc_c_var_type_t pdc_type, int initial_capacity);

size_t
get_BULKI_Entity_size(BULKI_Entity *bulk_entity)
{
    FUNC_ENTER(NULL);

    if (bulk_entity == NULL) {
        FUNC_LEAVE(0);
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

    FUNC_LEAVE(size);
}

size_t
get_BULKI_size(BULKI *bulki)
{
    FUNC_ENTER(NULL);

    if (bulki == NULL) {
        FUNC_LEAVE(0);
    }
    size_t size = sizeof(uint64_t) * 6; // totalSize, numKeys, headerSize, dataSize, headerOffset, dataOffset

    for (size_t i = 0; i < bulki->numKeys; i++) {
        size += get_BULKI_Entity_size(&bulki->header->keys[i]);
        size += get_BULKI_Entity_size(&bulki->data->values[i]);
    }
    bulki->totalSize = size;

    FUNC_LEAVE(size);
}

/**
 * Return the serialized byte length of a BULKI_Entity for incremental size updates.
 *
 * Uses the cached `entity->size` when set; otherwise falls back to a full
 * `get_BULKI_Entity_size()` walk. Used by `_incremental` append helpers to avoid
 * recomputing sibling subtrees on each append.
 */
static size_t
bulki_entity_wire_size(BULKI_Entity *entity)
{
    if (entity != NULL && entity->size > 0)
        return entity->size;
    return get_BULKI_Entity_size(entity);
}

/**
 * Return the serialized byte length of a BULKI map for incremental size updates.
 *
 * Uses the cached `bulki->totalSize` when set; otherwise falls back to
 * `get_BULKI_size()`. Used when appending a BULKI child to an array entity.
 */
static size_t
bulki_wire_size(BULKI *bulki)
{
    if (bulki != NULL && bulki->totalSize > 0)
        return bulki->totalSize;
    return get_BULKI_size(bulki);
}

/**
 * Recompute `bulki->totalSize` from cached header and data region sizes in O(1).
 *
 * Assumes `headerSize` and `dataSize` are already accurate (maintained by
 * `BULKI_put_incremental` / `BULKI_delete_incremental`). Does not walk keys.
 */
static void
bulki_refresh_total_size(BULKI *bulki)
{
    bulki->totalSize = BULKI_WIRE_META_SIZE + bulki->header->headerSize + bulki->data->dataSize;
}

/**
 * Grow a BULKI_Entity array backing store when `count` reaches `capacity`.
 *
 * Doubles capacity (minimum 1) and reallocates `dest->data`. No-op when free
 * slots remain. Used by `_incremental` append paths to avoid per-element realloc.
 *
 * @param dest          Array entity (PDC_CLS_ARRAY, PDC_BULKI or PDC_BULKI_ENT)
 * @param element_size  sizeof(BULKI) or sizeof(BULKI_Entity)
 */
static void
bulki_entity_array_ensure_capacity(BULKI_Entity *dest, size_t element_size)
{
    if (dest->count < dest->capacity)
        return;

    if (dest->capacity == 0)
        dest->capacity = 1;
    else
        dest->capacity *= 2;

    dest->data = PDC_realloc(dest->data, dest->capacity * element_size);
}

/**
 * Allocate an empty array BULKI_Entity, optionally with pre-sized backing storage.
 *
 * Shared by `empty_BULKI_Array_Entity_with_capacity` and
 * `empty_Bent_Array_Entity_with_capacity`. Initializes `size` to the wire header
 * only; `_incremental` appends add child wire sizes as elements are inserted.
 *
 * @param pdc_type           PDC_BULKI or PDC_BULKI_ENT
 * @param initial_capacity   Slot count to pre-allocate; 0 leaves `data` NULL
 */
static BULKI_Entity *
empty_array_entity(pdc_c_var_type_t pdc_type, int initial_capacity)
{
    BULKI_Entity *bulki_entity = (BULKI_Entity *)PDC_calloc(1, sizeof(BULKI_Entity));
    bulki_entity->pdc_type     = pdc_type;
    bulki_entity->pdc_class    = PDC_CLS_ARRAY;
    bulki_entity->count        = 0;
    bulki_entity->capacity     = 0;
    bulki_entity->data         = NULL;
    bulki_entity->size         = BULKI_ENTITY_WIRE_HEADER_SIZE;

    if (initial_capacity > 0) {
        bulki_entity->capacity = (uint64_t)initial_capacity;
        if (pdc_type == PDC_BULKI)
            bulki_entity->data = PDC_calloc((size_t)initial_capacity, sizeof(BULKI));
        else if (pdc_type == PDC_BULKI_ENT)
            bulki_entity->data = PDC_calloc((size_t)initial_capacity, sizeof(BULKI_Entity));
    }

    return bulki_entity;
}

void
BULKI_Entity_print(BULKI_Entity *bulk_entity)
{
    FUNC_ENTER(NULL);

    if (bulk_entity == NULL) {
        LOG_ERROR("bulki_entity is NULL\n");
        FUNC_LEAVE_VOID();
    }
    LOG_INFO("BULKI_Entity:\n");
    LOG_INFO("pdc_class: %d\n", bulk_entity->pdc_class);
    LOG_INFO("pdc_type: %d\n", bulk_entity->pdc_type);
    LOG_INFO("count: %zu\n", bulk_entity->count);
    LOG_INFO("size: %zu\n", bulk_entity->size);
    if (bulk_entity->pdc_class == PDC_CLS_ARRAY) {
        if (bulk_entity->pdc_type == PDC_BULKI) {
            BULKI *bulki_array = (BULKI *)bulk_entity->data;
            for (size_t i = 0; i < bulk_entity->count; i++) {
                LOG_INFO("BULKI[%zu]:\n", i);
                BULKI_print(&bulki_array[i]);
            }
        }
        else if (bulk_entity->pdc_type == PDC_BULKI_ENT) {
            BULKI_Entity *bulki_entity_array = (BULKI_Entity *)bulk_entity->data;
            for (size_t i = 0; i < bulk_entity->count; i++) {
                LOG_INFO("BULKI_Entity[%zu]:\n", i);
                BULKI_Entity_print(&bulki_entity_array[i]);
            }
        }
        else {
            LOG_INFO("BULKI_Entity[%zu]:\n", bulk_entity->count);
            for (size_t i = 0; i < bulk_entity->count; i++) {
                LOG_INFO("%s : ", DataTypeNames[bulk_entity->pdc_type]);
            }
            LOG_INFO("\n");
        }
    }
    else if (bulk_entity->pdc_class == PDC_CLS_ITEM) {
        if (bulk_entity->pdc_type == PDC_BULKI) {
            LOG_INFO("BULKI:\n");
            BULKI_print((BULKI *)bulk_entity->data);
        }
        else {
            LOG_INFO("%s\n", DataTypeNames[bulk_entity->pdc_type]);
        }
    }

    FUNC_LEAVE_VOID();
}

void
BULKI_print(BULKI *bulki)
{
    FUNC_ENTER(NULL);

    if (bulki == NULL) {
        LOG_ERROR("Error: bulki is NULL\n");
        FUNC_LEAVE_VOID();
    }
    LOG_INFO("BULKI:\n");
    LOG_INFO("totalSize: %zu\n", bulki->totalSize);
    LOG_INFO("numKeys: %zu\n", bulki->numKeys);
    LOG_INFO("headerSize: %zu\n", bulki->header->headerSize);
    LOG_INFO("dataSize: %zu\n", bulki->data->dataSize);
    for (size_t i = 0; i < bulki->numKeys; i++) {
        LOG_INFO("key[%zu]:\n", i);
        BULKI_Entity_print(&bulki->header->keys[i]);
        LOG_INFO("value[%zu]:\n", i);
        BULKI_Entity_print(&bulki->data->values[i]);
    }

    FUNC_LEAVE_VOID();
}

BULKI_Entity *
empty_BULKI_Array_Entity()
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(empty_array_entity(PDC_BULKI, 0));
}

BULKI_Entity *
empty_BULKI_Array_Entity_with_capacity(int initial_capacity)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(empty_array_entity(PDC_BULKI, initial_capacity));
}

BULKI_Entity *
empty_Bent_Array_Entity()
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(empty_array_entity(PDC_BULKI_ENT, 0));
}

BULKI_Entity *
empty_Bent_Array_Entity_with_capacity(int initial_capacity)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(empty_array_entity(PDC_BULKI_ENT, initial_capacity));
}

BULKI_Entity *
BULKI_ENTITY_append_BULKI(BULKI_Entity *dest, BULKI *src)
{
    FUNC_ENTER(NULL);

    if (src == NULL || dest == NULL) {
        LOG_ERROR("Error: bulki is NULL\n");
        FUNC_LEAVE(NULL);
    }
    if (dest->pdc_class != PDC_CLS_ARRAY || dest->pdc_type != PDC_BULKI) {
        LOG_ERROR("Error: dest is not an array of BULKI structure\n");
        FUNC_LEAVE(NULL);
    }
    dest->count = dest->count + 1;
    dest->data  = PDC_realloc(dest->data, dest->count * sizeof(BULKI));
    memcpy(dest->data + (dest->count - 1) * sizeof(BULKI), src, sizeof(BULKI));
    get_BULKI_Entity_size(dest);

    FUNC_LEAVE(dest);
}

BULKI_Entity *
BULKI_ENTITY_append_BULKI_incremental(BULKI_Entity *dest, BULKI *src)
{
    FUNC_ENTER(NULL);

    size_t child_size;

    if (src == NULL || dest == NULL) {
        LOG_ERROR("Error: bulki is NULL\n");
        FUNC_LEAVE(NULL);
    }
    if (dest->pdc_class != PDC_CLS_ARRAY || dest->pdc_type != PDC_BULKI) {
        LOG_ERROR("Error: dest is not an array of BULKI structure\n");
        FUNC_LEAVE(NULL);
    }

    child_size = bulki_wire_size(src);
    bulki_entity_array_ensure_capacity(dest, sizeof(BULKI));
    memcpy((BULKI *)dest->data + dest->count, src, sizeof(BULKI));
    dest->size += child_size;
    dest->count++;

    FUNC_LEAVE(dest);
}

BULKI *
BULKI_ENTITY_get_BULKI(BULKI_Entity *bulki_entity, size_t idx)
{
    FUNC_ENTER(NULL);

    if (bulki_entity == NULL) {
        LOG_ERROR("Error: bulki_entity is NULL\n");
        FUNC_LEAVE(NULL);
    }
    if (bulki_entity->pdc_class != PDC_CLS_ARRAY || bulki_entity->pdc_type != PDC_BULKI) {
        LOG_ERROR("Error: bulki_entity is not an array of BULKI structure\n");
        FUNC_LEAVE(NULL);
    }
    if (idx >= bulki_entity->count) {
        LOG_WARNING("idx = %d, count = %d Warning: index for bulki_entity is out of bound\n", idx,
                    bulki_entity->count);
        FUNC_LEAVE(NULL);
    }

    FUNC_LEAVE(&((BULKI *)bulki_entity->data)[idx]);
}

BULKI_Entity *
BULKI_ENTITY_append_BULKI_Entity(BULKI_Entity *dest, BULKI_Entity *src)
{
    FUNC_ENTER(NULL);

    if (src == NULL || dest == NULL) {
        LOG_ERROR("Error: bulki is NULL\n");
        FUNC_LEAVE(NULL);
    }
    if (dest->pdc_class != PDC_CLS_ARRAY || dest->pdc_type != PDC_BULKI_ENT) {
        LOG_ERROR("Error: dest is not an array of BULKI_Entity structure\n");
        FUNC_LEAVE(NULL);
    }
    dest->count = dest->count + 1;
    dest->data  = PDC_realloc(dest->data, dest->count * sizeof(BULKI_Entity));
    memcpy(dest->data + (dest->count - 1) * sizeof(BULKI_Entity), src, sizeof(BULKI_Entity));
    get_BULKI_Entity_size(dest);

    FUNC_LEAVE(dest);
}

BULKI_Entity *
BULKI_ENTITY_append_BULKI_Entity_incremental(BULKI_Entity *dest, BULKI_Entity *src)
{
    FUNC_ENTER(NULL);

    size_t child_size;

    if (src == NULL || dest == NULL) {
        LOG_ERROR("Error: bulki is NULL\n");
        FUNC_LEAVE(NULL);
    }
    if (dest->pdc_class != PDC_CLS_ARRAY || dest->pdc_type != PDC_BULKI_ENT) {
        LOG_ERROR("Error: dest is not an array of BULKI_Entity structure\n");
        FUNC_LEAVE(NULL);
    }

    child_size = bulki_entity_wire_size(src);
    bulki_entity_array_ensure_capacity(dest, sizeof(BULKI_Entity));
    memcpy((BULKI_Entity *)dest->data + dest->count, src, sizeof(BULKI_Entity));
    dest->size += child_size;
    dest->count++;

    FUNC_LEAVE(dest);
}

BULKI_Entity *
BULKI_ENTITY_get_BULKI_Entity(BULKI_Entity *bulki_entity, size_t idx)
{
    FUNC_ENTER(NULL);

    if (bulki_entity == NULL) {
        LOG_ERROR("Error: bulki_entity is NULL\n");
        FUNC_LEAVE(NULL);
    }
    if (bulki_entity->pdc_class != PDC_CLS_ARRAY || bulki_entity->pdc_type != PDC_BULKI_ENT) {
        LOG_ERROR("Error: bulki_entity is not an array of BULKI_Entity structure\n");
        FUNC_LEAVE(NULL);
    }
    if (idx >= bulki_entity->count) {
        FUNC_LEAVE(NULL);
    }

    FUNC_LEAVE(&((BULKI_Entity *)bulki_entity->data)[idx]);
}

BULKI_Entity *
BULKI_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class)
{
    FUNC_ENTER(NULL);

    if (pdc_type == PDC_BULKI_ENT && pdc_class == PDC_CLS_ITEM) {
        LOG_ERROR("Error: BULKI_Entity cannot be an single item in another BULKI_Entity\n");
        FUNC_LEAVE(NULL);
    }
    BULKI_Entity *bulki_entity = (BULKI_Entity *)PDC_calloc(1, sizeof(BULKI_Entity));
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
        // note: BULKI_ENTITY currently deep-copies payload into BULKI-owned memory.
        // note: this copy can be avoided by introducing explicit pointer ownership semantics
        // note: (e.g., borrowed/non-owned data mode) so BULKI can reference external buffers
        // note: without freeing them in BULKI_Entity_free().
        bulki_entity->data = PDC_calloc(1, size);
        memcpy(bulki_entity->data, data, size);
    }

    get_BULKI_Entity_size(bulki_entity);

    FUNC_LEAVE(bulki_entity);
}

BULKI_Entity *
BULKI_singleton_ENTITY(void *data, pdc_c_var_type_t pdc_type)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(BULKI_ENTITY(data, 1, pdc_type, PDC_CLS_ITEM));
}

BULKI_Entity *
BULKI_array_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(BULKI_ENTITY(data, count, pdc_type, PDC_CLS_ARRAY));
}

BULKI *
BULKI_init(int initial_field_count)
{
    FUNC_ENTER(NULL);

    BULKI *buiki              = PDC_calloc(1, sizeof(BULKI));
    buiki->numKeys            = 0;
    buiki->capacity           = initial_field_count;
    buiki->header             = PDC_calloc(1, sizeof(BULKI_Header));
    buiki->header->keys       = PDC_calloc(buiki->capacity, sizeof(BULKI_Entity));
    buiki->header->headerSize = 0;
    buiki->data               = PDC_calloc(1, sizeof(BULKI_Data));
    buiki->data->values       = PDC_calloc(buiki->capacity, sizeof(BULKI_Entity));
    buiki->data->dataSize     = 0;
    bulki_refresh_total_size(buiki);

    FUNC_LEAVE(buiki);
}

int
BULKI_Entity_equal(BULKI_Entity *be1, BULKI_Entity *be2)
{
    FUNC_ENTER(NULL);

    int meta_equal = be1->pdc_type == be2->pdc_type && be1->pdc_class == be2->pdc_class &&
                     be1->count == be2->count && be1->size == be2->size;
    if (!meta_equal) {
        FUNC_LEAVE(0);
    }
    if (be1->pdc_class == PDC_CLS_ARRAY) {
        if (be1->pdc_type == PDC_BULKI) {
            BULKI *bulki_array1 = (BULKI *)be1->data;
            BULKI *bulki_array2 = (BULKI *)be2->data;
            for (size_t i = 0; i < be1->count; i++) {
                if (!BULKI_equal(&bulki_array1[i], &bulki_array2[i])) {
                    FUNC_LEAVE(0);
                }
            }
        }
        else if (be1->pdc_type == PDC_BULKI_ENT) {
            BULKI_Entity *bulki_entity_array1 = (BULKI_Entity *)be1->data;
            BULKI_Entity *bulki_entity_array2 = (BULKI_Entity *)be2->data;
            for (size_t i = 0; i < be1->count; i++) {
                if (!BULKI_Entity_equal(&bulki_entity_array1[i], &bulki_entity_array2[i])) {
                    FUNC_LEAVE(0);
                }
            }
        }
        else {
            if (memcmp(be1->data, be2->data, be1->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2) != 0) {
                FUNC_LEAVE(0);
            }
        }
    }
    else if (be1->pdc_class == PDC_CLS_ITEM) {
        if (be1->pdc_type == PDC_BULKI) {
            if (!BULKI_equal((BULKI *)be1->data, (BULKI *)be2->data)) {
                FUNC_LEAVE(0);
            }
        }
        else {
            if (memcmp(be1->data, be2->data, be1->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2) != 0) {
                FUNC_LEAVE(0);
            }
        }
    }

    FUNC_LEAVE(1);
}

int
BULKI_equal(BULKI *bulki1, BULKI *bulki2)
{
    FUNC_ENTER(NULL);

    if (bulki1->numKeys != bulki2->numKeys || bulki1->totalSize != bulki2->totalSize ||
        bulki1->header->headerSize != bulki2->header->headerSize ||
        bulki1->data->dataSize != bulki2->data->dataSize) {
        LOG_ERROR("Error: bulki1 and bulki2 are not equal in terms of metadata\n");
        FUNC_LEAVE(0);
    }
    for (size_t i = 0; i < bulki1->numKeys; i++) {
        if (!BULKI_Entity_equal(&bulki1->header->keys[i], &bulki2->header->keys[i]) ||
            !BULKI_Entity_equal(&bulki1->data->values[i], &bulki2->data->values[i])) {
            LOG_ERROR("Error: bulki1 and bulki2 are not equal in terms of data\n");
            FUNC_LEAVE(0);
        }
    }

    FUNC_LEAVE(1);
}

void
BULKI_put(BULKI *bulki, BULKI_Entity *key, BULKI_Entity *value)
{
    FUNC_ENTER(NULL);

    if (bulki == NULL || key == NULL || value == NULL) {
        LOG_ERROR("Error: bulki, key, or value is NULL\n");
        FUNC_LEAVE_VOID();
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
        FUNC_LEAVE_VOID();
    }
    if (bulki->numKeys >= bulki->capacity) {
        bulki->capacity *= 2;
        bulki->header->keys = PDC_realloc(bulki->header->keys, bulki->capacity * sizeof(BULKI_Entity));
        bulki->data->values = PDC_realloc(bulki->data->values, bulki->capacity * sizeof(BULKI_Entity));
    }
    memcpy(&bulki->header->keys[bulki->numKeys], key, sizeof(BULKI_Entity));
    // append bytes for type, size, and key
    bulki->header->headerSize += key->size;

    memcpy(&bulki->data->values[bulki->numKeys], value, sizeof(BULKI_Entity));
    // append bytes for class, type, size, and data
    bulki->data->dataSize += value->size;

    bulki->numKeys++;
    get_BULKI_size(bulki);

    FUNC_LEAVE_VOID();
}

void
BULKI_put_incremental(BULKI *bulki, BULKI_Entity *key, BULKI_Entity *value)
{
    FUNC_ENTER(NULL);

    if (bulki == NULL || key == NULL || value == NULL) {
        LOG_ERROR("Error: bulki, key, or value is NULL\n");
        FUNC_LEAVE_VOID();
    }
    // search for existing key
    BULKI_Entity *existing_value = BULKI_get(bulki, key);
    if (existing_value != NULL) {
        bulki->header->headerSize -= key->size;
        bulki->data->dataSize -= existing_value->size;
        memcpy(existing_value, value, sizeof(BULKI_Entity));
        bulki->header->headerSize += key->size;
        bulki->data->dataSize += value->size;
        bulki_refresh_total_size(bulki);
        FUNC_LEAVE_VOID();
    }
    if (bulki->numKeys >= bulki->capacity) {
        bulki->capacity *= 2;
        bulki->header->keys = PDC_realloc(bulki->header->keys, bulki->capacity * sizeof(BULKI_Entity));
        bulki->data->values = PDC_realloc(bulki->data->values, bulki->capacity * sizeof(BULKI_Entity));
    }
    memcpy(&bulki->header->keys[bulki->numKeys], key, sizeof(BULKI_Entity));
    // append bytes for type, size, and key
    bulki->header->headerSize += key->size;

    memcpy(&bulki->data->values[bulki->numKeys], value, sizeof(BULKI_Entity));
    // append bytes for class, type, size, and data
    bulki->data->dataSize += value->size;

    bulki->numKeys++;
    bulki_refresh_total_size(bulki);

    FUNC_LEAVE_VOID();
}

BULKI_Entity *
BULKI_delete(BULKI *bulki, BULKI_Entity *key)
{
    FUNC_ENTER(NULL);

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

    FUNC_LEAVE(value);
}

BULKI_Entity *
BULKI_delete_incremental(BULKI *bulki, BULKI_Entity *key)
{
    FUNC_ENTER(NULL);

    if (bulki == NULL || key == NULL) {
        LOG_ERROR("Error: bulki or key is NULL in BULKI_delete_incremental()\n");
        FUNC_LEAVE(NULL);
    }

    BULKI_Entity *value = NULL;

    if (bulki->numKeys == 0) {
        bulki_refresh_total_size(bulki);
        FUNC_LEAVE(NULL);
    }

    size_t last_index = bulki->numKeys - 1;
    for (size_t i = 0; i < bulki->numKeys; i++) {
        if (BULKI_Entity_equal(&bulki->header->keys[i], key)) {
            value = &bulki->data->values[i];
            bulki->header->headerSize -= key->size;
            bulki->data->dataSize -= value->size;
            if (i != last_index) {
                memcpy(&bulki->header->keys[i], &bulki->header->keys[last_index], sizeof(BULKI_Entity));
                memcpy(&bulki->data->values[i], &bulki->data->values[last_index], sizeof(BULKI_Entity));
            }
            bulki->numKeys--;
            break;
        }
    }
    bulki_refresh_total_size(bulki);

    FUNC_LEAVE(value);
}

BULKI_Entity_Iterator *
Bent_iterator_init(BULKI_Entity *array, void *filter, pdc_c_var_type_t filter_type)
{
    FUNC_ENTER(NULL);

    if (array == NULL || array->pdc_class != PDC_CLS_ARRAY) {
        LOG_ERROR("Error: not a proper array\n");
        FUNC_LEAVE(NULL);
    }
    BULKI_Entity_Iterator *iter = (BULKI_Entity_Iterator *)PDC_calloc(1, sizeof(BULKI_Entity_Iterator));
    iter->entity_array          = array;
    iter->total_size            = array->count;
    iter->current_idx           = 0;

    if (filter != NULL) {
        if (array->pdc_type == filter_type) {
            iter->bent_filter = filter;
            iter->filter_type = filter_type;
        } // if the filter is not appropriate, just ignore it.
    }

    FUNC_LEAVE(iter);
}

int
Bent_iterator_has_next_BULKI(BULKI_Entity_Iterator *it)
{
    FUNC_ENTER(NULL);

    if (it->bent_filter == NULL) {
        FUNC_LEAVE(it->current_idx < it->total_size);
    }
    else {
        while (it->current_idx < it->total_size) {
            if (it->filter_type == PDC_BULKI) {
                BULKI *array_content = (BULKI *)it->entity_array->data;
                BULKI *current_bulki = &(array_content[it->current_idx]);
                if (BULKI_equal(current_bulki, it->bent_filter)) {
                    FUNC_LEAVE(1);
                }
            }
            it->current_idx++;
        }
    }

    FUNC_LEAVE(0);
}

int
Bent_iterator_has_next_Bent(BULKI_Entity_Iterator *it)
{
    FUNC_ENTER(NULL);

    if (it->bent_filter == NULL) {
        FUNC_LEAVE(it->current_idx < it->total_size);
    }
    else {
        while (it->current_idx < it->total_size) {
            if (it->filter_type == PDC_BULKI_ENT) {
                BULKI_Entity *array_content  = (BULKI_Entity *)it->entity_array->data;
                BULKI_Entity *current_entity = &(array_content[it->current_idx]);
                if (BULKI_Entity_equal(current_entity, it->bent_filter)) {
                    FUNC_LEAVE(1);
                }
            }
            it->current_idx++;
        }
    }

    FUNC_LEAVE(0);
}

BULKI *
Bent_iterator_next_BULKI(BULKI_Entity_Iterator *it)
{
    FUNC_ENTER(NULL);

    if (it->current_idx < it->total_size) {
        if (it->entity_array->pdc_type == PDC_BULKI) {
            BULKI *array_content = (BULKI *)it->entity_array->data;
            FUNC_LEAVE(&(array_content[it->current_idx++]));
        }
    }

    FUNC_LEAVE(NULL);
}

BULKI_Entity *
Bent_iterator_next_Bent(BULKI_Entity_Iterator *it)
{
    FUNC_ENTER(NULL);

    if (it->current_idx < it->total_size) {
        if (it->entity_array->pdc_type == PDC_BULKI_ENT) {
            BULKI_Entity *array_content = (BULKI_Entity *)it->entity_array->data;
            FUNC_LEAVE(&(array_content[it->current_idx++]));
        }
    }

    FUNC_LEAVE(NULL);
}

BULKI_KV_Pair_Iterator *
BULKI_KV_Pair_iterator_init(BULKI *bulki)
{
    FUNC_ENTER(NULL);

    if (bulki == NULL) {
        LOG_ERROR("Error: bulki is NULL\n");
        FUNC_LEAVE(NULL);
    }
    BULKI_KV_Pair_Iterator *iter = (BULKI_KV_Pair_Iterator *)PDC_calloc(1, sizeof(BULKI_KV_Pair_Iterator));
    iter->bulki                  = bulki;
    iter->total_size             = bulki->numKeys;
    iter->current_idx            = 0;

    FUNC_LEAVE(iter);
}

int
BULKI_KV_Pair_iterator_has_next(BULKI_KV_Pair_Iterator *it)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(it->current_idx < it->total_size);
}

BULKI_KV_Pair *
BULKI_KV_Pair_iterator_next(BULKI_KV_Pair_Iterator *it)
{
    FUNC_ENTER(NULL);

    if (it->current_idx < it->total_size) {
        BULKI_KV_Pair *pair = (BULKI_KV_Pair *)PDC_calloc(1, sizeof(BULKI_KV_Pair));
        pair->key           = it->bulki->header->keys[it->current_idx];
        pair->value         = it->bulki->data->values[it->current_idx];
        it->current_idx++;
        FUNC_LEAVE(pair);
    }

    FUNC_LEAVE(NULL);
}

BULKI_Entity *
BULKI_get(BULKI *bulki, BULKI_Entity *key)
{
    FUNC_ENTER(NULL);

    for (size_t i = 0; i < bulki->numKeys; i++) {
        if (BULKI_Entity_equal(&bulki->header->keys[i], key)) {
            FUNC_LEAVE(&bulki->data->values[i]);
        }
    }

    FUNC_LEAVE(NULL);
}

void
BULKI_Entity_free(BULKI_Entity *bulk_entity, int free_struct)
{
    FUNC_ENTER(NULL);

    if (bulk_entity != NULL) {
        if (bulk_entity->pdc_class == PDC_CLS_ARRAY) { // if the entity is an array of BULKI or BULKI_Entity
            if (bulk_entity->pdc_type == PDC_BULKI && bulk_entity->data != NULL) {
                BULKI *bulki_array = (BULKI *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    BULKI_free(&bulki_array[i], 0);
                }
#ifdef BUKLI_DEBUG_LOG
                LOG_INFO("Freeing bulki_array 1\n");
#endif
                bulki_array = NULL;
            }
            else if (bulk_entity->pdc_type == PDC_BULKI_ENT && bulk_entity->data != NULL) {
                BULKI_Entity *bulki_entity_array = (BULKI_Entity *)bulk_entity->data;
                for (size_t i = 0; i < bulk_entity->count; i++) {
                    BULKI_Entity_free(&bulki_entity_array[i], 0);
                }
#ifdef BUKLI_DEBUG_LOG
                LOG_INFO("Freeing bulki_array 2\n");
#endif
                bulki_entity_array = NULL;
            }
        }
        // copilot suggests: BULKI_Entity_free() frees the internals of PDC_BULKI items
        // but not the BULKI struct itself (BULKI_free(..., 0)), and then clears bulk_entity->data.
        // For heap-allocated nested BULKI maps (as used in checkpoint trees), this leaks the BULKI
        // wrapper and makes it impossible to free later.
        else if (bulk_entity->pdc_class == PDC_CLS_ITEM) {
            if (bulk_entity->pdc_type == PDC_BULKI && bulk_entity->data != NULL) {
                BULKI_free((BULKI *)bulk_entity->data, 0);
                bulk_entity->data = NULL;
#ifdef BUKLI_DEBUG_LOG
                LOG_INFO("Freeing bulki_item 1\n");
#endif
            }
        }
#ifdef BUKLI_DEBUG_LOG
        LOG_INFO("Freeing bulk_entity\n");
#endif
        if (bulk_entity->data != NULL) {
#ifdef BUKLI_DEBUG_LOG
            LOG_INFO(
                "bulki_entity->class: %d, bulki_entity->class: %d, bulki_entity->data: %p, bulki_entity: "
                "%p\n",
                bulk_entity->pdc_class, bulk_entity->pdc_type, bulk_entity->data, bulk_entity);
#endif
            bulk_entity->data = (void *)PDC_free(bulk_entity->data);
            bulk_entity->data = NULL;
        }
        if (free_struct) {
            bulk_entity = (BULKI_Entity *)PDC_free(bulk_entity);
            bulk_entity = NULL;
        }
    }

    FUNC_LEAVE_VOID();
} // BULKI_Entity_free

void
BULKI_free(BULKI *bulki, int free_struct)
{
    FUNC_ENTER(NULL);

    if (bulki != NULL) {
        if (bulki->header != NULL) {
            if (bulki->header->keys != NULL) {
                for (size_t i = 0; i < bulki->numKeys; i++) {
                    BULKI_Entity_free(&bulki->header->keys[i], 0);
                }
                bulki->header->keys = (BULKI_Entity *)PDC_free(bulki->header->keys);
                bulki->header->keys = NULL;
            }
            bulki->header = (BULKI_Header *)PDC_free(bulki->header);
            bulki->header = NULL;
        }
        if (bulki->data != NULL) {
            if (bulki->data->values != NULL) {
                for (size_t i = 0; i < bulki->numKeys; i++) {
                    BULKI_Entity_free(&bulki->data->values[i], 0);
                }
                bulki->data->values = (BULKI_Entity *)PDC_free(bulki->data->values);
                bulki->data->values = NULL;
            }
            bulki->data = (BULKI_Data *)PDC_free(bulki->data);
            bulki->data = NULL;
        }
        if (free_struct) {
            bulki = (BULKI *)PDC_free(bulki);
            bulki = NULL;
        }
    }

    FUNC_LEAVE_VOID();
} // BULKI_free