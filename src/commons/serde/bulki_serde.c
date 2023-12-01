#include "bulki_serde.h"

// clang-format off
/**
 * This function serializes the entire BULKI structure.
 * 
 * The overview of the serialized binary data layout is:
 * +---------------------+---------------------+---------------------+---------------------+---------------------+----------------------+----------------------+----------------------+
 * |   Total Size        |   Number of Keys    | Size of the Header  |   Size of the Data  |   Header Region     | Data Offset          |   Data Region        | Data Offset          |
 * |     (uint64_t)      |     (uint64_t)      |     (uint64_t)      |   (uint64_t)        |   (uint64_t)        |   (uint64_t)         |   (uint64_t)         |   (uint64_t)         |
 * +---------------------+---------------------+---------------------+---------------------+---------------------+----------------------+----------------------+----------------------+
 * 
 * The first 3 field is called meta-header, which provides metadata about the total size of BULKI, number of keys, size of the header region and the size of the data region.
 * 
 * The header region contains multiple BULKI entities, each of which is a key. 
 * Each BULKI entity contains the following fields:
 * +-------------------------+-----------------------------+---------------------------+--------------------------+-----------------------------+
 * | size                    |     Entitiy class           | Entitiy type              | count                    | data                        |
 * | (uint64_t)              |     (uint8_t)               | (uint8_t)                 | (uint64_t)               | (Variable size depending    |
 * |                         |                             |                           |                          | on the type and class)      |
 * +-------------------------+-----------------------------+---------------------------+--------------------------+-----------------------------+
 * 
 * Note that the data field in the BULKI entity is a pointer to either an array of BULKI entities , an array of BULKI structures, an array of base type items, 
 * or a single item of BULKI, or a single item of a base type.
 * 
 * After the header region, there is a data offset field, which is used to validate the header region.
 * 
 * The data region contains multiple BULKI entities, each of which is a value entry.
 * 
 * After the data region, there is another data offset field, which is used to validate the data region.
 * 
 * Please refer to `get_size_by_class_n_type` function in pdc_generic.h for size calculation on scalar values and array values.
 * 
 * For performance and simplicity, we do not recommend to use BULKI for large and deeply embedded data structures. 
 *
 */

// clang-format on

/********************** Serialize ************************** */

void *
BULKI_Entity_serialize_to_buffer(BULKI_Entity *entity, void *buffer, size_t *offset)
{
    printf("offset: %zu\n", *offset);
    // serialize the size
    uint64_t size = (uint64_t)get_BULKI_Entity_size(entity);
    memcpy(buffer + *offset, &size, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // serialize the class
    int8_t pdc_class = (int8_t)(entity->pdc_class);
    memcpy(buffer + *offset, &pdc_class, sizeof(int8_t));
    *offset += sizeof(int8_t);

    // serialize the type
    int8_t pdc_type = (int8_t)(entity->pdc_type);
    memcpy(buffer + *offset, &pdc_type, sizeof(int8_t));
    *offset += sizeof(int8_t);

    // serialize the count
    uint64_t count = (uint64_t)(entity->count);
    memcpy(buffer + *offset, &count, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    printf("PRE-ser: size: %zu, class: %d, type: %d, count: %zu, offset: %zu\n", entity->size,
           entity->pdc_class, entity->pdc_type, entity->count, *offset);

    // serialize the data
    if (entity->pdc_class == PDC_CLS_ITEM) {
        if (entity->pdc_type == PDC_BULKI) { // BULKI
            BULKI *bulki = (BULKI *)(entity->data);
            BULKI_serialize_to_buffer(bulki, buffer, offset);
        }
        else { // all base types
            memcpy(buffer + *offset, entity->data, entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            *offset += (entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
        }
    }
    else if (entity->pdc_class <= PDC_CLS_ARRAY) {
        if (pdc_type == PDC_BULKI) { // BULKI
            for (size_t i = 0; i < entity->count; i++) {
                BULKI *bulki = (BULKI *)entity->data;
                BULKI_serialize_to_buffer(bulki, buffer, offset);
            }
        }
        else if (pdc_type == PDC_BULKI_ENT) { // BULKI_Entity
            for (size_t i = 0; i < entity->count; i++) {
                BULKI_Entity *bulki_entity = (BULKI_Entity *)entity->data;
                BULKI_Entity_serialize_to_buffer(bulki_entity, buffer, offset);
            }
        }
        else { // all base types
            memcpy(buffer + *offset, entity->data, entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            *offset += (entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
        }
    }
    else {
        printf("Error: unsupported class type %d\n", entity->pdc_class);
        return NULL;
    }

    printf("POST-ser: size: %zu, size: %zu, class: %d, type: %d, count: %zu, offset: %zu\n", entity->size,
           size, entity->pdc_class, entity->pdc_type, entity->count, *offset);
    return buffer;
}

void *
BULKI_Entity_serialize(BULKI_Entity *entity)
{
    void * buffer = calloc(1, get_BULKI_Entity_size(entity));
    size_t offset = 0;
    BULKI_Entity_serialize_to_buffer(entity, buffer, &offset);
    printf("offset: %zu\n", offset);
    return buffer;
}

void *
BULKI_serialize_to_buffer(BULKI *bulki, void *buffer, size_t *offset)
{
    // serialize the total size
    memcpy(buffer + *offset, &bulki->totalSize, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // serialize the number of keys
    memcpy(buffer + *offset, &bulki->numKeys, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // serialize the header size
    memcpy(buffer + *offset, &bulki->header->headerSize, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // serialize the data size
    memcpy(buffer + *offset, &bulki->data->dataSize, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // serialize the header
    for (size_t i = 0; i < bulki->numKeys; i++) {
        BULKI_Entity_serialize_to_buffer(&(bulki->header->keys[i]), buffer, offset);
    }

    // serialize the data offset
    uint64_t ofst = (uint64_t)(*offset);
    memcpy(buffer + *offset, &ofst, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // serialize the data
    for (size_t i = 0; i < bulki->numKeys; i++) {
        BULKI_Entity_serialize_to_buffer(&(bulki->data->values[i]), buffer, offset);
    }

    // serialize the data offset
    ofst = (uint64_t)(*offset) + sizeof(uint64_t);
    memcpy(buffer + *offset, &ofst, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    return buffer;
}

void *
BULKI_serialize(BULKI *data)
{
    void * buffer = calloc(1, get_BULKI_size(data));
    size_t offset = 0;
    BULKI_serialize_to_buffer(data, buffer, &offset);
    printf("offset: %zu\n", offset);
    return buffer;
}

/********************** Deserialize ************************** */

BULKI_Entity *
BULKI_Entity_deserialize_from_buffer(void *buffer, size_t *offset)
{
    printf("offset: %zu\n", *offset);
    BULKI_Entity *entity = malloc(sizeof(BULKI_Entity));
    // deserialize the size
    uint64_t size;
    memcpy(&size, buffer + *offset, sizeof(uint64_t));
    entity->size = (size_t)size;
    *offset += sizeof(uint64_t);

    // deserialize the class
    int8_t pdc_class;
    memcpy(&pdc_class, buffer + *offset, sizeof(int8_t));
    *offset += sizeof(int8_t);
    entity->pdc_class = (pdc_c_var_class_t)pdc_class;

    // deserialize the type
    int8_t pdc_type;
    memcpy(&pdc_type, buffer + *offset, sizeof(int8_t));
    *offset += sizeof(int8_t);
    entity->pdc_type = (pdc_c_var_type_t)pdc_type;

    // deserialize the count
    uint64_t count;
    memcpy(&count, buffer + *offset, sizeof(uint64_t));
    entity->count = (size_t)count;
    *offset += sizeof(uint64_t);

    printf("PRE-DE: size: %zu, class: %d, type: %d, count: %zu, offset: %zu\n", entity->size,
           entity->pdc_class, entity->pdc_type, entity->count, *offset);

    // deserialize the data
    if (entity->pdc_class == PDC_CLS_ITEM) {
        if (entity->pdc_type == PDC_BULKI) { // BULKI
            entity->data = BULKI_deserialize_from_buffer(buffer, offset);
        }
        else { // all base types
            entity->data = malloc(entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            memcpy(entity->data, buffer + *offset, entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            *offset += (entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
        }
    }
    else if (entity->pdc_class <= PDC_CLS_ARRAY) {
        if (pdc_type == PDC_BULKI) { // BULKI
            BULKI *bulki_array = malloc(sizeof(BULKI) * entity->count);
            for (size_t i = 0; i < entity->count; i++) {
                memcpy(bulki_array + i, BULKI_deserialize_from_buffer(buffer, offset), sizeof(BULKI));
            }
            entity->data = bulki_array;
        }
        else if (pdc_type == PDC_BULKI_ENT) { // BULKI_Entity
            BULKI_Entity *bulki_entity_array = malloc(sizeof(BULKI_Entity) * entity->count);
            for (size_t i = 0; i < entity->count; i++) {
                memcpy(bulki_entity_array + i, BULKI_Entity_deserialize_from_buffer(buffer, offset),
                       sizeof(BULKI_Entity));
            }
            entity->data = bulki_entity_array;
        }
        else { // all base types
            entity->data = malloc(entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            memcpy(entity->data, buffer + *offset, entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            *offset += (entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
        }
    }
    else {
        printf("Error: unsupported class type %d\n", entity->pdc_class);
        return NULL;
    }

    printf("POST-DE: size: %zu, class: %d, type: %d, count: %zu, offset: %zu\n", entity->size,
           entity->pdc_class, entity->pdc_type, entity->count, *offset);
    return entity;
}

BULKI *
BULKI_deserialize_from_buffer(void *buffer, size_t *offset)
{
    BULKI *bulki = malloc(sizeof(BULKI));
    // deserialize the total size
    uint64_t totalSize;
    memcpy(&totalSize, buffer + *offset, sizeof(uint64_t));
    bulki->totalSize = totalSize;
    *offset += sizeof(uint64_t);
    printf("totalSize: %zu\n", bulki->totalSize);

    // deserialize the number of keys
    uint64_t numKeys;
    memcpy(&numKeys, buffer + *offset, sizeof(uint64_t));
    bulki->numKeys = numKeys;
    *offset += sizeof(uint64_t);

    // deserialize the header size
    uint64_t headerSize;
    memcpy(&headerSize, buffer + *offset, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // deserialize the data size
    uint64_t dataSize;
    memcpy(&dataSize, buffer + *offset, sizeof(uint64_t));
    *offset += sizeof(uint64_t);

    // deserialize the header
    BULKI_Header *header = malloc(sizeof(BULKI_Header));
    header->keys         = malloc(sizeof(BULKI_Entity) * numKeys);
    header->headerSize   = headerSize;
    for (size_t i = 0; i < numKeys; i++) {
        memcpy(&(header->keys[i]), BULKI_Entity_deserialize_from_buffer(buffer, offset),
               sizeof(BULKI_Entity));
    }

    // deserialize the data offset
    uint64_t dataOffset;
    memcpy(&dataOffset, buffer + *offset, sizeof(uint64_t));
    // check the data offset
    if (((size_t)dataOffset) != *offset) {
        printf("Error: data offset does not match the expected offset.\n");
        return NULL;
    }
    *offset += sizeof(uint64_t);

    bulki->header = header;

    // deserialize the data
    BULKI_Data *data = malloc(sizeof(BULKI_Data));
    data->values     = malloc(sizeof(BULKI_Entity) * numKeys);
    data->dataSize   = dataSize;
    for (size_t i = 0; i < numKeys; i++) {
        memcpy(&(data->values[i]), BULKI_Entity_deserialize_from_buffer(buffer, offset),
               sizeof(BULKI_Entity));
    }
    // check the total size
    memcpy(&dataOffset, buffer + *offset, sizeof(uint64_t));
    *offset += sizeof(uint64_t);
    printf("dataOffset: %zu, offset: %zu\n", dataOffset, *offset);

    // check the data offset
    if (((size_t)dataOffset) != *offset) {
        printf("Error: data offset does not match the expected offset.\n");
        return NULL;
    }

    bulki->data = data;

    return bulki;
}

BULKI_Entity *
BULKI_Entity_deserialize(void *buffer)
{
    size_t offset = 0;
    return BULKI_Entity_deserialize_from_buffer(buffer, &offset);
}

BULKI *
BULKI_deserialize(void *buffer)
{
    size_t offset = 0;
    return BULKI_deserialize_from_buffer(buffer, &offset);
}