#include "bulki_serde.h"
#include "bulki_vle_util.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

// clang-format off
/**
 * This function serializes the entire BULKI structure.
 * 
 * The overview of the serialized binary data layout is:
 * +---------------------+---------------------+---------------------+---------------------+---------------------+----------------------+
 * |   Total Size        |   Number of Keys    | Size of the Header  |   Size of the Data  |   Header Region     | Data Region          |
 * |     (uint64_t)      |     (uint64_t)      |     (uint64_t)      |   (uint64_t)        |   (uint64_t)        |   (uint64_t)         |
 * +---------------------+---------------------+---------------------+---------------------+---------------------+----------------------+
 * 
 * The first 4 fields are called meta-header, which provides metadata about the total size of BULKI, number of keys, size of the header region and the size of the data region.
 * 
 * The header/data region contains multiple BULKI entities. 
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

// | 7 | 6 | 5 | 4 | 3 | 2 | 1 |        0      |
// |---|---|---|---|---|---|---|---------------|
// |   Unused  | Type (5 bits) | Class (1 bit) |
uint8_t
serialize_type_class(pdc_c_var_type_t type, pdc_c_var_class_t class)
{
    FUNC_ENTER(NULL);

    // Ensure the type and class values fit within their bit allocations
    if (type >= PDC_TYPE_COUNT || class >= PDC_CLS_COUNT) {
        FUNC_LEAVE(0); // Handle invalid inputs, or use an appropriate error handling approach
    }

    // Pack the type and class into a single byte
    uint8_t serialized = (type & 0x1F) | ((class & 0x01) << 5);

    FUNC_LEAVE(serialized);
}

void *
BULKI_Entity_serialize_to_buffer(BULKI_Entity *entity, void *buffer, size_t *offset)
{
    FUNC_ENTER(NULL);

    // serialize the size
    // note: redundant tree walk — entity->size is already cached after incremental build; could use it
    // directly
    uint64_t size = (uint64_t)get_BULKI_Entity_size(entity);
    *offset += BULKI_vle_encode_uint(size, buffer + *offset);

    // serialize the type_class
    uint8_t type_class = serialize_type_class(entity->pdc_type, entity->pdc_class);
    memcpy(buffer + *offset, &type_class, sizeof(uint8_t));
    *offset += sizeof(uint8_t);

    // serialize the count
    uint64_t count = (uint64_t)(entity->count);
    *offset += BULKI_vle_encode_uint(count, buffer + *offset);

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
        if (entity->pdc_type == PDC_BULKI) { // BULKI
            for (size_t i = 0; i < entity->count; i++) {
                BULKI *bulki = ((BULKI *)entity->data) + i;
                BULKI_serialize_to_buffer(bulki, buffer, offset);
            }
        }
        else if (entity->pdc_type == PDC_BULKI_ENT) { // BULKI_Entity
            for (size_t i = 0; i < entity->count; i++) {
                BULKI_Entity *bulki_entity = ((BULKI_Entity *)entity->data) + i;
                BULKI_Entity_serialize_to_buffer(bulki_entity, buffer, offset);
            }
        }
        else { // all base types
            memcpy(buffer + *offset, entity->data, entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            *offset += (entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
        }
    }
    else {
        LOG_ERROR("unsupported class type %d\n", entity->pdc_class);
        FUNC_LEAVE(NULL);
    }

    FUNC_LEAVE(buffer);
}

void *
BULKI_Entity_serialize(BULKI_Entity *entity, size_t *size)
{
    FUNC_ENTER(NULL);

    // note: redundant tree walk — entity->size is already cached; serialize_to_buffer recomputes per entity
    // below
    size_t estimated_size = get_BULKI_Entity_size(entity);
    void * buffer         = PDC_calloc(1, estimated_size);
    size_t offset         = 0;
    BULKI_Entity_serialize_to_buffer(entity, buffer, &offset);
    if (offset < estimated_size) {
        void *small_buffer = PDC_realloc(buffer, offset);
        if (small_buffer != NULL) {
            buffer = small_buffer;
        }
    }
    *size = offset;

    FUNC_LEAVE(buffer);
}

void *
BULKI_serialize_to_buffer(BULKI *bulki, void *buffer, size_t *offset)
{
    FUNC_ENTER(NULL);

    // serialize the total size
    *offset += BULKI_vle_encode_uint(bulki->totalSize, buffer + *offset);

    // serialize the number of keys
    *offset += BULKI_vle_encode_uint(bulki->numKeys, buffer + *offset);

    // serialize the header size
    *offset += BULKI_vle_encode_uint(bulki->header->headerSize, buffer + *offset);

    // serialize the data size
    *offset += BULKI_vle_encode_uint(bulki->data->dataSize, buffer + *offset);

    // serialize the header
    for (size_t i = 0; i < bulki->numKeys; i++) {
        BULKI_Entity_serialize_to_buffer(&(bulki->header->keys[i]), buffer, offset);
    }

    // serialize the data offset
    uint64_t ofst = (uint64_t)(*offset);
    *offset += BULKI_vle_encode_uint(ofst, buffer + *offset);

    // serialize the data
    for (size_t i = 0; i < bulki->numKeys; i++) {
        BULKI_Entity_serialize_to_buffer(&(bulki->data->values[i]), buffer, offset);
    }

    // serialize the data offset
    ofst = (uint64_t)(*offset);
    *offset += BULKI_vle_encode_uint(ofst, buffer + *offset);

    FUNC_LEAVE(buffer);
}

void *
BULKI_serialize(BULKI *data, size_t *size)
{
    FUNC_ENTER(NULL);

    // note: redundant tree walk — data->totalSize is already cached after BULKI_put_incremental
    size_t estimated_size = get_BULKI_size(data);
    void * buffer         = PDC_calloc(1, estimated_size);
    size_t offset         = 0;
    BULKI_serialize_to_buffer(data, buffer, &offset);
    if (offset < estimated_size) {
        void *small_buffer = PDC_realloc(buffer, offset);
        if (small_buffer != NULL) {
            buffer = small_buffer;
        }
    }
    *size = offset;

    FUNC_LEAVE(buffer);
}

void
BULKI_Entity_serialize_to_file(BULKI_Entity *entity, FILE *fp)
{
    FUNC_ENTER(NULL);

    size_t size;
    // note: buffer-then-fwrite — builds entire entity in memory before a single fwrite; consider streaming to
    // fp
    void *buffer = BULKI_Entity_serialize(entity, &size);
    fwrite(buffer, size, 1, fp);
    buffer = (void *)PDC_free(buffer);
    fclose(fp);

    FUNC_LEAVE_VOID();
}

void
BULKI_serialize_to_file(BULKI *bulki, FILE *fp)
{
    FUNC_ENTER(NULL);

    size_t size = 0;
    // note: buffer-then-fwrite — full checkpoint serialized to RAM first; consider streaming
    // serialize_to_file
    void *buffer = BULKI_serialize(bulki, &size);
    fwrite(buffer, size, 1, fp);
    buffer = (void *)PDC_free(buffer);
    fclose(fp);

    FUNC_LEAVE_VOID();
}

/********************** Deserialize ************************** */

void
deserialize_type_class(uint8_t byte, pdc_c_var_type_t *type, pdc_c_var_class_t *class)
{
    FUNC_ENTER(NULL);

    // Extract the type and class from the byte
    *type  = (pdc_c_var_type_t)(byte & 0x1F);
    *class = (pdc_c_var_class_t)((byte >> 5) & 0x01);

    FUNC_LEAVE_VOID();
}

BULKI_Entity *
BULKI_Entity_deserialize_from_buffer(void *buffer, size_t *offset)
{
    FUNC_ENTER(NULL);

    BULKI_Entity *entity = PDC_malloc(sizeof(BULKI_Entity));
    // deserialize the size
    size_t   bytes_read;
    uint64_t size = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);
    entity->size  = (size_t)size;
    *offset += bytes_read;

    // deserialize the type_class
    uint8_t type_class;
    memcpy(&type_class, buffer + *offset, sizeof(uint8_t));
    *offset += sizeof(uint8_t);
    deserialize_type_class(type_class, &entity->pdc_type, &entity->pdc_class);

    // deserialize the count
    uint64_t count = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);
    entity->count  = (size_t)count;
    *offset += bytes_read;

    // deserialize the data
    if (entity->pdc_class == PDC_CLS_ITEM) {
        if (entity->pdc_type == PDC_BULKI) { // BULKI
            entity->data = BULKI_deserialize_from_buffer(buffer, offset);
        }
        else if (entity->pdc_type == PDC_BULKI_ENT) {
            entity->data = BULKI_Entity_deserialize_from_buffer(buffer, offset);
        }
        else { // all base types
            entity->data = PDC_malloc(entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            memcpy(entity->data, buffer + *offset, entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            *offset += (entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
        }
    }
    else if (entity->pdc_class <= PDC_CLS_ARRAY) {
        if (entity->pdc_type == PDC_BULKI) { // BULKI
            BULKI *bulki_array = PDC_malloc(sizeof(BULKI) * entity->count);
            for (size_t i = 0; i < entity->count; i++) {
                memcpy(bulki_array + i, BULKI_deserialize_from_buffer(buffer, offset), sizeof(BULKI));
            }
            entity->data = bulki_array;
        }
        else if (entity->pdc_type == PDC_BULKI_ENT) { // BULKI_Entity
            BULKI_Entity *bulki_entity_array = PDC_malloc(sizeof(BULKI_Entity) * entity->count);
            for (size_t i = 0; i < entity->count; i++) {
                memcpy(bulki_entity_array + i, BULKI_Entity_deserialize_from_buffer(buffer, offset),
                       sizeof(BULKI_Entity));
            }
            entity->data = bulki_entity_array;
        }
        else { // all base types
            entity->data = PDC_malloc(entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            memcpy(entity->data, buffer + *offset, entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
            *offset += (entity->size - sizeof(uint8_t) * 2 - sizeof(uint64_t) * 2);
        }
    }
    else {
        LOG_ERROR("unsupported class type %d\n", entity->pdc_class);
        FUNC_LEAVE(NULL);
    }

    FUNC_LEAVE(entity);
}

BULKI *
BULKI_deserialize_from_buffer(void *buffer, size_t *offset)
{
    FUNC_ENTER(NULL);

    BULKI *bulki = PDC_malloc(sizeof(BULKI));
    // deserialize the total size
    size_t   bytes_read;
    uint64_t totalSize = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);
    bulki->totalSize   = totalSize;
    *offset += bytes_read;

    // deserialize the number of keys
    uint64_t numKeys = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);
    bulki->numKeys   = numKeys;
    *offset += bytes_read;

    // deserialize the header size
    uint64_t headerSize = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);
    *offset += bytes_read;

    // deserialize the data size
    uint64_t dataSize = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);
    *offset += bytes_read;

    // deserialize the header
    BULKI_Header *header = PDC_malloc(sizeof(BULKI_Header));
    header->keys         = PDC_malloc(sizeof(BULKI_Entity) * numKeys);
    header->headerSize   = headerSize;
    for (size_t i = 0; i < numKeys; i++) {
        memcpy(&(header->keys[i]), BULKI_Entity_deserialize_from_buffer(buffer, offset),
               sizeof(BULKI_Entity));
    }

    // deserialize the data offset
    uint64_t dataOffset = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);
    // check the data offset
    if (((size_t)dataOffset) != *offset) {
        LOG_ERROR("Error1: data offset does not match the expected offset. Expected: %zu, Found: %zu, "
                  "bytes_read: %zu \n",
                  (size_t)dataOffset, *offset, bytes_read);
        FUNC_LEAVE(NULL);
    }
    *offset += bytes_read;

    bulki->header = header;

    // deserialize the data
    BULKI_Data *data = PDC_malloc(sizeof(BULKI_Data));
    data->values     = PDC_malloc(sizeof(BULKI_Entity) * numKeys);
    data->dataSize   = dataSize;
    for (size_t i = 0; i < numKeys; i++) {
        memcpy(&(data->values[i]), BULKI_Entity_deserialize_from_buffer(buffer, offset),
               sizeof(BULKI_Entity));
    }
    // check the total size
    dataOffset = BULKI_vle_decode_uint(buffer + *offset, &bytes_read);

    // check the data offset
    if (((size_t)dataOffset) != *offset) {
        LOG_ERROR("Error2: data offset does not match the expected offset. Expected: %zu, Found: %zu, "
                  "bytes_read: %zu\n",
                  (size_t)dataOffset, *offset, bytes_read);
        FUNC_LEAVE(NULL);
    }
    *offset += bytes_read;

    bulki->data = data;

    FUNC_LEAVE(bulki);
}

BULKI_Entity *
BULKI_Entity_deserialize(void *buffer)
{
    FUNC_ENTER(NULL);

    size_t offset = 0;

    FUNC_LEAVE(BULKI_Entity_deserialize_from_buffer(buffer, &offset));
}

BULKI *
BULKI_deserialize(void *buffer)
{
    FUNC_ENTER(NULL);

    size_t offset = 0;

    FUNC_LEAVE(BULKI_deserialize_from_buffer(buffer, &offset));
}

BULKI_Entity *
BULKI_Entity_deserialize_from_file(FILE *fp)
{
    FUNC_ENTER(NULL);

    fseek(fp, 0, SEEK_END);
    size_t fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET); /* same as rewind(f); */
    // read the file into the buffer
    void *buffer = PDC_malloc(fsize + 1);
    fread(buffer, fsize, 1, fp);
    fclose(fp);
    BULKI_Entity *rst = BULKI_Entity_deserialize(buffer);
    buffer            = (void *)PDC_free(buffer);

    FUNC_LEAVE(rst);
}

BULKI *
BULKI_deserialize_from_file(FILE *fp)
{
    FUNC_ENTER(NULL);

    fseek(fp, 0, SEEK_END);
    size_t fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET); /* same as rewind(f); */
    // read the file into the buffer
    void *buffer = PDC_malloc(fsize + 1);
    fread(buffer, fsize, 1, fp);
    fclose(fp);
    BULKI *rst = BULKI_deserialize(buffer);
    buffer     = (void *)PDC_free(buffer);

    FUNC_LEAVE(rst);
}