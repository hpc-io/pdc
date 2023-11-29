#include "bulki_serde.h"

// clang-format off
/**
 * This function serializes the entire BULKI structure.
 * 
 * The overview of the serialized binary data layout is:
 * +---------------------+---------------------+---------------------+----------------------+----------------------+----------------------+
 * | Size of the Header  |   Size of the Data  |   Header Region     | Data Offset          |   Data Region        | Data Offset          |
 * |     (uint64_t)      |     (uint64_t)      |                     |   (uint64_t)         |                      |   (uint64_t)         |
 * +---------------------+---------------------+---------------------+----------------------+----------------------+----------------------+
 * 
 * The first 3 field is called meta-header, which provides metadata about size of the header region and the size of the data region.
 * Note that the size of the header region doesn't include the 'Number of Keys' field.
 * Also, the size of the data region doesn't include the 'Data Offset' field.
 * 
 * Then the following is the header region with two keys:
 * +----------------------+-------------------------+-----------------------------+---------------------------+--------------------------+-----------------------------+---------------------------+
 * | Number of K-Vs       | Key 1 Type              | Key 1 Size                  | Key 1 Data                | Key 2 Type               | Key 2 Size                  | Key 2 Data                |
 * |   (uint64_t)         | (uint8_t)               | (uint64_t)                  | (Variable size depending  | (uint8_t)                | (uint64_t)                  | (Variable size depending  |
 * |                      |                         |                             | on Key 1 Size)            |                          |                             | on Key 2 Size)            |
 * +----------------------+-------------------------+-----------------------------+---------------------------+--------------------------+-----------------------------+---------------------------+
 * 
 * Then, the following is the layout of the data region with the final offset validation point.
 *
 * |----------------------------------------------------------------------------------------------------------------|
 * | Number of K-V Pairs (uint64_t)     | Value 1 Class (uint8_t) | Value 1 Type (uint8_t) | Value 1 Size (uint64_t)|
 * |----------------------------------------------------------------------------------------------------------------|
 * | Value 1 Data (Variable size depending on Value 1 Size) | Value 2 Class (uint8_t)      | Value 2 Type (uint8_t) |
 * |----------------------------------------------------------------------------------------------------------------|
 * | Value 2 Size (uint64_t) | Value 2 Data (Variable size depending on Value 2 Size)                               |
 * |----------------------------------------------------------------------------------------------------------------|
 * | ...repeated for the number of value entries in the data...                                                     |
 * |----------------------------------------------------------------------------------------------------------------|
 * | Final Data Offset (uint64_t)                                                                                   |
 * |----------------------------------------------------------------------------------------------------------------|
 * 
 * Please refer to `get_size_by_class_n_type` function in pdc_generic.h for size calculation on scalar values and array values.
 *
 */
// clang-format on
void *
BULKI_serialize(BULKI *data)
{
    // The buffer contains:
    // the size of the header (size_t) +
    // the size of the data (size_t) +
    // the number of keys (size_t) +
    // the header region +
    // the data offset (size_t) +
    // the number of value entries (size_t) +
    // the data region
    void *buffer = malloc(get_BULKI_size(data, 1, PDC_BULKI, PDC_CLS_ITEM));
    // serialize the meta header, which contains only the size of the header and the size of the data region.
    memcpy(buffer, &data->header->headerSize, sizeof(size_t));
    memcpy(buffer + sizeof(size_t), &data->data->dataSize, sizeof(size_t));
    // serialize the header
    // start with the number of keys
    memcpy(buffer + sizeof(size_t) * 2, &data->numKeys, sizeof(size_t));

    // then the keys
    size_t offset = sizeof(size_t) * 3;
    for (size_t i = 0; i < data->numKeys; i++) {
        int8_t pdc_type = (int8_t)(data->header->keys[i].pdc_type);
        memcpy(buffer + offset, &pdc_type, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(buffer + offset, &data->header->keys[i].size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(buffer + offset, data->header->keys[i].key, data->header->keys[i].size);
        offset += data->header->keys[i].size;
    }

    // serialize the data offset, this is for validation purpose to see if header region is corrupted.
    memcpy(buffer + offset, &offset, sizeof(size_t));
    offset += sizeof(size_t);

    // serialize the data
    // start with the number of value entries
    memcpy(buffer + offset, &data->numKeys, sizeof(size_t));
    offset += sizeof(size_t);
    // then the values
    for (size_t i = 0; i < data->numKeys; i++) {
        int8_t pdc_class = (int8_t)data->data->values[i].pdc_class;
        int8_t pdc_type  = (int8_t)data->data->values[i].pdc_type;
        memcpy(buffer + offset, &pdc_class, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(buffer + offset, &pdc_type, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(buffer + offset, &data->data->values[i].size, sizeof(size_t));
        offset += sizeof(size_t);

        if (data->data->values[i].pdc_class == PDC_BULKI) {
            void *sdata = BULKI_serde_serialize((BULKI *)(data->data->values[i].data));
            memcpy(buffer + offset, sdata, data->data->values[i].size);
        }
        else if (data->data->values[i].pdc_class <= PDC_CLS_ARRAY) {
            memcpy(buffer + offset, data->data->values[i].data, data->data->values[i].size);
        }
        else {
            printf("Error: unsupported class type %d\n", data->data->values[i].pdc_class);
            return NULL;
        }

        offset += data->data->values[i].size;
        memcpy(buffer + offset, data->data->values[i].data, data->data->values[i].size);
        offset += data->data->values[i].size;
    }
    // serialize the data offset again, this is for validation purpose to see if data region is corrupted.
    memcpy(buffer + offset, &offset, sizeof(size_t));
    offset += sizeof(size_t);
    return buffer;
}

BULKI *
BULKI_serde_deserialize(void *buffer)
{
    BULKI *bulki  = malloc(sizeof(BULKI));
    size_t offset = 0;
    // read the meta header
    size_t headerSize;
    size_t dataSize;
    memcpy(&headerSize, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(&dataSize, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);

    printf("headerSize: %zu\n", headerSize);
    printf("dataSize: %zu\n", dataSize);

    // read the header
    size_t numKeys;
    memcpy(&numKeys, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);
    bulki->numKeys = numKeys;

    printf("numKeys: %zu\n", numKeys);

    BULKI_Header *header = malloc(sizeof(BULKI_Header));
    header->keys         = malloc(sizeof(BULKI_Entity) * numKeys);
    header->headerSize   = headerSize;

    printf("iterating %zu keys in the header\n", numKeys);

    for (size_t i = 0; i < numKeys; i++) {
        int8_t pdc_type;
        size_t size;
        memcpy(&pdc_type, buffer + offset, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(&size, buffer + offset, sizeof(size_t));
        offset += sizeof(size_t);
        void *key = malloc(size);
        memcpy(key, buffer + offset, size);
        offset += size;
        header->keys[i].data     = key;
        header->keys[i].pdc_type = (pdc_c_var_type_t)pdc_type;
        header->keys[i].size     = size;

        printf("key %zu: %s, size: %zu, type: %s\n", i, (char *)key, size, get_name_by_dtype(pdc_type));
    }

    // read the data offset
    size_t dataOffset;
    memcpy(&dataOffset, buffer + offset, sizeof(size_t));
    // check the data offset
    if (dataOffset != offset) {
        printf("Error: data offset does not match the expected offset.\n");
        return NULL;
    }
    offset += sizeof(size_t);

    // read the data
    size_t numValues;
    memcpy(&numValues, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);
    BULKI_Data *data = malloc(sizeof(BULKI_Data));
    data->values     = malloc(sizeof(BULKI_Entity) * numValues);
    data->dataSize   = dataSize;
    for (size_t i = 0; i < numValues; i++) {
        int8_t pdc_class;
        int8_t pdc_type;
        size_t size;
        memcpy(&pdc_class, buffer + offset, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(&pdc_type, buffer + offset, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(&size, buffer + offset, sizeof(size_t));
        offset += sizeof(size_t);
        void *value = malloc(size);
        memcpy(value, buffer + offset, size);
        offset += size;

        // TODO: postponed deserialization of struct data, need to be finished here.
        data->values[i].pdc_class = (pdc_c_var_class_t)pdc_class;
        data->values[i].pdc_type  = (pdc_c_var_type_t)pdc_type;
        data->values[i].size      = size;
        data->values[i].data      = value;
        printf("value %zu: size: %zu, type: %s\n", i, size, get_name_by_dtype(pdc_type));
    }
    // check the total size
    memcpy(&dataOffset, buffer + offset, sizeof(size_t));
    // check the data offset
    if (dataOffset != offset) {
        printf("Error: data offset does not match the expected offset.\n");
        return NULL;
    }
    offset += sizeof(size_t);
    if (offset != headerSize + sizeof(size_t) * 6 + dataSize) {
        printf("Error: total size does not match the expected size.\n");
        return NULL;
    }
    // create the serialized data
    bulki->header    = header;
    bulki->data      = data;
    bulki->totalSize = headerSize + dataSize + sizeof(size_t) * 6;

    return bulki;
}
