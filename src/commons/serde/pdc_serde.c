#include "pdc_serde.h"

PDC_SERDE_SerializedData *
pdc_serde_init(int initial_field_count)
{
    PDC_SERDE_SerializedData *data = malloc(sizeof(PDC_SERDE_SerializedData));
    data->header                   = malloc(sizeof(PDC_SERDE_Header));
    data->header->keys             = malloc(sizeof(PDC_SERDE_Key) * initial_field_count);
    data->header->numKeys          = 0;
    data->header->totalSize        = 0;
    data->data                     = malloc(sizeof(PDC_SERDE_Data));
    data->data->values             = malloc(sizeof(PDC_SERDE_Value) * initial_field_count);
    data->data->numValues          = 0;
    data->data->totalSize          = 0;
    return data;
}

void
pdc_serde_append_key_value(PDC_SERDE_SerializedData *data, PDC_SERDE_Key *key, PDC_SERDE_Value *value)
{
    data->header->keys[data->header->numKeys] = *key;
    data->header->numKeys++;
    // append type, size, and key
    data->header->totalSize += (sizeof(int) + sizeof(size_t) + key->size);
    data->data->values[data->data->numValues] = *value;
    data->data->numValues++;
    // append class, type, size, and data
    data->data->totalSize += (sizeof(int) + sizeof(int) + sizeof(size_t) + value->size);
}

size_t get_total_size_for_serialized_data(PDC_SERDE_SerializedData *data) {
    if (data->totalSize <= 0) {
        size_t total_size = data->header->totalSize + data->data->totalSize + sizeof(size_t) * 6;
        data->totalSize = total_size;
    }
    return data->totalSize;
}

// clang-format off
/**
 * This function serializes the entire PDC_SERDE_SerializedData structure.
 * 
 * The overview of the serialized binary data layout is:
 * +---------------------+---------------------+----------------------+---------------------+----------------------+----------------------+----------------------+----------------------+
 * | Size of the Header  |   Size of the Data  | Number of Keys       |   Header Region     | Data Offset          | Number of Values     |   Data Region        | Data Offset          |
 * |     (size_t)        |     (size_t)        |   (size_t)           |                     |   (size_t)           |   (size_t)           |                      |   (size_t)           |
 * +---------------------+---------------------+----------------------+---------------------+----------------------+----------------------+----------------------+----------------------+
 * 
 * The first 2 field is called meta-header, which provides metadata about size of the header region and the size of the data region.
 * Note that the size of the header region doesn't include the 'Number of Keys' field.
 * Also, the size of the data region doesn't include the 'Data Offset' field.
 * 
 * Then the following is the header region with two keys:
 * +-----------------------+-------------------------+-----------------------------+---------------------------+--------------------------+-----------------------------+---------------------------+
 * | Number of Keys        | Key 1 Type              | Key 1 Size                  | Key 1 Data                | Key 2 Type               | Key 2 Size                  | Key 2 Data                |
 * | (size_t)              | (int8_t)                | (size_t)                    | (Variable size depending  | (int8_t)                 | (size_t)                    | (Variable size depending  |
 * |                       |                         |                             | on Key 1 Size)            |                          |                             | on Key 2 Size)            |
 * +-----------------------+-------------------------+-----------------------------+---------------------------+--------------------------+-----------------------------+---------------------------+
 * 
 * Then, the following is a header offset validation point and the data region with the final offset validation point.
 *
 * |----------------------------------------------------------------------------------------------------------|
 * | Data Offset (size_t) | Number of Value Entries (size_t) | Value 1 Class (int8_t) | Value 1 Type (int8_t) |
 * |----------------------------------------------------------------------------------------------------------|
 * | Value 1 Size (size_t)| Value 1 Data (Variable size depending on Value 1 Size) | Value 2 Class (int8_t)   |
 * |----------------------------------------------------------------------------------------------------------|
 * | Value 2 Type (int8_t)| Value 2 Size (size_t) | Value 2 Data (Variable size depending on Value 2 Size)    |
 * |----------------------------------------------------------------------------------------------------------|
 * | ...repeated for the number of value entries in the data...                                               |
 * |----------------------------------------------------------------------------------------------------------|
 * | Final Data Offset (size_t)                                                                               |
 * |----------------------------------------------------------------------------------------------------------|
 * 
 * Please refer to `get_size_by_class_n_type` function in pdc_generic.h for size calculation on scalar values and array values.
 *
 */
// clang-format on
void *
pdc_serde_serialize(PDC_SERDE_SerializedData *data)
{
    // The buffer contains:
    // the size of the header (size_t) +
    // the size of the data (size_t) +
    // the number of keys (size_t) +
    // the header region +
    // the data offset (size_t) +
    // the number of value entries (size_t) +
    // the data region
    void *buffer = malloc(get_total_size_for_serialized_data(data));
    // serialize the meta header, which contains only the size of the header and the size of the data region.
    memcpy(buffer, &data->header->totalSize, sizeof(size_t));
    memcpy(buffer + sizeof(size_t), &data->data->totalSize, sizeof(size_t));

    // serialize the header
    // start with the number of keys
    memcpy(buffer + sizeof(size_t) * 2, &data->header->numKeys, sizeof(size_t));
    // then the keys
    size_t offset = sizeof(size_t) * 3;
    for (int i = 0; i < data->header->numKeys; i++) {
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
    memcpy(buffer + offset, &data->data->numValues, sizeof(size_t));
    offset += sizeof(size_t);
    // then the values
    for (int i = 0; i < data->data->numValues; i++) {
        int8_t pdc_class = (int8_t)data->data->values[i].pdc_class;
        int8_t pdc_type  = (int8_t)data->data->values[i].pdc_type;
        memcpy(buffer + offset, &pdc_class, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(buffer + offset, &pdc_type, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(buffer + offset, &data->data->values[i].size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(buffer + offset, data->data->values[i].data, data->data->values[i].size);
        offset += data->data->values[i].size;
    }
    // serialize the data offset again, this is for validation purpose to see if data region is corrupted.
    memcpy(buffer + offset, &offset, sizeof(size_t));
    offset += sizeof(size_t);
    return buffer;
}

PDC_SERDE_SerializedData *
pdc_serde_deserialize(void *buffer)
{
    size_t offset = 0;
    // read the meta header
    size_t headerSize;
    size_t dataSize;
    memcpy(&headerSize, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(&dataSize, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);

    // read the header
    size_t numKeys;
    memcpy(&numKeys, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);
    PDC_SERDE_Header *header = malloc(sizeof(PDC_SERDE_Header));
    header->keys             = malloc(sizeof(PDC_SERDE_Key) * numKeys);
    header->numKeys          = numKeys;
    header->totalSize        = headerSize;
    for (int i = 0; i < numKeys; i++) {
        int8_t pdc_type;
        size_t size;
        memcpy(&pdc_type, buffer + offset, sizeof(int8_t));
        offset += sizeof(int8_t);
        memcpy(&size, buffer + offset, sizeof(size_t));
        offset += sizeof(size_t);
        void *key = malloc(size);
        memcpy(key, buffer + offset, size);
        offset += size;
        header->keys[i].key      = key;
        header->keys[i].pdc_type = (PDC_CType)pdc_type;
        header->keys[i].size     = size;
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
    PDC_SERDE_Data *data = malloc(sizeof(PDC_SERDE_Data));
    data->values         = malloc(sizeof(PDC_SERDE_Value) * numValues);
    data->numValues      = numValues;
    data->totalSize      = dataSize;
    for (int i = 0; i < numValues; i++) {
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
        data->values[i].data      = value;
        data->values[i].pdc_class = (PDC_CType_Class)pdc_class;
        data->values[i].pdc_type  = (PDC_CType)pdc_type;
        data->values[i].size      = size;
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
    PDC_SERDE_SerializedData *serializedData = malloc(sizeof(PDC_SERDE_SerializedData));
    serializedData->header                   = header;
    serializedData->data                     = data;
    serializedData->totalSize                = headerSize + dataSize + sizeof(size_t) * 6;

    return serializedData;
}

void
pdc_serde_free(PDC_SERDE_SerializedData *data)
{
    for (int i = 0; i < data->header->numKeys; i++) {
        free(data->header->keys[i].key);
    }
    free(data->header->keys);
    for (int i = 0; i < data->data->numValues; i++) {
        free(data->data->values[i].data);
    }
    free(data->data->values);
    free(data->header);
    free(data->data);
    free(data);
}

void
pdc_serde_print(PDC_SERDE_SerializedData *data)
{
    printf("Header:\n");
    printf("  numKeys: %zu\n", data->header->numKeys);
    printf("  totalSize: %zu\n", data->header->totalSize);
    for (int i = 0; i < data->header->numKeys; i++) {
        printf("  key %d:\n", i);
        printf("    type: %d\n", data->header->keys[i].pdc_type);
        printf("    size: %zu\n", data->header->keys[i].size);
        printf("    key: %s\n", (char *)data->header->keys[i].key);
    }
    printf("Data:\n");
    printf("  numValues: %zu\n", data->data->numValues);
    printf("  totalSize: %zu\n", data->data->totalSize);
    for (int i = 0; i < data->data->numValues; i++) {
        printf("  value %d:\n", i);
        printf("    class: %d\n", data->data->values[i].pdc_class);
        printf("    type: %d\n", data->data->values[i].pdc_type);
        printf("    size: %zu\n", data->data->values[i].size);
        printf("    data: ");
        if (data->data->values[i].pdc_class == PDC_STRING) {
            printf("%s\n", (char *)data->data->values[i].data);
        }
        else {
            printf("\n");
        }
    }
}

int
test_serde_framework()
{
    // Initialize a serialized data structure
    PDC_SERDE_SerializedData *data = pdc_serde_init(5);

    // Create and append key-value pairs for different data types
    char *           intKey_str = "int";
    int              intVal     = 42;
    PDC_SERDE_Key *  intKey     = PDC_SERDE_KEY(intKey_str, PDC_STRING, sizeof(intKey_str));
    PDC_SERDE_Value *intValue   = PDC_SERDE_VALUE(&intVal, PDC_INT, PDC_CLS_SCALAR, sizeof(int));
    pdc_serde_append_key_value(data, intKey, intValue);

    char *           doubleKey_str = "double";
    double           doubleVal     = 3.14159;
    PDC_SERDE_Key *  doubleKey     = PDC_SERDE_KEY(doubleKey_str, PDC_STRING, sizeof(doubleKey_str));
    PDC_SERDE_Value *doubleValue =
        PDC_SERDE_VALUE(&doubleVal, PDC_DOUBLE, PDC_CLS_SCALAR, sizeof(double));
    pdc_serde_append_key_value(data, doubleKey, doubleValue);

    char *         strKey_str = "string";
    char *         strVal     = "Hello, World!";
    PDC_SERDE_Key *strKey = PDC_SERDE_KEY(strKey_str, PDC_STRING, (strlen(strKey_str) + 1) * sizeof(char));
    PDC_SERDE_Value *strValue =
        PDC_SERDE_VALUE(strVal, PDC_STRING, PDC_CLS_SCALAR, (strlen(strVal) + 1) * sizeof(char));
    pdc_serde_append_key_value(data, strKey, strValue);

    char *           arrayKey_str = "array";
    int              intArray[3]  = {1, 2, 3};
    PDC_SERDE_Key *  arrayKey     = PDC_SERDE_KEY(arrayKey_str, PDC_STRING, sizeof(arrayKey_str));
    PDC_SERDE_Value *arrayValue   = PDC_SERDE_VALUE(intArray, PDC_INT, PDC_CLS_ARRAY, sizeof(int) * 3);
    pdc_serde_append_key_value(data, arrayKey, arrayValue);

    typedef struct {
        int x;
        int y;
    } Point;

    char *pointKey = "point";
    Point pointVal = {10, 20};

    PDC_SERDE_SerializedData *point_data = pdc_serde_init(2);
    PDC_SERDE_Key *           x_name     = PDC_SERDE_KEY("x", PDC_STRING, sizeof(char *));
    PDC_SERDE_Value *x_value = PDC_SERDE_VALUE(&pointVal.x, PDC_INT, PDC_CLS_SCALAR, sizeof(int));

    PDC_SERDE_Key *  y_name  = PDC_SERDE_KEY("y", PDC_STRING, sizeof(char *));
    PDC_SERDE_Value *y_value = PDC_SERDE_VALUE(&pointVal.y, PDC_INT, PDC_CLS_SCALAR, sizeof(int));

    pdc_serde_append_key_value(point_data, x_name, x_value);
    pdc_serde_append_key_value(point_data, y_name, y_value);
    void *point_buffer = pdc_serde_serialize(point_data);

    PDC_SERDE_Key *  structKey = PDC_SERDE_KEY(pointKey, PDC_STRING, sizeof(pointKey));
    PDC_SERDE_Value *structValue =
        PDC_SERDE_VALUE(point_buffer, PDC_VOID_PTR, PDC_CLS_STRUCT, sizeof(Point));
    pdc_serde_append_key_value(data, structKey, structValue);

    // Serialize the data
    void *buffer = pdc_serde_serialize(data);

    // Deserialize the buffer
    PDC_SERDE_SerializedData *deserializedData = pdc_serde_deserialize(buffer);

    // Print the deserialized data
    pdc_serde_print(deserializedData);

    // Free the memory
    pdc_serde_free(data);
    pdc_serde_free(deserializedData);
    free(buffer);

    return 0;
}