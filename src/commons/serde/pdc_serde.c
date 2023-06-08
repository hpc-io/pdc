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
    void *buffer = malloc(data->header->totalSize + data->data->totalSize + sizeof(size_t) * 5);
    // serialize the meta header, which contains only the size of the header and the size of the data region.
    memcpy(buffer, &data->header->totalSize, sizeof(size_t));
    memcpy(buffer + sizeof(size_t), &data->data->totalSize, sizeof(size_t));

    // serialize the header
    // start with the number of keys
    memcpy(buffer + sizeof(size_t) * 2, &data->header->numKeys, sizeof(size_t));
    // then the keys
    size_t offset = sizeof(size_t) * 3;
    for (int i = 0; i < data->header->numKeys; i++) {
        int type = (int)(data->header->keys[i].type);
        memcpy(buffer + offset, &type, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &data->header->keys[i].size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(buffer + offset, data->header->keys[i].key, data->header->keys[i].size);
        offset += data->header->keys[i].size;
    }

    // serialize the data offset
    memcpy(buffer + offset, &offset, sizeof(size_t));
    offset += sizeof(size_t);

    // serialize the data
    // start with the number of value entries
    memcpy(buffer + offset, &data->data->numValues, sizeof(size_t));
    offset += sizeof(size_t);
    // then the values
    for (int i = 0; i < data->data->numValues; i++) {
        int class = (int)data->data->values[i].class;
        int type = (int)data->data->values[i].type;
        memcpy(buffer + offset, &class, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &type, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &data->data->values[i].size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(buffer + offset, data->data->values[i].data, data->data->values[i].size);
        offset += data->data->values[i].size;
    }
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
        int type;
        size_t size;
        memcpy(&type, buffer + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&size, buffer + offset, sizeof(size_t));
        offset += sizeof(size_t);
        char *key = malloc(size);
        memcpy(key, buffer + offset, size);
        offset += size;
        header->keys[i].key         = key;
        header->keys[i].type        = (PDC_CType)type;
        header->keys[i].size        = size;
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
        int class;
        int type;
        size_t size;
        memcpy(&class, buffer + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&type, buffer + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&size, buffer + offset, sizeof(size_t));
        offset += sizeof(size_t);
        void *value = malloc(size);
        memcpy(value, buffer + offset, size);
        offset += size;
        data->values[i].data  = value;
        data->values[i].class = (PDC_CTypeClass)class;
        data->values[i].type  = (PDC_CType)type;
        data->values[i].size  = size;
    }
    // check the total size
    if (offset != headerSize + sizeof(size_t) * 5 + dataSize) {
        printf("Error: total size does not match the expected size.\n");
        return NULL;
    }
    // create the serialized data
    PDC_SERDE_SerializedData *serializedData = malloc(sizeof(PDC_SERDE_SerializedData));
    serializedData->header                   = header;
    serializedData->data                     = data;
    serializedData->totalSize                = headerSize + dataSize + sizeof(size_t) * 5;

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
        printf("    type: %d\n", data->header->keys[i].type);
        printf("    size: %zu\n", data->header->keys[i].size);
        printf("    key: %s\n", (char *)data->header->keys[i].key);
    }
    printf("Data:\n");
    printf("  numValues: %zu\n", data->data->numValues);
    printf("  totalSize: %zu\n", data->data->totalSize);
    for (int i = 0; i < data->data->numValues; i++) {
        printf("  value %d:\n", i);
        printf("    class: %d\n", data->data->values[i].class);
        printf("    type: %d\n", data->data->values[i].type);
        printf("    size: %zu\n", data->data->values[i].size);
        printf("    data: ");
        if (data->data->values[i].class == PDC_CT_STRING) {
            printf("%s\n", (char *)data->data->values[i].data);
        }
        else {
            printf("\n");
        }
    }
}

int test_serde_framework() {
    // Initialize a serialized data structure
    PDC_SERDE_SerializedData* data = pdc_serde_init(5);

    // Create and append key-value pairs for different data types
    char *intKey_str = "int";
    int intVal = 42;
    PDC_SERDE_Key* intKey = PDC_SERDE_KEY(intKey_str, PDC_CT_STRING, sizeof(intKey_str));
    PDC_SERDE_Value* intValue = PDC_SERDE_VALUE(&intVal, PDC_CT_INT, PDC_CT_CLS_SCALAR, sizeof(int));
    pdc_serde_append_key_value(data, intKey, intValue);

    char *doubleKey_str = "double";
    double doubleVal = 3.14159;
    PDC_SERDE_Key* doubleKey = PDC_SERDE_KEY(doubleKey_str, PDC_CT_STRING, sizeof(doubleKey_str));
    PDC_SERDE_Value* doubleValue = PDC_SERDE_VALUE(&doubleVal, PDC_CT_DOUBLE, PDC_CT_CLS_SCALAR, sizeof(double));
    pdc_serde_append_key_value(data, doubleKey, doubleValue);

    char *strKey_str = "string";
    char* strVal = "Hello, World!";
    PDC_SERDE_Key* strKey = PDC_SERDE_KEY(strKey_str, PDC_CT_STRING, (strlen(strKey_str) + 1) * sizeof(char));
    PDC_SERDE_Value* strValue = PDC_SERDE_VALUE(strVal, PDC_CT_STRING, PDC_CT_CLS_SCALAR, (strlen(strVal) + 1) * sizeof(char));
    pdc_serde_append_key_value(data, strKey, strValue);

    char *arrayKey_str = "array";
    int intArray[3] = {1, 2, 3};
    PDC_SERDE_Key* arrayKey = PDC_SERDE_KEY(arrayKey_str, PDC_CT_STRING, sizeof(arrayKey_str));
    PDC_SERDE_Value* arrayValue = PDC_SERDE_VALUE(intArray, PDC_CT_INT, PDC_CT_CLS_ARRAY, sizeof(int) * 3);
    pdc_serde_append_key_value(data, arrayKey, arrayValue);

    typedef struct {
        int x;
        int y;
    } Point;

    char *pointKey = "point";
    Point pointVal = {10, 20};

    PDC_SERDE_SerializedData* point_data = pdc_serde_init(2);
    PDC_SERDE_Key* x_name = PDC_SERDE_KEY("x", PDC_CT_STRING, sizeof(char *));
    PDC_SERDE_Value* x_value = PDC_SERDE_VALUE(&pointVal.x, PDC_CT_INT, PDC_CT_CLS_SCALAR, sizeof(int));

    PDC_SERDE_Key* y_name = PDC_SERDE_KEY("y", PDC_CT_STRING, sizeof(char *));
    PDC_SERDE_Value* y_value = PDC_SERDE_VALUE(&pointVal.y, PDC_CT_INT, PDC_CT_CLS_SCALAR, sizeof(int));

    pdc_serde_append_key_value(point_data, x_name, x_value);
    pdc_serde_append_key_value(point_data, y_name, y_value);
    void *point_buffer = pdc_serde_serialize(point_data);


    PDC_SERDE_Key* structKey = PDC_SERDE_KEY(pointKey, PDC_CT_STRING, sizeof(pointKey));
    PDC_SERDE_Value* structValue = PDC_SERDE_VALUE(point_buffer, PDC_CT_VOID_PTR, PDC_CT_CLS_STRUCT, sizeof(Point));
    pdc_serde_append_key_value(data, structKey, structValue);

    // Serialize the data
    void* buffer = pdc_serde_serialize(data);

    // Deserialize the buffer
    PDC_SERDE_SerializedData* deserializedData = pdc_serde_deserialize(buffer);

    // Print the deserialized data
    pdc_serde_print(deserializedData);

    // Free the memory
    pdc_serde_free(data);
    pdc_serde_free(deserializedData);
    free(buffer);

    return 0;
}