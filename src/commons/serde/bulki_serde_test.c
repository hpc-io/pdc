#include "bulki_serde.h"

int
test_serde_framework()
{
    // Initialize a serialized data structure
    BULKI *data = BULKI_init(5);

    // Create and append key-value pairs for different data types
    char *        intKey_str = "int";
    int           intVal     = 42;
    BULKI_Entity *intKey     = BULKI_ENTITY(intKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *intValue   = BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM);
    BULKI_add(data, intKey, intValue);

    char *        doubleKey_str = "double";
    double        doubleVal     = 3.14159;
    BULKI_Entity *doubleKey     = BULKI_ENTITY(doubleKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *doubleValue   = BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM);
    BULKI_add(data, doubleKey, doubleValue);

    char *        strKey_str = "string";
    char *        strVal     = "Hello, World!";
    BULKI_Entity *strKey     = BULKI_ENTITY(strKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *strValue   = BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_add(data, strKey, strValue);

    // Serialize the data
    void *buffer = BULKI_serialize(data);

    printf("Serialized data:\n");
    BULKI_print(data, 1, PDC_BULKI, PDC_CLS_ITEM);

    // Deserialize the buffer
    BULKI *deserializedData = BULKI_deserialize(buffer);

    printf("Deserialized data:\n");

    // Print the deserialized data
    BULKI_print(deserializedData, 1, PDC_BULKI, PDC_CLS_ITEM);

    // Free the memory
    BULKI_free(data);
    BULKI_free(deserializedData);
    free(buffer);

    return 0;
}

int
main(int argc, char *argv[])
{
    return test_serde_framework();
}