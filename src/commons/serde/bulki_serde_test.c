#include "bulki_serde.h"

int
test_base_type()
{
    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(5);

    // Create and append key-value pairs for different data types
    char *        intKey_str = "int";
    int           intVal     = 42;
    BULKI_Entity *intKey     = BULKI_ENTITY(intKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *intValue   = BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM);
    BULKI_add(bulki, intKey, intValue);

    char *        doubleKey_str = "double";
    double        doubleVal     = 3.14159;
    BULKI_Entity *doubleKey     = BULKI_ENTITY(doubleKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *doubleValue   = BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM);
    BULKI_add(bulki, doubleKey, doubleValue);

    char *        strKey_str = "string";
    char *        strVal     = "Hello, World!";
    BULKI_Entity *strKey     = BULKI_ENTITY(strKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *strValue   = BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_add(bulki, strKey, strValue);

    // Serialize the data
    void *buffer = BULKI_serialize(bulki);

    printf("Serialized data:\n");
    BULKI_print(bulki);

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer);

    printf("Deserialized data:\n");

    // Print the deserialized data
    BULKI_print(deserializedBulki);

    // Free the memory
    BULKI_free(bulki);
    BULKI_free(deserializedBulki);
    free(buffer);

    return 0;
}

int
test_base_array_entitiy()
{

    return 0;
}

int
test_embedded_entitiy()
{
    return 0;
}

int
main(int argc, char *argv[])
{
    printf("test_base_type RST = %d\n", test_base_type());
    printf("test_base_array_entitiy RST = %d\n", test_base_array_entitiy());
    printf("test_embedded_entitiy RST = %d\n", test_embedded_entitiy());
}