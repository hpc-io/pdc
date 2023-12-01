#include "bulki_serde.h"

int
test_base_type()
{
    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(2);

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

    // printf("Serialized data:\n");
    // BULKI_print(bulki);

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer);

    // printf("Deserialized data:\n");
    // BULKI_print(deserializedBulki);

    int equal = BULKI_equal(bulki, deserializedBulki);
    printf("bulki == deserializedBulki: %d\n", equal);

    // Free the memory
    BULKI_free(deserializedBulki, 1);
    // printf("Freed deserializedBulki\n");
    BULKI_free(bulki, 1);
    // printf("Freed bulki\n");
    free(buffer);

    return equal;
}

int
test_base_array_entitiy()
{
    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(2);

    // Create and append key-value pairs for different data types
    char *        intKey_str = "int";
    int           intVal     = 42;
    uint64_t      intObjID   = 12416574651687;
    BULKI_Entity *intKey     = BULKI_ENTITY(intKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *intArr     = empty_BULKI_Entity(PDC_BULKI_ENT, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_add(bulki, intKey, intArr);

    char *        doubleKey_str = "double";
    double        doubleVal     = 3.14159;
    uint64_t      doubleObjID   = 564987951987494;
    BULKI_Entity *doubleKey     = BULKI_ENTITY(doubleKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *doubleArr     = empty_BULKI_Entity(PDC_BULKI_ENT, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_add(bulki, doubleKey, doubleArr);

    char *strKey_str = "string";
    char *strVal     = "Hello, World!";

    BULKI_Entity *strKey = BULKI_ENTITY(strKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *strArr = empty_BULKI_Entity(PDC_BULKI_ENT, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_add(bulki, strKey, strArr);

    char *        mixedKey_str = "mixed";
    BULKI_Entity *mixedKey     = BULKI_ENTITY(mixedKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *mixedArr     = empty_BULKI_Entity(PDC_BULKI_ENT, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_add(bulki, mixedKey, mixedArr);

    // Initialize a serialized data structure
    BULKI *bulki2 = BULKI_init(2);

    // Create and append key-value pairs for different data types
    char *        intKey_str2 = "int";
    int           intVal2     = 42;
    BULKI_Entity *intKey2     = BULKI_ENTITY(intKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *intArr2     = empty_BULKI_Entity(PDC_BULKI, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI(intArr2, bulki);
    BULKI_ENTITY_append_BULKI(intArr2, bulki);
    BULKI_add(bulki2, intKey2, intArr2);

    // Serialize the data
    void *buffer = BULKI_serialize(bulki2);

    // printf("Serialized data:\n");
    // BULKI_print(bulki2);

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer);

    // printf("Deserialized data:\n");
    // BULKI_print(deserializedBulki);

    int equal = BULKI_equal(bulki2, deserializedBulki);
    printf("bulki2 == deserializedBulki: %d\n", equal);

    // Free the memory
    BULKI_free(deserializedBulki, 1);
    // printf("Freed deserializedBulki\n");
    BULKI_free(bulki, 1);
    // printf("Freed bulki\n");
    free(buffer);

    return equal;
}

int
test_embedded_entitiy()
{
    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(2);

    // Create and append key-value pairs for different data types
    char *        intKey_str = "int";
    int           intVal     = 42;
    uint64_t      intObjID   = 12416574651687;
    BULKI_Entity *intKey     = BULKI_ENTITY(intKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *intArr     = empty_BULKI_Entity(PDC_BULKI_ENT, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_add(bulki, intKey, intArr);

    char *        doubleKey_str = "double";
    double        doubleVal     = 3.14159;
    uint64_t      doubleObjID   = 564987951987494;
    BULKI_Entity *doubleKey     = BULKI_ENTITY(doubleKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *doubleArr     = empty_BULKI_Entity(PDC_BULKI_ENT, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_add(bulki, doubleKey, doubleArr);

    char *strKey_str = "string";
    char *strVal     = "Hello, World!";

    BULKI_Entity *strKey = BULKI_ENTITY(strKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *strArr = empty_BULKI_Entity(PDC_BULKI_ENT, PDC_CLS_ARRAY);
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_add(bulki, strKey, strArr);

    // Serialize the data
    void *buffer = BULKI_serialize(bulki);

    // printf("Serialized data:\n");
    // BULKI_print(bulki);

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer);

    // printf("Deserialized data:\n");
    // BULKI_print(deserializedBulki);

    int equal = BULKI_equal(bulki, deserializedBulki);
    printf("bulki == deserializedBulki: %d\n", equal);

    // Free the memory
    BULKI_free(deserializedBulki, 1);
    // printf("Freed deserializedBulki\n");
    BULKI_free(bulki, 1);
    // printf("Freed bulki\n");
    free(buffer);

    return equal;
}

int
main(int argc, char *argv[])
{
    printf("test_base_type RST = %d\n", test_base_type());
    printf("test_base_array_entitiy RST = %d\n", test_base_array_entitiy());
    printf("test_embedded_entitiy RST = %d\n", test_embedded_entitiy());
}