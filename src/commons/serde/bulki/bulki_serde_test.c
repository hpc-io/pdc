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
    BULKI_put(bulki, intKey, intValue);

    int *intArrVal          = (int *)malloc(3 * sizeof(int));
    intArrVal[0]            = 9; // x
    intArrVal[1]            = 8; // y
    intArrVal[2]            = 7; // z
    BULKI_Entity *intArrKey = BULKI_ENTITY(intArrVal, 3, PDC_INT, PDC_CLS_ARRAY);
    // Note that if you already inserted a BULKI_Entity into a BULKI or another BULKI_Entity,
    // you should not reuse it again, rather, you should create a new BULKI_Entity. Otherwise, there will be
    // freeing issue.
    BULKI_put(bulki, intArrKey, BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ARRAY));

    char *        doubleKey_str = "double";
    double        doubleVal     = 3.14159;
    BULKI_Entity *doubleKey     = BULKI_ENTITY(doubleKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *doubleValue   = BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM);
    BULKI_put(bulki, doubleKey, doubleValue);

    char *        strKey_str = "string";
    char *        strVal     = "Hello, World!";
    BULKI_Entity *strKey     = BULKI_ENTITY(strKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *strValue   = BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_put(bulki, strKey, strValue);

    // Serialize the data
    size_t size;
    void * buffer = BULKI_serialize(bulki, &size);

    // printf("Serialized data:\n");
    // BULKI_print(bulki);

    // Do some I/O if you like
    FILE *fp = fopen("test_bulki.bin", "wb");
    fwrite(buffer, 1, size, fp);
    fclose(fp);

    BULKI_free(bulki, 1);
    // printf("Freed bulki\n");

    // read the file and deserialize
    fp = fopen("test_bulki.bin", "rb");
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET); /* same as rewind(f); */
    // read the file into the buffer
    void *buffer2 = malloc(fsize + 1);
    fread(buffer2, fsize, 1, fp);
    // printf("Read %ld bytes\n", fsize);
    fclose(fp);

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer2);

    // printf("Deserialized data:\n");
    // BULKI_print(deserializedBulki);

    int equal = BULKI_equal(bulki, deserializedBulki);
    printf("bulki == deserializedBulki: %d\n", equal);

    // Free the memory
    BULKI_free(deserializedBulki, 1);
    // printf("Freed deserializedBulki\n");

    free(buffer);

    return equal;
}

int
test_put_replace()
{
    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(2);

    // Create and append key-value pairs for different data types
    BULKI_put(bulki, BULKI_ENTITY("key1", 1, PDC_STRING, PDC_CLS_ITEM),
              BULKI_ENTITY("value1", 1, PDC_STRING, PDC_CLS_ITEM));

    BULKI_put(bulki, BULKI_ENTITY("key2", 1, PDC_STRING, PDC_CLS_ITEM),
              BULKI_ENTITY("value2", 1, PDC_STRING, PDC_CLS_ITEM));

    uint64_t u64value = 7987;
    BULKI_put(bulki, BULKI_ENTITY("key1", 1, PDC_STRING, PDC_CLS_ITEM),
              BULKI_ENTITY(&u64value, 1, PDC_UINT64, PDC_CLS_ITEM));

    BULKI_Entity *dataEnt = BULKI_get(bulki, BULKI_ENTITY("key1", 1, PDC_STRING, PDC_CLS_ITEM));
    int           equal   = BULKI_Entity_equal(dataEnt, BULKI_ENTITY(&u64value, 1, PDC_UINT64, PDC_CLS_ITEM));
    printf("first value is desired after replacing the original value: %d\n", equal);
    dataEnt = BULKI_get(bulki, BULKI_ENTITY("key2", 1, PDC_STRING, PDC_CLS_ITEM));
    equal   = BULKI_Entity_equal(dataEnt, BULKI_ENTITY("value2", 1, PDC_STRING, PDC_CLS_ITEM));
    printf("second value not changed after replace put: %d\n", equal);
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
    BULKI_Entity *intArr     = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put(bulki, intKey, intArr);

    char *        doubleKey_str = "double";
    double        doubleVal     = 3.14159;
    uint64_t      doubleObjID   = 564987951987494;
    BULKI_Entity *doubleKey     = BULKI_ENTITY(doubleKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *doubleArr     = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put(bulki, doubleKey, doubleArr);

    char *strKey_str = "string";
    char *strVal     = "Hello, World!";

    BULKI_Entity *strKey = BULKI_ENTITY(strKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *strArr = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put(bulki, strKey, strArr);

    char *        mixedKey_str = "mixed";
    BULKI_Entity *mixedKey     = BULKI_ENTITY(mixedKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *mixedArr     = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(mixedArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put(bulki, mixedKey, mixedArr);

    // Initialize a serialized data structure
    BULKI *bulki2 = BULKI_init(2);

    // Create and append key-value pairs for different data types
    char *        intKey_str2 = "int";
    int           intVal2     = 42;
    BULKI_Entity *intKey2     = BULKI_ENTITY(intKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *intArr2     = empty_BULKI_Array_Entity();
    BULKI_ENTITY_append_BULKI(intArr2, bulki);
    BULKI_ENTITY_append_BULKI(intArr2, bulki);
    BULKI_put(bulki2, intKey2, intArr2);

    // Serialize the data
    size_t size;
    void * buffer = BULKI_serialize(bulki2, &size);

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
    BULKI_Entity *intArr     = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(intArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put(bulki, intKey, intArr);

    char *        doubleKey_str = "double";
    double        doubleVal     = 3.14159;
    uint64_t      doubleObjID   = 564987951987494;
    BULKI_Entity *doubleKey     = BULKI_ENTITY(doubleKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *doubleArr     = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleVal, 1, PDC_DOUBLE, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(doubleArr, BULKI_ENTITY(&doubleObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put(bulki, doubleKey, doubleArr);

    char *strKey_str = "string";
    char *strVal     = "Hello, World!";

    BULKI_Entity *strKey = BULKI_ENTITY(strKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *strArr = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(strVal, 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(strArr, BULKI_ENTITY(&intObjID, 1, PDC_UINT64, PDC_CLS_ITEM));
    BULKI_put(bulki, strKey, strArr);

    // Serialize the data
    size_t size;
    void * buffer = BULKI_serialize(bulki, &size);

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
test_bulki_in_entitiy()
{

    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(1);
    // BULKI in BULKI_Entity
    BULKI_Entity *nestEntity = BULKI_ENTITY(bulki, 1, PDC_BULKI, PDC_CLS_ITEM);
    size_t        size;
    void *        buffer         = BULKI_Entity_serialize(nestEntity, &size);
    BULKI_Entity *des_nestEntity = BULKI_Entity_deserialize(buffer);

    int equal = BULKI_Entity_equal(nestEntity, des_nestEntity);

    printf("EMPTY BULKI in BULKI Entity = %d \n", equal);

    BULKI_put(bulki, BULKI_ENTITY("key", 1, PDC_STRING, PDC_CLS_ITEM),
              BULKI_ENTITY("value", 1, PDC_STRING, PDC_CLS_ITEM));

    buffer         = BULKI_Entity_serialize(nestEntity, &size);
    des_nestEntity = BULKI_Entity_deserialize(buffer);

    equal = BULKI_Entity_equal(nestEntity, des_nestEntity);
    printf("non-empty base BULKI in BULKI Entity = %d \n", equal);

    BULKI_Entity *secondValue = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(secondValue, BULKI_ENTITY("secondValue1", 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(secondValue, BULKI_ENTITY("secondValue2", 1, PDC_STRING, PDC_CLS_ITEM));

    BULKI_put(bulki, BULKI_ENTITY("key2", 1, PDC_STRING, PDC_CLS_ITEM), secondValue);

    buffer         = BULKI_Entity_serialize(nestEntity, &size);
    des_nestEntity = BULKI_Entity_deserialize(buffer);

    equal = BULKI_Entity_equal(nestEntity, des_nestEntity);
    printf("non-empty compound BULKI in BULKI Entity with array = %d \n", equal);

    return equal;
}

int
bulki_small_json_serialization_test()
{
    // Initialize the BULKI structure
    BULKI *dataset = BULKI_init(10); // Assuming initial field count is 10

    // Create and insert "dataset_name" key-value pair
    BULKI_Entity *key1   = BULKI_singleton_ENTITY("dataset_name", PDC_STRING);
    BULKI_Entity *value1 = BULKI_singleton_ENTITY("BOSS", PDC_STRING);
    BULKI_put(dataset, key1, value1);

    // Create and insert "dataset_description" key-value pair
    BULKI_Entity *key2   = BULKI_singleton_ENTITY("dataset_description", PDC_STRING);
    BULKI_Entity *value2 = BULKI_singleton_ENTITY("LLSM dataset", PDC_STRING);
    BULKI_put(dataset, key2, value2);

    // Create and insert "source_URL" key-value pair
    BULKI_Entity *key3   = BULKI_singleton_ENTITY("source_URL", PDC_STRING);
    BULKI_Entity *value3 = BULKI_singleton_ENTITY(" ", PDC_STRING);
    BULKI_put(dataset, key3, value3);

    // Create and insert "collector" key-value pair
    BULKI_Entity *key4   = BULKI_singleton_ENTITY("collector", PDC_STRING);
    BULKI_Entity *value4 = BULKI_singleton_ENTITY("Wei Zhang", PDC_STRING);
    BULKI_put(dataset, key4, value4);

    BULKI_Entity *key5   = BULKI_singleton_ENTITY("objects", PDC_STRING);
    BULKI_Entity *value5 = empty_BULKI_Array_Entity();

    BULKI *object1 = BULKI_init(4); // 3 fields: name, type, full_path, properties
    BULKI_put(object1, BULKI_singleton_ENTITY("name", PDC_STRING),
              BULKI_singleton_ENTITY("3551-55156-1-coadd", PDC_STRING));
    BULKI_put(object1, BULKI_singleton_ENTITY("type", PDC_STRING),
              BULKI_singleton_ENTITY("file", PDC_STRING));
    BULKI_put(object1, BULKI_singleton_ENTITY("full_path", PDC_STRING),
              BULKI_singleton_ENTITY("/pscratch/sd/h/houhun/h5boss_v2/3551-55156.hdf5", PDC_STRING));

    BULKI_Entity *property_array = empty_BULKI_Array_Entity();

    BULKI *       property1 = BULKI_init(2); // 2 fields: name, value
    BULKI_Entity *name      = BULKI_singleton_ENTITY("AIRMASS", PDC_STRING);
    BULKI_Entity *data      = BULKI_ENTITY(&(float){1.19428}, 1, PDC_FLOAT, PDC_CLS_ITEM);
    BULKI_put(property1, name, data);

    // BULKI *       property2 = BULKI_init(2); // 2 fields: name, value
    BULKI_Entity *name2 = BULKI_singleton_ENTITY("ALT", PDC_STRING);
    BULKI_Entity *data2 = BULKI_ENTITY(&(float){54.0012}, 1, PDC_FLOAT, PDC_CLS_ITEM);
    BULKI_put(property1, name2, data2);

    // BULKI *       property3 = BULKI_init(2); // 2 fields: name, value
    BULKI_Entity *name3 = BULKI_singleton_ENTITY("ARCOFFX", PDC_STRING);
    BULKI_Entity *data3 = BULKI_ENTITY(&(float){0.001891}, 1, PDC_FLOAT, PDC_CLS_ITEM);
    BULKI_put(property1, name3, data3);

    // BULKI *       property4 = BULKI_init(2); // 2 fields: name, value
    BULKI_Entity *name4 = BULKI_singleton_ENTITY("ARCOFFY", PDC_STRING);
    BULKI_Entity *data4 = BULKI_ENTITY(&(float){0.001101}, 1, PDC_FLOAT, PDC_CLS_ITEM);
    BULKI_put(property1, name4, data4);

    BULKI_ENTITY_append_BULKI(property_array, property1);
    // BULKI_ENTITY_append_BULKI(property_array, property2);
    // BULKI_ENTITY_append_BULKI(property_array, property3);
    // BULKI_ENTITY_append_BULKI(property_array, property4);

    BULKI_put(object1, BULKI_singleton_ENTITY("properties", PDC_STRING), property_array);
    BULKI_ENTITY_append_BULKI(value5, object1);

    BULKI_put(dataset, key5, value5);

    FILE *fp = fopen("dataset.bin", "w");
    BULKI_serialize_to_file(dataset, fp);
    // fclose(fp);
    // Free the memory
    // BULKI_free(dataset, 1);

    return 0;
}

int
main(int argc, char *argv[])
{
    printf("test_base_type RST = %d\n", test_base_type());
    printf("test_put_replace RST = %d\n", test_put_replace());
    printf("test_base_array_entitiy RST = %d\n", test_base_array_entitiy());
    printf("test_embedded_entitiy RST = %d\n", test_embedded_entitiy());
    printf("test_nested_entitiy RST = %d\n", test_bulki_in_entitiy());
    printf("bulki_small_json_serialization_test RST = %d\n", bulki_small_json_serialization_test());
    return 0;
}