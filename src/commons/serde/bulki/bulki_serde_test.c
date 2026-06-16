#include "bulki_serde.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

int
test_base_type()
{
    FUNC_ENTER(NULL);

    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(2);

    // Create and append key-value pairs for different data types
    char *        intKey_str = "int";
    int           intVal     = 42;
    BULKI_Entity *intKey     = BULKI_ENTITY(intKey_str, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *intValue   = BULKI_ENTITY(&intVal, 1, PDC_INT, PDC_CLS_ITEM);
    BULKI_put(bulki, intKey, intValue);

    int *intArrVal          = (int *)PDC_malloc(3 * sizeof(int));
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

    // Do some I/O if you like
    FILE *fp = fopen("test_bulki.bin", "wb");
    fwrite(buffer, 1, size, fp);
    fclose(fp);

    BULKI_free(bulki, 1);

    // read the file and deserialize
    fp = fopen("test_bulki.bin", "rb");
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET); /* same as rewind(f); */
    // read the file into the buffer
    void *buffer2 = PDC_malloc(fsize + 1);
    fread(buffer2, fsize, 1, fp);
    fclose(fp);

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer2);

    int equal = BULKI_equal(bulki, deserializedBulki);
    LOG_INFO("bulki == deserializedBulki: %d\n", equal);

    // Free the memory
    BULKI_free(deserializedBulki, 1);

    buffer = (void *)PDC_free(buffer);

    FUNC_LEAVE(equal);
}

int
test_put_replace()
{
    FUNC_ENTER(NULL);

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
    LOG_INFO("first value is desired after replacing the original value: %d\n", equal);
    dataEnt = BULKI_get(bulki, BULKI_ENTITY("key2", 1, PDC_STRING, PDC_CLS_ITEM));
    equal   = BULKI_Entity_equal(dataEnt, BULKI_ENTITY("value2", 1, PDC_STRING, PDC_CLS_ITEM));
    LOG_INFO("second value not changed after replace put: %d\n", equal);

    FUNC_LEAVE(equal);
}

int
test_base_array_entitiy()
{
    FUNC_ENTER(NULL);

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

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer);

    int equal = BULKI_equal(bulki2, deserializedBulki);
    LOG_INFO("bulki2 == deserializedBulki: %d\n", equal);

    // Free the memory
    BULKI_free(deserializedBulki, 1);
    BULKI_free(bulki, 1);
    buffer = (void *)PDC_free(buffer);

    FUNC_LEAVE(equal);
}

int
test_embedded_entitiy()
{
    FUNC_ENTER(NULL);

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

    // Deserialize the buffer
    BULKI *deserializedBulki = BULKI_deserialize(buffer);

    int equal = BULKI_equal(bulki, deserializedBulki);
    LOG_INFO("bulki == deserializedBulki: %d\n", equal);

    // Free the memory
    BULKI_free(deserializedBulki, 1);
    BULKI_free(bulki, 1);
    buffer = (void *)PDC_free(buffer);

    FUNC_LEAVE(equal);
}

int
test_bulki_in_entitiy()
{
    FUNC_ENTER(NULL);

    // Initialize a serialized data structure
    BULKI *bulki = BULKI_init(1);
    // BULKI in BULKI_Entity
    BULKI_Entity *nestEntity = BULKI_ENTITY(bulki, 1, PDC_BULKI, PDC_CLS_ITEM);
    size_t        size;
    void *        buffer         = BULKI_Entity_serialize(nestEntity, &size);
    BULKI_Entity *des_nestEntity = BULKI_Entity_deserialize(buffer);

    int equal = BULKI_Entity_equal(nestEntity, des_nestEntity);

    LOG_INFO("EMPTY BULKI in BULKI Entity = %d \n", equal);

    BULKI_put(bulki, BULKI_ENTITY("key", 1, PDC_STRING, PDC_CLS_ITEM),
              BULKI_ENTITY("value", 1, PDC_STRING, PDC_CLS_ITEM));

    buffer         = BULKI_Entity_serialize(nestEntity, &size);
    des_nestEntity = BULKI_Entity_deserialize(buffer);

    equal = BULKI_Entity_equal(nestEntity, des_nestEntity);
    LOG_INFO("non-empty base BULKI in BULKI Entity = %d \n", equal);

    BULKI_Entity *secondValue = empty_Bent_Array_Entity();
    BULKI_ENTITY_append_BULKI_Entity(secondValue, BULKI_ENTITY("secondValue1", 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_ENTITY_append_BULKI_Entity(secondValue, BULKI_ENTITY("secondValue2", 1, PDC_STRING, PDC_CLS_ITEM));

    BULKI_put(bulki, BULKI_ENTITY("key2", 1, PDC_STRING, PDC_CLS_ITEM), secondValue);

    buffer         = BULKI_Entity_serialize(nestEntity, &size);
    des_nestEntity = BULKI_Entity_deserialize(buffer);

    equal = BULKI_Entity_equal(nestEntity, des_nestEntity);
    LOG_INFO("non-empty compound BULKI in BULKI Entity with array = %d \n", equal);

    FUNC_LEAVE(equal);
}

int
test_incremental_array_append_sizes()
{
    FUNC_ENTER(NULL);

    const int     n_items = 128;
    BULKI_Entity *arr     = empty_BULKI_Array_Entity_with_capacity(n_items);
    int           i;
    int           pass = 1;

    for (i = 0; i < n_items; i++) {
        BULKI *item = BULKI_init(1);
        int    val  = i;

        BULKI_put_incremental(item, BULKI_ENTITY("key", 1, PDC_STRING, PDC_CLS_ITEM),
                              BULKI_ENTITY(&val, 1, PDC_INT, PDC_CLS_ITEM));

        {
            size_t size_before   = arr->size;
            size_t expected_step = item->totalSize;

            BULKI_ENTITY_append_BULKI_incremental(arr, item);
            if (arr->size != size_before + expected_step) {
                LOG_ERROR("incremental array size mismatch at i=%d\n", i);
                pass = 0;
                break;
            }
        }
    }

    if (pass) {
        size_t recomputed = get_BULKI_Entity_size(arr);
        if (arr->size != recomputed || arr->count != (uint64_t)n_items) {
            LOG_ERROR("array size/count mismatch after append\n");
            pass = 0;
        }
    }

    if (pass && arr->capacity < (uint64_t)n_items) {
        LOG_ERROR("preallocated capacity too small: %zu\n", (size_t)arr->capacity);
        pass = 0;
    }

    if (arr != NULL)
        BULKI_Entity_free(arr, 1);

    FUNC_LEAVE(pass);
}

int
test_preallocated_array_roundtrip()
{
    FUNC_ENTER(NULL);

    const int     n_items = 64;
    BULKI *       root    = BULKI_init(1);
    BULKI_Entity *arr     = empty_BULKI_Array_Entity_with_capacity(n_items);
    int           i;

    for (i = 0; i < n_items; i++) {
        BULKI *item = BULKI_init(2);
        int    val  = i * 10;

        BULKI_put_incremental(item, BULKI_singleton_ENTITY("name", PDC_STRING),
                                BULKI_singleton_ENTITY("entry", PDC_STRING));
        BULKI_put_incremental(item, BULKI_singleton_ENTITY("value", PDC_STRING),
                                BULKI_ENTITY(&val, 1, PDC_INT, PDC_CLS_ITEM));
        BULKI_ENTITY_append_BULKI_incremental(arr, item);
    }

    BULKI_put_incremental(root, BULKI_singleton_ENTITY("entries", PDC_STRING), arr);

    {
        size_t   size;
        void *   buffer          = BULKI_serialize(root, &size);
        BULKI *  deserialized    = BULKI_deserialize(buffer);
        int      equal           = BULKI_equal(root, deserialized);

        LOG_INFO("preallocated array roundtrip equal: %d\n", equal);
        BULKI_free(deserialized, 1);
        buffer = (void *)PDC_free(buffer);
        BULKI_free(root, 1);

        FUNC_LEAVE(equal);
    }
}

int
test_incremental_bulki_put_total_size()
{
    FUNC_ENTER(NULL);

    BULKI *bulki = BULKI_init(4);
    int    pass  = 1;

    BULKI_put_incremental(bulki, BULKI_ENTITY("a", 1, PDC_STRING, PDC_CLS_ITEM),
                          BULKI_ENTITY("1", 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_put_incremental(bulki, BULKI_ENTITY("b", 1, PDC_STRING, PDC_CLS_ITEM),
                          BULKI_ENTITY("2", 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_put_incremental(bulki, BULKI_ENTITY("c", 1, PDC_STRING, PDC_CLS_ITEM),
                          BULKI_ENTITY("3", 1, PDC_STRING, PDC_CLS_ITEM));

    if (bulki->totalSize != get_BULKI_size(bulki)) {
        LOG_ERROR("incremental totalSize mismatch after BULKI_put_incremental\n");
        pass = 0;
    }

    BULKI_put_incremental(bulki, BULKI_ENTITY("a", 1, PDC_STRING, PDC_CLS_ITEM),
                          BULKI_ENTITY("updated", 1, PDC_STRING, PDC_CLS_ITEM));
    if (bulki->totalSize != get_BULKI_size(bulki)) {
        LOG_ERROR("incremental totalSize mismatch after BULKI_put_incremental replace\n");
        pass = 0;
    }

    BULKI_free(bulki, 1);
    FUNC_LEAVE(pass);
}

int
test_incremental_entity_array_append_sizes()
{
    FUNC_ENTER(NULL);

    const int     n_items = 64;
    BULKI_Entity *arr     = empty_Bent_Array_Entity_with_capacity(n_items);
    int           i;
    int           pass = 1;

    for (i = 0; i < n_items; i++) {
        int           val  = i * 3;
        BULKI_Entity *item = BULKI_ENTITY(&val, 1, PDC_INT, PDC_CLS_ITEM);

        {
            size_t size_before   = arr->size;
            size_t expected_step = item->size;

            BULKI_ENTITY_append_BULKI_Entity_incremental(arr, item);
            if (arr->size != size_before + expected_step) {
                LOG_ERROR("incremental entity array size mismatch at i=%d\n", i);
                pass = 0;
                break;
            }
        }
    }

    if (pass) {
        size_t recomputed = get_BULKI_Entity_size(arr);
        if (arr->size != recomputed || arr->count != (uint64_t)n_items) {
            LOG_ERROR("entity array size/count mismatch after append\n");
            pass = 0;
        }
    }

    if (pass && arr->capacity < (uint64_t)n_items) {
        LOG_ERROR("preallocated entity array capacity too small: %zu\n", (size_t)arr->capacity);
        pass = 0;
    }

    if (arr != NULL)
        BULKI_Entity_free(arr, 1);

    FUNC_LEAVE(pass);
}

int
test_incremental_bulki_delete_total_size()
{
    FUNC_ENTER(NULL);

    BULKI *bulki = BULKI_init(4);
    int    pass  = 1;

    BULKI_put_incremental(bulki, BULKI_ENTITY("a", 1, PDC_STRING, PDC_CLS_ITEM),
                          BULKI_ENTITY("1", 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_put_incremental(bulki, BULKI_ENTITY("b", 1, PDC_STRING, PDC_CLS_ITEM),
                          BULKI_ENTITY("2", 1, PDC_STRING, PDC_CLS_ITEM));
    BULKI_put_incremental(bulki, BULKI_ENTITY("c", 1, PDC_STRING, PDC_CLS_ITEM),
                          BULKI_ENTITY("3", 1, PDC_STRING, PDC_CLS_ITEM));

    if (bulki->numKeys != 3 || bulki->totalSize != get_BULKI_size(bulki)) {
        LOG_ERROR("incremental setup failed before BULKI_delete_incremental\n");
        pass = 0;
        goto done;
    }

    if (BULKI_delete_incremental(bulki, BULKI_ENTITY("b", 1, PDC_STRING, PDC_CLS_ITEM)) == NULL) {
        LOG_ERROR("BULKI_delete_incremental did not remove existing key\n");
        pass = 0;
        goto done;
    }

    if (bulki->numKeys != 2 || bulki->totalSize != get_BULKI_size(bulki)) {
        LOG_ERROR("incremental totalSize mismatch after BULKI_delete_incremental\n");
        pass = 0;
        goto done;
    }

    if (BULKI_get(bulki, BULKI_ENTITY("b", 1, PDC_STRING, PDC_CLS_ITEM)) != NULL) {
        LOG_ERROR("deleted key still present after BULKI_delete_incremental\n");
        pass = 0;
        goto done;
    }

    if (BULKI_delete_incremental(bulki, BULKI_ENTITY("missing", 1, PDC_STRING, PDC_CLS_ITEM)) != NULL) {
        LOG_ERROR("BULKI_delete_incremental should return NULL for missing key\n");
        pass = 0;
        goto done;
    }

    if (bulki->numKeys != 2 || bulki->totalSize != get_BULKI_size(bulki)) {
        LOG_ERROR("incremental totalSize mismatch after missing-key delete\n");
        pass = 0;
    }

done:
    BULKI_free(bulki, 1);
    FUNC_LEAVE(pass);
}

int
bulki_small_json_serialization_test()
{
    FUNC_ENTER(NULL);

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

    FUNC_LEAVE(0);
}

int
main(int argc, char *argv[])
{
    FUNC_ENTER(NULL);

    LOG_INFO("test_base_type RST = %d\n", test_base_type());
    LOG_INFO("test_put_replace RST = %d\n", test_put_replace());
    LOG_INFO("test_base_array_entitiy RST = %d\n", test_base_array_entitiy());
    LOG_INFO("test_embedded_entitiy RST = %d\n", test_embedded_entitiy());
    LOG_INFO("test_nested_entitiy RST = %d\n", test_bulki_in_entitiy());
    LOG_INFO("test_incremental_array_append_sizes RST = %d\n", test_incremental_array_append_sizes());
    LOG_INFO("test_preallocated_array_roundtrip RST = %d\n", test_preallocated_array_roundtrip());
    LOG_INFO("test_incremental_bulki_put_total_size RST = %d\n", test_incremental_bulki_put_total_size());
    LOG_INFO("test_incremental_entity_array_append_sizes RST = %d\n",
             test_incremental_entity_array_append_sizes());
    LOG_INFO("test_incremental_bulki_delete_total_size RST = %d\n", test_incremental_bulki_delete_total_size());
    LOG_INFO("bulki_small_json_serialization_test RST = %d\n", bulki_small_json_serialization_test());

    FUNC_LEAVE(0);
}