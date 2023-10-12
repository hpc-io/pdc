#include "bulki_serde.h"

int
test_serde_framework()
{
    // Initialize a serialized data structure
    BULKI *data = BULKI_serde_init(5);

    // Create and append key-value pairs for different data types
    char *       intKey_str = "int";
    int          intVal     = 42;
    BULKI_Key *  intKey     = BULKI_KEY(intKey_str, PDC_STRING, sizeof(intKey_str));
    BULKI_Value *intValue   = BULKI_VALUE(&intVal, PDC_INT, PDC_CLS_SCALAR, sizeof(int));
    BULKI_serde_append_key_value(data, intKey, intValue);

    char *       doubleKey_str = "double";
    double       doubleVal     = 3.14159;
    BULKI_Key *  doubleKey     = BULKI_KEY(doubleKey_str, PDC_STRING, sizeof(doubleKey_str));
    BULKI_Value *doubleValue   = BULKI_VALUE(&doubleVal, PDC_DOUBLE, PDC_CLS_SCALAR, sizeof(double));
    BULKI_serde_append_key_value(data, doubleKey, doubleValue);

    char *       strKey_str = "string";
    char *       strVal     = "Hello, World!";
    BULKI_Key *  strKey     = BULKI_KEY(strKey_str, PDC_STRING, (strlen(strKey_str) + 1) * sizeof(char));
    BULKI_Value *strValue =
        BULKI_VALUE(strVal, PDC_STRING, PDC_CLS_SCALAR, (strlen(strVal) + 1) * sizeof(char));
    BULKI_serde_append_key_value(data, strKey, strValue);

    char *       arrayKey_str = "array";
    int          intArray[3]  = {1, 2, 3};
    BULKI_Key *  arrayKey     = BULKI_KEY(arrayKey_str, PDC_STRING, sizeof(arrayKey_str));
    BULKI_Value *arrayValue   = BULKI_VALUE(intArray, PDC_INT, PDC_CLS_ARRAY, 3);
    BULKI_serde_append_key_value(data, arrayKey, arrayValue);

    typedef struct {
        int x;
        int y;
    } Point;

    Point pointVal = {10, 20};

    // prepare the data of a struct
    BULKI *      point_data = BULKI_serde_init(2);
    BULKI_Key *  x_name     = BULKI_KEY("x", PDC_STRING, sizeof(char *));
    BULKI_Value *x_value    = BULKI_VALUE(&pointVal.x, PDC_INT, PDC_CLS_SCALAR, sizeof(int));
    BULKI_Key *  y_name     = BULKI_KEY("y", PDC_STRING, sizeof(char *));
    BULKI_Value *y_value    = BULKI_VALUE(&pointVal.y, PDC_INT, PDC_CLS_SCALAR, sizeof(int));

    BULKI_serde_append_key_value(point_data, x_name, x_value);
    BULKI_serde_append_key_value(point_data, y_name, y_value);

    // append the struct data as a key value pair, along with a key.
    char *       pointKey    = "point";
    BULKI_Key *  structKey   = BULKI_KEY(pointKey, PDC_STRING, sizeof(pointKey));
    BULKI_Value *structValue = BULKI_VALUE(point_data, PDC_VOID_PTR, PDC_CLS_STRUCT, sizeof(Point));
    BULKI_serde_append_key_value(data, structKey, structValue);

    // Serialize the data
    void *buffer = BULKI_serde_serialize(data);

    printf("Serialized data:\n");
    BULKI_serde_print(data);

    // Deserialize the buffer
    BULKI *deserializedData = BULKI_serde_deserialize(buffer);

    printf("Deserialized data:\n");

    // Print the deserialized data
    BULKI_serde_print(deserializedData);

    // Free the memory
    BULKI_serde_free(data);
    BULKI_serde_free(deserializedData);
    free(buffer);

    return 0;
}

int
main(int argc, char *argv[])
{
    return test_serde_framework();
}