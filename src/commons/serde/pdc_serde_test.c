#include "pdc_serde.h"

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
    PDC_SERDE_Value *doubleValue   = PDC_SERDE_VALUE(&doubleVal, PDC_DOUBLE, PDC_CLS_SCALAR, sizeof(double));
    pdc_serde_append_key_value(data, doubleKey, doubleValue);

    char *           strKey_str = "string";
    char *           strVal     = "Hello, World!";
    PDC_SERDE_Key *  strKey = PDC_SERDE_KEY(strKey_str, PDC_STRING, (strlen(strKey_str) + 1) * sizeof(char));
    PDC_SERDE_Value *strValue =
        PDC_SERDE_VALUE(strVal, PDC_STRING, PDC_CLS_SCALAR, (strlen(strVal) + 1) * sizeof(char));
    pdc_serde_append_key_value(data, strKey, strValue);

    char *           arrayKey_str = "array";
    int              intArray[3]  = {1, 2, 3};
    PDC_SERDE_Key *  arrayKey     = PDC_SERDE_KEY(arrayKey_str, PDC_STRING, sizeof(arrayKey_str));
    PDC_SERDE_Value *arrayValue   = PDC_SERDE_VALUE(intArray, PDC_INT, PDC_CLS_ARRAY, 3);
    pdc_serde_append_key_value(data, arrayKey, arrayValue);

    typedef struct {
        int x;
        int y;
    } Point;

    Point pointVal = {10, 20};

    // prepare the data of a struct
    PDC_SERDE_SerializedData *point_data = pdc_serde_init(2);
    PDC_SERDE_Key *           x_name     = PDC_SERDE_KEY("x", PDC_STRING, sizeof(char *));
    PDC_SERDE_Value *         x_value    = PDC_SERDE_VALUE(&pointVal.x, PDC_INT, PDC_CLS_SCALAR, sizeof(int));
    PDC_SERDE_Key *           y_name     = PDC_SERDE_KEY("y", PDC_STRING, sizeof(char *));
    PDC_SERDE_Value *         y_value    = PDC_SERDE_VALUE(&pointVal.y, PDC_INT, PDC_CLS_SCALAR, sizeof(int));

    pdc_serde_append_key_value(point_data, x_name, x_value);
    pdc_serde_append_key_value(point_data, y_name, y_value);

    // append the struct data as a key value pair, along with a key.
    char *           pointKey    = "point";
    PDC_SERDE_Key *  structKey   = PDC_SERDE_KEY(pointKey, PDC_STRING, sizeof(pointKey));
    PDC_SERDE_Value *structValue = PDC_SERDE_VALUE(point_data, PDC_VOID_PTR, PDC_CLS_STRUCT, sizeof(Point));
    pdc_serde_append_key_value(data, structKey, structValue);

    // Serialize the data
    void *buffer = pdc_serde_serialize(data);

    printf("Serialized data:\n");
    pdc_serde_print(data);

    // Deserialize the buffer
    PDC_SERDE_SerializedData *deserializedData = pdc_serde_deserialize(buffer);

    printf("Deserialized data:\n");

    // Print the deserialized data
    pdc_serde_print(deserializedData);

    // Free the memory
    pdc_serde_free(data);
    pdc_serde_free(deserializedData);
    free(buffer);

    return 0;
}

int
main(int argc, char *argv[])
{
    return test_serde_framework();
}