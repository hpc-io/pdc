#include "pdc_generic.h"



DataType getDataTypeByName(const char* typeName) {
    for (int i = 0; i < SIZE_COUNT; i++) {
        if (strcmp(typeName, DataTypeNames[i]) == 0) {
            return (DataType)i;
        }
    }
    return -1;  // or some other error value
}


