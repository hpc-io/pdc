#include "pdc_generic.h"

PDC_CType getDataTypeByName(const char* typeName) {
    for (int i = 0; i < SIZE_COUNT; i++) {
        if (strcmp(typeName, DataTypeNames[i]) == 0) {
            return (PDC_CType)i;
        }
    }
    return -1;  // or some other error value
}


