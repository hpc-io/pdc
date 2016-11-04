#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"


int main() {
    PDC_prop_t p;
    pdcid_t pdc = PDC_init(p);

    pdcid_t test_obj = -1;
    test_obj = PDCobj_create(pdc, "TestObj", NULL);
    if (test_obj > 0) {
        printf("Object ID is: %llu\n", test_obj);
    }
    else {
        printf("Error getting an object id from server\n");
    }

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
}
