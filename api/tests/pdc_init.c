#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"


int main() {
    PDC_prop_t p;
    pdcid_t pdc = PDC_init(p);
    printf("generated new pdc, id is %lld\n", pdc);

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
}
