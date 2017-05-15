#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"


int main() {
    struct PDC_prop p;
    pdcid_t pdc;
    
    // create a pdc
    pdc = PDC_init(p);
    printf("generated new pdc, id is %lld\n", pdc);

    // close pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
}
