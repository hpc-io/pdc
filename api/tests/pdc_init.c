#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"


int main(int argc, char **argv) {
    pdcid_t pdc;
    
    // create a pdc
    pdc = PDC_init("pdc");
    printf("generated new pdc, id is %lld\n", pdc);

    // close pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
