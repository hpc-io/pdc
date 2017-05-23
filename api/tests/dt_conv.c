#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1;
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    
    // create a pdc
    pdcid_t pdc = PDC_init("pdc");
    printf("generated new pdc, id is %lld\n", pdc);

    float a[10] = {1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1};
    int b[5];
    pdc_type_conv(PDC_FLOAT, PDC_INT, a, b, 5, 2);
    
    int i;
    for(i = 0; i<5; i++)
        printf("b[%d] is %d\n", i, b[i]);
    // close pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");
    
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
}
