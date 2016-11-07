#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"

void print_usage() {
    printf("Usage: srun -n ./creat_obj -c num_of_obj_per_rank\n");
}


int main(int argc, const char *argv[])
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    int count = -1;
    char c;
    while ((c = getopt (argc, argv, "c:")) != -1)
        switch (c)
        {
         case 'c':
           count = atoi(optarg);
           break;
         case '?':
           if (optopt == 'c')
             fprintf (stderr, "Option -%c requires an argument.\n", optopt);
           else if (isprint (optopt))
             fprintf (stderr, "Unknown option `-%c'.\n", optopt);
           else
             fprintf (stderr, "Unknown option character `\\x%x'.\n", optopt);
           return 1;
         default:
           print_usage();
           exit(-1);
        }

    if (count == -1) {
        print_usage();
        exit(-1);
    }


    PDC_prop_t p;
    pdcid_t pdc = PDC_init(p);

    pdcid_t test_obj = -1;

    int i;
    char obj_name[16];

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    gettimeofday(&ht_total_start, 0);

    for (i = 0; i < count; i++) {
        sprintf(obj_name, "%s_%d", "Obj", i + rank * 100000);
        test_obj = PDCobj_create(pdc, obj_name, NULL);
        if (test_obj < 0) { 
            printf("Error getting an object id from server, exit...\n");
            exit(-1);
        }

    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 100000.0;
    printf("Total time for creating %d obj per processs with %d ranks: %.6f\n", count, size, ht_total_sec);


    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    else
       printf("PDC is closed\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
