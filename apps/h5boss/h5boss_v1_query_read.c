#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"


void print_usage() {
    printf("Usage: srun -n ./h5boss_query_random /path/to/pmf_list.txt \n");
}

int main(int argc, char **argv)
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    const char *pm_filename = argv[1];

    if (argc != 2 || pm_filename == NULL) {
        print_usage();
        exit(-1);
    }

    int i, j, count, my_count;
    int plate[3000], mjd[3000], fiber[3000];
    int my_plate[3000], my_mjd[3000], my_fiber[3000];
    int *plate_ptr, *mjd_ptr, *fiber_ptr;

    // Rank 0 read from pm file
    if (rank == 0) {
        printf("Reading from %s\n", pm_filename);
        FILE *pm_file = fopen(pm_filename, "r");

        if (NULL == pm_file) {
            fprintf(stderr, "Error opening file: %s\n", pm_filename);
            return (-1);
        }

        i = 0;
        while (EOF != fscanf(pm_file, "%d %d %d", &plate[i], &mjd[i], &fiber[i])) {
            /* printf("%d, %d, %d\n", plate[i], mjd[i], fiber[i]); */
            i++;
        }

        fclose(pm_file);
        count = i;
    }

    

    my_count  = count;
    plate_ptr = plate;
    mjd_ptr   = mjd;
    fiber_ptr = fiber;

#ifdef ENABLE_MPI
    MPI_Bcast( &count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (size > count) {
        if (rank == 0) {
            printf("Number of process is more than plate-mjd-fiber combination, exiting...\n");
        }
        MPI_Finalize();
        exit (-1);
    }

    // Distribute work evenly
    my_count = count / size;

    // Last rank may have extra work
    if (rank == size - 1) {
        my_count += count % size;
    }


    int *sendcount = (int*)malloc(sizeof(int)* size);
    int *displs = (int*)malloc(sizeof(int)* size);
    for (i = 0; i < size; i++) {
        sendcount[i] = count / size;
        if (i == size - 1) 
            sendcount[i] += count % size;
        displs[i] = i * (count / size);
    }
    
    MPI_Scatterv(plate, sendcount, displs, MPI_INT, my_plate, my_count, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Scatterv(mjd,   sendcount, displs, MPI_INT, my_mjd,   my_count, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Scatterv(fiber, sendcount, displs, MPI_INT, my_fiber, my_count, MPI_INT, 0, MPI_COMM_WORLD);
    plate_ptr = my_plate;
    mjd_ptr   = my_mjd;
    fiber_ptr = my_fiber;
    MPI_Barrier(MPI_COMM_WORLD);
    free(displs);
    free(sendcount);
#endif 

    printf("%d: myquery = %d\n", rank, my_count);


    pdcid_t pdc = PDC_init("PDC");

    char **obj_names;
    void **buf;
    int *buf_sizes;

    pdcid_t test_obj = -1;

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;


#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (rank == 0) {
        printf("Starting to query...\n");
    }

    gettimeofday(&ht_total_start, 0);

    obj_names = (char**)calloc(my_count, sizeof(char*));
    buf       = (void**)calloc(my_count, sizeof(void*));
    buf_sizes = (int*)calloc(my_count, sizeof(int));
    for (i = 0; i < my_count; i++) {
        obj_names[i] = (char*)calloc(128, sizeof(char));
        buf[i] = (void*)calloc(128, sizeof(void*));
        sprintf(obj_names[i], "/%d/%d/%d/exposures/104198/r", plate_ptr[i], mjd_ptr[i], fiber_ptr[i]);
    }


    PDC_Client_query_name_read_entire_obj(my_count, obj_names, buf, buf_sizes);


    fflush(stdout);
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to query and read %d obj with %d ranks: %.6f\n", count, size, ht_total_sec);
        fflush(stdout);
    }

done:
    if(PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
