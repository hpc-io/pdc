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

#define NDSET 25304802
#define NAMELEN 48

void print_usage() {
    printf("Usage: srun -n ./h5boss_query_read /path/to/dset_list.txt \n");
}

int main(int argc, char **argv)
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    int i, j, count, my_count;
    char **all_dset_names = NULL, *all_dset_names_1d = NULL;
    char **my_dset_names = NULL, *my_dset_names_1d = NULL;
    const char *pm_filename = argv[1];

    if (argc != 2 || pm_filename == NULL ) {
        print_usage();
        exit(-1);
    }


    // Rank 0 read from pm file
    if (rank == 0) {
        all_dset_names    = (char**)calloc(NDSET, sizeof(char*));
        all_dset_names_1d = (char*)calloc(NDSET*NAMELEN, sizeof(char));
        for (i = 0; i < NDSET; i++) 
            all_dset_names[i] = all_dset_names_1d + i*NAMELEN;

        printf("Reading from %s\n", pm_filename);
        FILE *pm_file = fopen(pm_filename, "r");
        if (NULL == pm_file) {
            fprintf(stderr, "Error opening file: %s\n", pm_filename);
            return (-1);
        }

        i = 0;
        while (EOF != fscanf(pm_file, "%s", all_dset_names[i])) {
            i++;
        }

        fclose(pm_file);
        count = i;

        /* for (i = 0; i < count; i++) { */
        /*     printf("[%s]\n", all_dset_names[i]); */
        /* } */
    }

    fflush(stdout);
    my_count  = count;
    my_dset_names = all_dset_names;

#ifdef ENABLE_MPI
    MPI_Bcast( &count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (size > count) {
        if (rank == 0) {
            printf("Number of process is more than number of dsets, exiting...\n");
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

    /* printf("%d: mycount is %d\n", rank, my_count); */
    /* fflush(stdout); */

    my_dset_names    = (char**)calloc(my_count, sizeof(char*));
    my_dset_names_1d = (char*)calloc(my_count*NAMELEN, sizeof(char));

    int *sendcount = (int*)calloc(sizeof(int), size);
    int *displs = (int*)calloc(sizeof(int), size);
    for (i = 0; i < size; i++) {
        sendcount[i] = (count / size) * NAMELEN;
        if (i == size-1) 
            sendcount[i] += (count % size) * NAMELEN;
        displs[i] = i * (count / size) * NAMELEN;
        /* if (rank == 0) */ 
        /*     printf("count[%d]=%d, displs[%d]=%d\n", i, sendcount[i], i, displs[i]); */
    }
    
    /* printf("%d: all_dset_names_1d [%s]\n", rank, all_dset_names_1d+NAMELEN); */
    MPI_Scatterv(all_dset_names_1d, sendcount, displs, MPI_CHAR, my_dset_names_1d, my_count*NAMELEN, MPI_CHAR, 0, 
                 MPI_COMM_WORLD);
    for (i = 0; i < my_count; i++) 
        my_dset_names[i] = my_dset_names_1d + i*NAMELEN;
    free(displs);
    free(sendcount);
#endif 
    /* printf("%d: my_dset_names_1d [%s]\n", rank, my_dset_names_1d); */

    /* printf("%d: my count is %d\n", rank, my_count); */
    /* for (i = 0; i < my_count; i++) { */
    /*     printf("%d: [%s]\n", rank, my_dset_names[i]); */
    /* } */
    /* fflush(stdout); */

    pdcid_t pdc = PDC_init("PDC");

    void **buf;
    size_t *buf_sizes = calloc(sizeof(size_t), my_count);

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;


    if (rank == 0) {
        printf("Starting to query...\n");
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    PDC_Client_query_name_read_entire_obj_client(my_count, my_dset_names, &buf, buf_sizes);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    long long my_total_data_size = 0, all_total_data_size;
    for (i = 0; i < my_count; i++) {
        my_total_data_size += buf_sizes[i];
        /* printf("%d: read [%s], size %lu\n", rank, my_dset_names[i], buf_sizes[i]); */
    }
    /* printf("%d: my total read size = %lu\n", rank, my_total_data_size); */
#ifdef ENABLE_MPI
    MPI_Reduce(&my_total_data_size, &all_total_data_size, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_total_data_size = my_total_data_size;
#endif

    if (rank == 0) {
        printf("Time to query and read %d obj of %.4f MB with %d ranks: %.4f\n", 
                count, all_total_data_size/1048576.0, size, ht_total_sec);
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
