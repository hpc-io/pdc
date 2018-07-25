#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>

#define ENABLE_MPI 1

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

int assign_work_to_rank(int rank, int size, int nwork, int *my_count, int *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        printf("assign_work_to_rank(): Error with input!\n");
        return -1;
    }
    if (nwork < size) {
        if (rank < nwork) 
            *my_count = 1;
        else
            *my_count = 0;
        (*my_start) = rank * (*my_count);
    }
    else {
        (*my_count) = nwork / size;
        (*my_start) = rank * (*my_count);

        // Last few ranks may have extra work
        if (rank >= size - nwork % size) {
            (*my_count)++; 
            (*my_start) += (rank - (size - nwork % size));
        }
    }

    return 1;
}


int main(int argc, char **argv)
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    int i, j, count, my_count, my_start;
    char **all_dset_names = NULL, *all_dset_names_1d = NULL;
    char **my_dset_names = NULL;
    const char *pm_filename = argv[1];

    if (argc != 2 || pm_filename == NULL ) {
        print_usage();
        exit(-1);
    }

    all_dset_names    = (char**)calloc(NDSET, sizeof(char*));
    all_dset_names_1d = (char*)calloc(NDSET*NAMELEN, sizeof(char));
    for (i = 0; i < NDSET; i++) 
        all_dset_names[i] = all_dset_names_1d + i*NAMELEN;

    // Rank 0 read from pm file
    if (rank == 0) {

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
    my_count      = count;
    my_start      = 0;
    my_dset_names = all_dset_names;

#ifdef ENABLE_MPI
    MPI_Bcast( &count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast( all_dset_names_1d, NDSET*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);

    assign_work_to_rank(rank, size, count, &my_count, &my_start);

    /* printf("Proc %d: my_count: %d, my_start: %d\n", my_rank, my_count, my_start); */
    /* fflush(stdout); */
#endif 

    if (my_count > 0) {
        my_dset_names = (char**)calloc(my_count, sizeof(char*)); 
        for (i = 0; i < my_count; i++) 
            my_dset_names[i] = all_dset_names[i+my_start];
    }
    else
        my_dset_names = NULL;

    /* printf("%d: my count is %d\n", rank, my_count); */
    /* for (i = 0; i < my_count; i++) { */
    /*     printf("%d: [%s]\n", rank, my_dset_names[i]); */
    /* } */
    /* fflush(stdout); */

    pdcid_t pdc = PDC_init("PDC");

    double start_time, end_time, elapsed_time;
    void **buf;
    size_t *buf_sizes = calloc(sizeof(size_t), my_count);

    if (rank == 0) {
        printf("Starting to query and read...\n");
        fflush(stdout);
    }

    int iter = 0, niter = 10;
    for (iter = 0; iter < niter; iter++) {
    #ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        start_time = MPI_Wtime();
    #endif
    
        /* PDC_Client_query_name_read_entire_obj_client(my_count, my_dset_names, &buf, buf_sizes); */
        /* PDC_Client_query_name_read_entire_obj_client_agg(my_count, my_dset_names, &buf, buf_sizes); */
        PDC_Client_query_name_read_entire_obj_client_agg_cache_iter(my_count, my_dset_names, &buf, buf_sizes, 
                                                                    iter*10);
    
    #ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        end_time = MPI_Wtime();
        elapsed_time = end_time - start_time;
    #endif
    
        /* printf("%d: my count is %d\n", rank, my_count); */
        /* fflush(stdout); */
    
        long long my_total_data_size = 0, all_total_data_size;
        for (i = 0; i < my_count; i++) {
            my_total_data_size += buf_sizes[i];
            /* printf("%d: read [%s], size %lu\n", rank, my_dset_names[i], buf_sizes[i]); */
        }
        /* printf("%d: my total read size = %lu\n", rank, my_total_data_size); */
        /* fflush(stdout); */
    
    #ifdef ENABLE_MPI
        MPI_Reduce(&my_total_data_size, &all_total_data_size, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    #else
        all_total_data_size = my_total_data_size;
    #endif
    
        if (rank == 0) {
            printf("Time to query and read %d obj of %.4f MB with %d ranks and cache %d\%: %.4f\n", 
                    count, all_total_data_size/1048576.0, size, iter*10, elapsed_time);
            fflush(stdout);
        }

        sleep(3);
    }
done:
    if(PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
