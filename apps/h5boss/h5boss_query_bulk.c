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
#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"


void print_usage() {
    printf("Usage: srun -n ./h5boss_query_random /path/to/pm_list.txt n_query\n");
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

    if (argc < 3 || pm_filename == NULL) {
        print_usage();
        exit(-1);
    }

    int n_query, my_query;
    n_query = atoi(argv[2]);

    n_query /= 1000;

    int i, j, count, my_count;
    int plate[3000], mjd[3000];
    int my_plate[3000], my_mjd[3000];
    int *plate_ptr, *mjd_ptr;

    // Rank 0 read from pm file
    if (rank == 0) {
        printf("Reading from %s\n", pm_filename);
        FILE *pm_file = fopen(pm_filename, "r");

        if (NULL == pm_file) {
            fprintf(stderr, "Error opening file: %s\n", pm_filename);
            return (-1);
        }

        i = 0;
        while (EOF != fscanf(pm_file, "%d %d", &plate[i], &mjd[i])) {
            /* printf("%d, %d\n", plate[i], mjd[i]); */
            i++;
        }

        fclose(pm_file);
        count = i;
    }

    

    my_query  = n_query;
    my_count  = count;
    plate_ptr = plate;
    mjd_ptr   = mjd;

#ifdef ENABLE_MPI
    MPI_Bcast( &count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (size > count) {
        if (rank == 0) {
            printf("Number of process is more than plate-mjd combination, exiting...\n");
        }
        MPI_Finalize();
        exit (-1);
    }

    // Distribute work evenly
    my_count = count / size;
    my_query = n_query/ size;

    // Last rank may have extra work
    if (rank == size - 1) {
        my_count += count % size;
        my_query += n_query % size;
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
    plate_ptr = my_plate;
    mjd_ptr   = my_mjd;
    MPI_Barrier(MPI_COMM_WORLD);
    free(displs);
    free(sendcount);
#endif 

    /* printf("%d: myquery = %d\n", rank, my_query); */


    PDC_prop_t p;
    pdcid_t pdc = PDC_init(p);

    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    pdcid_t cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    char data_loc[128];
    char obj_name[128];
    uint64_t dims[2] = {1000, 2};
    PDCprop_set_obj_dims(obj_prop, 2, dims, pdc);

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

    int n_fiber = 1000;
    for (i = 0; i < my_query; i++) {

        // Everyone has 1000 fibers
        for (j = 1; j <= n_fiber; j++) {

            sprintf(obj_name, "%d-%d-%d", plate_ptr[i], mjd_ptr[i], j);
            /* printf("%d: querying %s\n", rank, obj_name); */

            pdc_metadata_t *res = NULL;
            PDC_Client_query_metadata_name_timestep(obj_name, 0, &res);
            if (res == NULL) {
                printf("%d: No result found for current query with name [%s]\n", rank, obj_name);
                exit(-1);
            }
            /* else { */
            /*     PDC_print_metadata(res); */
            /* } */
        }

        // Print progress
        /* int progress_factor = my_count < 10 ? 1 : 10; */
        /* if (i > 0 && i % (my_count/progress_factor) == 0) { */
        /*     gettimeofday(&ht_total_end, 0); */
        /*     ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec; */
        /*     ht_total_sec        = ht_total_elapsed / 1000000.0; */
        /*     if (rank == 0) { */
        /*         printf("%10d created ... %.4f s\n", i * size * j, ht_total_sec); */
        /*         fflush(stdout); */
        /*     } */
        /* } */

    }
    fflush(stdout);
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to query %d obj with %d ranks: %.6f\n", n_query*1000, size, ht_total_sec);
        fflush(stdout);
    }

done:
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);

    if(PDCprop_close(cont_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
