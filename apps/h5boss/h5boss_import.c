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


void print_usage() {
    printf("Usage: srun -n ./import_h5boss /path/to/pm_list.txt\n");
}

int main(int argc, char **argv)
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    printf("MPI enabled!\n");
    fflush(stdout);
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    const char *pm_filename = argv[1];

    if (pm_filename == NULL) {
        print_usage();
        exit(-1);
    }

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

    // Last rank may have extra work
    if (rank == size - 1) 
        my_count += count % size;

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

    /* printf("mycount = %d\n", my_count); */


    pdcid_t pdc = PDC_init("pdc");

    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    pdcid_t cont = PDCcont_create("c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    char data_loc[128];
    char obj_name[128];
    uint64_t dims[2] = {1000, 2};
    PDCprop_set_obj_dims(obj_prop, 2, dims);

    pdcid_t test_obj = -1;

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    if (rank == 0) {
        printf("Starting to import h5boss metadata...\n");
    }

    PDCprop_set_obj_user_id( obj_prop, getuid());
    PDCprop_set_obj_time_step( obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "H5BOSS");
    PDCprop_set_obj_tags(    obj_prop, "tag0=1");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_start, 0);

    int n_fiber = 1000;
    for (i = 0; i < my_count; i++) {
        sprintf(data_loc, "/global/cscratch1/sd/jialin.old/h5boss/%d-%d.hdf5", plate_ptr[i], mjd_ptr[i]);
        PDCprop_set_obj_data_loc(obj_prop, data_loc);

        /* printf("%d: creating %d-%d\n", rank, plate_ptr[i], mjd_ptr[i]); */
        // Everyone has 1000 fibers
        for (j = 1; j <= n_fiber; j++) {

            sprintf(obj_name, "%d-%d-%d", plate_ptr[i], mjd_ptr[i], j);

            test_obj = PDCobj_create(cont, obj_name, obj_prop);
            if (test_obj < 0) {
                printf("Error getting an object id of %s from server, exit...\n", obj_name);
                exit(-1);
            }
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
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to create %d obj with %d ranks: %.6f\n", count*1000, size, ht_total_sec);
        fflush(stdout);
    }

done:
    if(PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
