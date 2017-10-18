#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <unistd.h> 
#include <sys/types.h>
#include <sys/time.h>

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"
/* #include "pdc_client_server_common.h" */

void print_usage() {
    printf("Usage: srun -n ./data_server_read obj_name size_MB n_timestep sleepseconds\n");
}

int main(int argc, char **argv)
{
    int rank = 0, size = 1, ntimestep = 1, i, ts;
    float sleepseconds;
    uint64_t size_MB, size_B;
    PDC_Request_t request;
    pdc_metadata_t *metadata;
    perr_t ret_value = SUCCEED;

    struct timeval  total_start;
    struct timeval  total_end;
    struct timeval  wait_start;
    struct timeval  wait_end;
    struct timeval  meta_start;
    struct timeval  meta_end;
    long long total_elapsed, wait_elapsed, meta_elapsed;
    double total_wait_sec = 0.0, total_meta_sec = 0.0;


#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (argc < 5) {
        print_usage();
        return 0;
    }

    char *obj_name = argv[1];
    size_MB   = atoi(argv[2]);
    ntimestep = atoi(argv[3]);
    sleepseconds = atof(argv[4]);

    long microseconds = (long)(sleepseconds * 1000000);

    if (rank == 0) {
        printf("Writing a %lu MB object [%s]: %d timesteps,  %.2fs sleep time, and %d clients.\n", 
                        size_MB,      obj_name, ntimestep,       sleepseconds,    size);
    }
    size_B = size_MB * 1048576;

    // create a pdc
    pdcid_t pdc = PDC_init("pdc");
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        goto done;
    }

    // create a container
    pdcid_t cont = PDCcont_create("c1", cont_prop);
    if(cont <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        goto done;
    }

    // create an object property
    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0) {
        printf("Fail to create object property @ line  %d!\n", __LINE__);
        goto done;
    }

    pdcid_t test_obj = -1;
    uint64_t my_data_size = size_B / size;

    int ndim = 1;
    uint64_t dims[1]={size_B};
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id( obj_prop, getuid());
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(    obj_prop, "tag0=1");

    struct PDC_region_info region;
    region.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim);


    region.ndim = ndim;
    region.offset[0] = rank * my_data_size;
    region.size[0] = my_data_size;

    char *mydata = (char*)malloc(my_data_size);
    memset(mydata, 'A' + rank%26, my_data_size);
    for (i = 0; i < 5; i++) {
        mydata[i+1] = (mydata[i] + 3) % 26;
    }


#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Timing
    gettimeofday(&total_start, 0);

    for (ts = 0; ts < ntimestep; ts++) {

        gettimeofday(&meta_start, 0);

        PDCprop_set_obj_time_step(obj_prop, ts);

        // Create a object with only rank 0
        if (rank == 0) {
            printf("Creating an object with name [%s], timestep %u\n", obj_name, ts);
            /* fflush(stdout); */
            test_obj = PDCobj_create(cont, obj_name, obj_prop);
            if (test_obj <= 0) {
                printf("Error getting an object id of %s from server, exit...\n", "DataServerTestBin");
                exit(-1);
            }
        }

        /* printf("%d: object created.\n", rank); */
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        // Query the created object
        if (rank == 0) 
            printf("%d: Start to query object just created ...", rank);

        PDC_Client_query_metadata_name_timestep( obj_name, ts, &metadata);
        /* if (rank == 1) { */
        /*     PDC_print_metadata(metadata); */
        /* } */
        if (metadata == NULL || metadata->obj_id == 0) {
            printf("[%d]: Error with metadata!\n", rank);
            exit(-1);
        }
        /* printf("%d: writing to (%llu, %llu) of %llu bytes\n", rank, region.offset[0], region.offset[1], region.size[0]*region.size[1]); */
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif
        if (rank == 0) 
            printf(" done\n", rank);

        gettimeofday(&meta_end, 0);
        meta_elapsed = (meta_end.tv_sec-meta_start.tv_sec)*1000000LL + 
                           meta_end.tv_usec-meta_start.tv_usec;
        total_meta_sec += meta_elapsed / 1000000.0;



        if (rank == 0) {
            printf("Sleep %.2f seconds.\n", sleepseconds);
            fflush(stdout);
        }

        // Fake computation
        usleep(microseconds);


        if (ts != 0) {
            gettimeofday(&wait_start, 0);
            // Wait for previous write completion before writting current timestep
            ret_value = PDC_Client_wait(&request, 60000, 100);
            if (ret_value != SUCCEED) {
                printf("==PDC_CLIENT: PDC_Client_write - PDC_Client_wait error\n");
                goto done;
            }

#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
#endif

            gettimeofday(&wait_end, 0);
            if (rank == 0) {

                wait_elapsed    = (wait_end.tv_sec-wait_start.tv_sec)*1000000LL + 
                                       wait_end.tv_usec-wait_start.tv_usec;
                total_wait_sec += wait_elapsed / 1000000.0;
                printf("Timestep %d written, metadata %.2f s, wait %.2f s.\n", 
                                 ts,   meta_elapsed / 1000000.0, wait_elapsed / 1000000.0);
                fflush(stdout);
            }
        }

        /* PDC_Client_write_wait_notify(metadata, &region, mydata); */

        /* if (rank == 0) */ 
        /*     printf("Before iwrite\n"); */
        /* fflush(stdout); */

        // (pdc_metadata_t *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf)
        ret_value = PDC_Client_iwrite(metadata, &region, &request, mydata);
        if (ret_value != SUCCEED) {
            printf("[%d] Error with PDC_Client_iwrite!\n", rank);
            goto done;
        }

        /* if (rank == 0) */ 
        /*     printf("After iwrite\n"); */
        /* fflush(stdout); */


    } /* End of for ntimestep */

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Timing
    gettimeofday(&total_end, 0);
    total_elapsed    = (total_end.tv_sec-total_start.tv_sec)*1000000LL + 
                           total_end.tv_usec-total_start.tv_usec;

    if (rank == 0) { 
        printf("Total time write %d ts data each %luMB with %d ranks: %.6f, meta %.2f, wait %.2f, sleep %.2f\n", 
                                ntimestep,            size_MB,   size,   total_elapsed / 1000000.0,
                                                                      total_meta_sec, total_wait_sec, 
                                                                                        sleepseconds*ntimestep);
        fflush(stdout);
    }

done:
    // close a container
    if(PDCcont_close(cont) < 0)
        printf("fail to close container %ld\n", cont);

    // close a container property
    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
