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
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"


static char *rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK...";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            int key = rand() % (int) (sizeof(charset) - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

void print_usage() {
    printf("Usage: srun -n ./creat_obj -r num_of_obj_per_rank\n");
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
    while ((c = getopt (argc, argv, "r:")) != -1)
        switch (c)
        {
         case 'r':
           count = atoi(optarg);
           break;
         case '?':
           if (optopt == 'r')
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

    count /= size;

    if (rank == 0) 
        printf("Querying %d objects per MPI rank\n", count);
    fflush(stdout);

    int i;
    const int metadata_size = 512;
    char obj_name[metadata_size];

    PDC_prop_t p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    pdcid_t cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    pdcid_t test_obj = -1;

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;


    char obj_prefix[4][10] = {"x", "y", "z", "energy"};
    char tmp_str[128];

    int use_name = -1;
    char *env_str = getenv("PDC_OBJ_NAME");
    if (env_str != NULL) {
        use_name = atoi(env_str);
    }

    char name_mode[6][32] = {"Random Obj Names", "INVALID!", "One Obj Name", "INVALID!", "INVALID!", "Four Obj Names"}; 
    if (rank == 0) {
        printf("Using %s\n", name_mode[use_name+1]);
    }


#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_start, 0);

    for (i = 0; i < count; i++) {
        if (use_name == -1) 
            sprintf(obj_name, "%s_", rand_string(tmp_str, 16));
        else if (use_name == 1) 
            sprintf(obj_name, "%s_", obj_prefix[0]);
        else if (use_name == 4) 
            sprintf(obj_name, "%s_", obj_prefix[i%4]);
        else {
            printf("Unsupported name choice\n");
            goto done;
        }

        // Force to send the entire string, not just the first few characters.
        /* obj_name[strlen(obj_name)] = ' '; */

        pdc_metadata_t *res = NULL;
        PDC_Client_query_metadata_with_name(obj_name, &res);
        if (res == NULL) {
            printf("No result found for current query with name [%s]\n", obj_name);
        }
        /* else { */
        /*     printf("Got response from server.\n"); */
        /*     PDC_print_metadata(res); */
        /* } */
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) { 
        printf("Time to query %d obj/rank with %d ranks: %.6f\n", count, size, ht_total_sec);
        fflush(stdout);
    }

done:
    // close a container
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);

    // close a container property
    if(PDCprop_close(cont_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
