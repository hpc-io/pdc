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

static char *rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
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
        printf("Creating %d objects per MPI rank\n", count);
    fflush(stdout);

    int i;
    const int metadata_size = 512;
    struct PDC_prop p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("Create a container property, id is %lld\n", cont_prop); */

    // create a container
    pdcid_t cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("Create a container, id is %lld\n", cont); */

    // create an object property
    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("Create an object property, id is %lld\n", obj_prop); */

    uint64_t dims[3] = {100, 200, 700};
    PDCprop_set_obj_dims(obj_prop, 3, dims, pdc);

    pdcid_t test_obj = -1;

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;


    char obj_name[512];


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

    srand(rank+1);


    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif

    gettimeofday(&ht_total_start, 0);

    for (i = 0; i < count; i++) {

        if (use_name == -1) {
            sprintf(obj_name, "%s", rand_string(tmp_str, 16));
            PDCprop_set_obj_time_step(obj_prop, rank, pdc);
            /* sprintf(obj_name[i], "%s_%d", rand_string(tmp_str, 16), i + rank * count); */
        }
        else if (use_name == 1) {
            sprintf(obj_name, "%s", obj_prefix[0]);
            PDCprop_set_obj_time_step(obj_prop, i + rank * count, pdc);
        }
        else if (use_name == 4) {
            sprintf(obj_name, "%s", obj_prefix[i%4]);
            PDCprop_set_obj_time_step(obj_prop, i/4 + rank * count, pdc);
        }
        else {
            printf("Unsupported name choice\n");
            goto done;
        }
        PDCprop_set_obj_user_id( obj_prop, getuid(),    pdc);
        PDCprop_set_obj_app_name(obj_prop, "test_app",  pdc);
        PDCprop_set_obj_tags(    obj_prop, "tag0=1",    pdc);

        /* if (count < 4) { */
        /*     printf("Proc %d Name: %s\n", rank, obj_name); */
        /* } */
        test_obj = PDCobj_create(pdc, cont, obj_name, obj_prop);
        if (test_obj < 0) { 
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            exit(-1);
        }
        /* else */ 
        /*     printf("%lld\n", test_obj); */

        // Print progress
        int progress_factor = count < 10 ? 1 : 10;
        if (i > 0 && i % (count/progress_factor) == 0) {
            gettimeofday(&ht_total_end, 0);
            ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
            ht_total_sec        = ht_total_elapsed / 1000000.0;
            if (rank == 0) {
                printf("%10d created ... %.4f s\n", i * size, ht_total_sec);
                fflush(stdout);
            }
            #ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
            #endif
        }

    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) { 
        printf("Time to create %d obj/rank with %d ranks: %.6f\n", count, size, ht_total_sec);
        fflush(stdout);
    }

    /* // Check for duplicate insertion */
    /* int dup_obj_id; */
    /* sprintf(obj_name, "%s_%d", obj_prefix[0], rank * 10000000); */
    /* dup_obj_id = PDCobj_create(pdc, obj_name, NULL); */
    /* int all_dup_obj_id; */

/* #ifdef ENABLE_MPI */
    /* MPI_Reduce(&dup_obj_id, &all_dup_obj_id, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD); */  
/* #else */
    /* all_dup_obj_id = dup_obj_id; */
/* #endif */

    /* if (rank == 0) { */
    /*     if (all_dup_obj_id>=0 ) */ 
    /*         printf("Duplicate insertion test failed!\n"); */
    /*     else */ 
    /*         printf("Duplicate insertion test succeed!\n"); */
    /* } */

done:

    // close a container
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("successfully close container # %lld\n", cont); */

    // close a container property
    if(PDCprop_close(cont_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("successfully close container property # %lld\n", cont_prop); */

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    /* else */
    /*    printf("PDC is closed\n"); */

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
