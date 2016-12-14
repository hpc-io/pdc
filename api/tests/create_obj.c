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
        printf("Creating %d objects per MPI rank\n", count);
    fflush(stdout);

    PDC_prop_t p;
    pdcid_t pdc = PDC_init(p);

    pdcid_t test_obj = -1;

    int i;
    const int metadata_size = 512;
    char obj_name[metadata_size];
    for (i = 0; i < metadata_size; i++) {
        obj_name[i] = ' ';
    }
    obj_name[metadata_size - 4] = 'T';
    obj_name[metadata_size - 3] = 'a';
    obj_name[metadata_size - 2] = 'n';
    obj_name[metadata_size - 1] = 'g';
    obj_name[metadata_size    ] = 0;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    gettimeofday(&ht_total_start, 0);

    char obj_prefix[4][10] = {"x", "y", "z", "energy"};
    char tmp_str[128];

    int use_name = -1;
    char *env_str = getenv("PDC_OBJ_NAME");
    if (env_str != NULL) {
        use_name = atoi(env_str);
        if (rank == 0) {
            printf("use_name=%d\n", use_name);
        }
    }

    for (i = 0; i < count; i++) {
        if (use_name == -1) 
            sprintf(obj_name, "%s_%d", rand_string(tmp_str, 8), i + rank * 10000000);
        else if (use_name == 1) 
            sprintf(obj_name, "%s_%d", obj_prefix[0], i + rank * 10000000);
        else if (use_name == 4) 
            sprintf(obj_name, "%s_%d", obj_prefix[i%4], i/4 + rank * 10000000);
        else {
            printf("Unsupported name choice\n");
            goto done;
        }

        // Force to send the entire string, not just the first few characters.
        /* obj_name[strlen(obj_name)] = ' '; */

        test_obj = PDCobj_create(pdc, obj_name, NULL);
        if (test_obj < 0) { 
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            exit(-1);
        }

        // Print progress
        int progress_factor = count < 10 ? 1 : 10;
        if (rank == 0 && i > 0 && i % (count/progress_factor) == 0) {
            gettimeofday(&ht_total_end, 0);
            ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
            ht_total_sec        = ht_total_elapsed / 1000000.0;

            printf("%10d created ... %.2fs\n", i * size, ht_total_sec);
            fflush(stdout);
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

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");
    /* else */
    /*    printf("PDC is closed\n"); */

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
