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
        printf("Update/Delete %d objects per MPI rank\n", count);
    fflush(stdout);

    int i;

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

    pdc_metadata_t new;
    new.time_step = -1;
    strcpy(new.app_name, "updated_app_name");
    strcpy(new.data_location, "updated_obj_data_location");
    strcpy(new.tags, "updated_tags");
    srand(rank+1);

    char name_mode[6][32] = {"Random Obj Names", "INVALID!", "One Obj Name", "INVALID!", "INVALID!", "Four Obj Names"};
    if (rank == 0) {
        printf("Using %s\n", name_mode[use_name+1]);
    }

    const int metadata_size = 512;
    char **obj_names = (char**)malloc(count * sizeof(char*));
    int  *obj_ts     = (int*)  malloc(count * sizeof(int)  );
    for (i = 0; i < count; i++) {
        obj_names[i] = (char*)malloc(128*sizeof(char));
    }
    char filename[128], pdc_server_tmp_dir_g[128];
    int n_entry;
    // Set up tmp dir
    char *tmp_dir = getenv("PDC_TMPDIR");
    if (tmp_dir == NULL)
        strcpy(pdc_server_tmp_dir_g, "./pdc_tmp");
    else
        strcpy(pdc_server_tmp_dir_g, tmp_dir);

    sprintf(filename, "%s/metadata_checkpoint.%d", pdc_server_tmp_dir_g, rank);

    /* printf("file name: %s\n", filename); */
    FILE *file = fopen(filename, "r");
    if (file==NULL) {fputs("Checkpoint file not available\n", stderr); return -1;}

    fread(&n_entry, sizeof(int), 1, file);
    /* printf("%d entries\n", n_entry); */

    pdc_metadata_t entry;
    uint32_t *hash_key;
    int j, read_count = 0, tmp_count;
    while (n_entry > 0) {
        fread(&tmp_count, sizeof(int), 1, file);
        /* printf("Count:%d\n", tmp_count); */

        hash_key = (uint32_t *)malloc(sizeof(uint32_t));
        fread(hash_key, sizeof(uint32_t), 1, file);
        /* printf("Hash key is %u\n", *hash_key); */


         // read each metadata
         for (j = 0; j < tmp_count; j++) {
              if (read_count >= count) {
                  n_entry = 0;
                  break;
              }
             fread(&entry, sizeof(pdc_metadata_t), 1, file);
             sprintf(obj_names[read_count], "%s", entry.obj_name);
             obj_ts[read_count] = entry.time_step;

             /* printf("Read name %s\n", obj_names[read_count]); */
             read_count++;

         }
         n_entry--;
     }


    fclose(file);

    count = read_count;


#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_start, 0);
    for (i = 0; i < count; i++) {
        /* if (use_name == -1) */
        /*     sprintf(obj_name, "%s_%d", rand_string(tmp_str, 16), rank); */
        /* else if (use_name == 1) */
        /*     sprintf(obj_name, "%s_%d", obj_prefix[0], i + rank * count); */
        /* else if (use_name == 4) */
        /*     sprintf(obj_name, "%s_%d", obj_prefix[i%4], i/4 + rank * count); */
        /* else { */
        /*     printf("Unsupported name choice\n"); */
        /*     goto done; */
        /* } */

        pdc_metadata_t *res = NULL;
        /* printf("Querying metadata with name [%s]\n", obj_names[i]); */
        PDC_Client_query_metadata_name_timestep(obj_names[i], obj_ts[i], &res);
        if (res == NULL) {
            printf("No result found for current query with name [%s]\n", obj_names[i]);
        }
        else {
            /* printf("Got response from server.\n"); */
            /* PDC_print_metadata(res); */

            /* printf("Updating Metadata\n"); */
            PDC_Client_update_metadata(res, &new);

            /* printf("Querying new Metadata\n"); */
            /* PDC_Client_query_metadata_with_name(obj_names[i], &res); */
            /* if (res == NULL) */ 
            /*     printf("No result found for current query with name [%s]\n", obj_names[i]); */
            /* else { */
            /*     PDC_print_metadata(res); */
            /* } */

        }

        if (rank == 0) {
            // Print progress
            int progress_factor = count < 20 ? 1 : 20;
            if (rank == 0 && i > 0 && i % (count/progress_factor) == 0) {
                gettimeofday(&ht_total_end, 0);
                ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
                ht_total_sec        = ht_total_elapsed / 1000000.0;

                printf("updated %10d ... %.2f\n", i * size, ht_total_sec);
                fflush(stdout);
            }
        }
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) { 
        printf("Time to update %d obj/rank with %d ranks: %.6f\n\n\n", count, size, ht_total_sec);
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
