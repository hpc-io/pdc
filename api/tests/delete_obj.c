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
    printf("Usage: srun -n ./delete_obj -r total_objects_to_create_delete\n");
}

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1, i;
    int count = -1;
    char c;
    pdcid_t pdc, cont_prop, cont, obj_prop;
    pdcid_t *create_obj_ids;
    int use_name = -1;
    char obj_prefix[4][10] = {"x", "y", "z", "energy"};
    char tmp_str[128];
    char *env_str;
    char name_mode[6][32] = {"Random Obj Names", "INVALID!", "One Obj Name", "INVALID!", "INVALID!", "Four Obj Names"};
    char obj_name[32];
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

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
        printf("Delete %d objects per MPI rank\n", count);
    fflush(stdout);

    // create a pdc
    pdc = PDC_init("pdc");
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    create_obj_ids = (pdcid_t*)malloc(sizeof(pdcid_t) * count);

    env_str = getenv("PDC_OBJ_NAME");
    if (env_str != NULL) {
        use_name = atoi(env_str);
    }

    if (rank == 0) {
        printf("Using %s\n", name_mode[use_name+1]);
        fflush(stdout);
    }

    srand(rank+1);


#ifdef ENABLE_MPI
MPI_Barrier(MPI_COMM_WORLD);
#endif

    // This test first creates a number of objects, then delete them one by one.
    for (i = 0; i < count; i++) {

        if (use_name == -1) {
            sprintf(obj_name, "%s", rand_string(tmp_str, 16));
            PDCprop_set_obj_time_step(obj_prop, rank);
            /* sprintf(obj_name[i], "%s_%d", rand_string(tmp_str, 16), i + rank * count); */
        }
        else if (use_name == 1) {
            sprintf(obj_name, "%s", obj_prefix[0]);
            PDCprop_set_obj_time_step(obj_prop, i + rank * count);
        }
        else if (use_name == 4) {
            sprintf(obj_name, "%s", obj_prefix[i%4]);
            PDCprop_set_obj_time_step(obj_prop, i/4 + rank * count);
        }
        else {
            printf("Unsupported name choice\n");
            goto done;
        }
        PDCprop_set_obj_user_id( obj_prop, getuid()    );
        PDCprop_set_obj_app_name(obj_prop, "obj_delete_test");
        PDCprop_set_obj_tags(    obj_prop, "tag0=1");

        // Create object
        create_obj_ids[i] = PDCobj_create(cont, obj_name, obj_prop);
        if (create_obj_ids[i] < 0 ) {
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            exit(-1);
        }
    }

#ifdef ENABLE_MPI
MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Delete and check if success
    /* for (i = 0; i < count/2; i++) { */
    for (i = 0; i < count; i++) {
        /* printf("Proc %d: deleting metadata\n", rank); */
        /* fflush(stdout); */
        perr_t ret;
        ret = PDC_Client_delete_metadata_by_id(pdc, cont, create_obj_ids[i]);
        if (ret != SUCCEED) {
            printf("Delete fail with process %d, exiting\n", rank);
            goto done;
        }
        /* // Check by querying */
        /* pdc_metadata_t *res = NULL; */
        /* /1* printf("Proc %d: querying metadata with name [%s]\n", rank, obj_name); *1/ */
        /* PDC_Client_query_metadata_with_name(obj_names[i], &res); */
        /* if (res != NULL) { */
        /*     printf("Deletion FAIL! Metadata of [%s] still exist in server\n", res->obj_names); */
        /*     fflush(stdout); */
        /* } */
    }
    if (rank == 0) {
        printf("Delete test SUCCEED.\n");
    }

done:
    // close a container
    if(PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    // close a container property
    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close() < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
