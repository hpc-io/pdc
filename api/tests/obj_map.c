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

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    int i;
    const int metadata_size = 512;
    PDC_prop_t p;
    // create a pdc
    pdcid_t pdc_id = PDC_init(p);
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("Create a container property, id is %lld\n", cont_prop); */

    // create a container
    pdcid_t cont_id = PDCcont_create(pdc_id, "c1", cont_prop);
    if(cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("Create a container, id is %lld\n", cont_id); */

    // create an object property
    pdcid_t obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    pdcid_t obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    pdcid_t obj_prop3 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    char obj_name1[512];
    char obj_name2[512];
    char obj_name3[512];

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

	int myArray1[3][3] = { {1, 2, 3}, {4, 5, 6}, {7, 8, 9} };
    int myArray2[3][3];
	int myArray3[3][3];

	char srank[10];
	sprintf(srank, "%d", rank);
	sprintf(obj_name1, "%s%s", rand_string(tmp_str, 16), srank);
	sprintf(obj_name2, "%s%s", rand_string(tmp_str, 16), srank);
	sprintf(obj_name3, "%s%s", rand_string(tmp_str, 16), srank);

	uint64_t dims[2] = {3, 3};
    PDCprop_set_obj_dims(obj_prop1, 2, dims, pdc_id);
    PDCprop_set_obj_dims(obj_prop2, 2, dims, pdc_id);
    PDCprop_set_obj_dims(obj_prop3, 2, dims, pdc_id);

    PDCprop_set_obj_type(obj_prop1, PDC_INT, pdc_id);
    PDCprop_set_obj_type(obj_prop2, PDC_INT, pdc_id);
    PDCprop_set_obj_type(obj_prop3, PDC_INT, pdc_id);

    PDCprop_set_obj_buf(obj_prop1, &myArray1[0][0], pdc_id);
    PDCprop_set_obj_time_step(obj_prop1, rank, pdc_id);
    PDCprop_set_obj_user_id( obj_prop1, getuid(),    pdc_id);
    PDCprop_set_obj_app_name(obj_prop1, "test_app",  pdc_id);
    PDCprop_set_obj_tags(    obj_prop1, "tag0=1",    pdc_id);

	PDCprop_set_obj_buf(obj_prop2, &myArray2[0][0], pdc_id);
    PDCprop_set_obj_time_step(obj_prop2, rank, pdc_id);
    PDCprop_set_obj_user_id( obj_prop2, getuid(),    pdc_id);
    PDCprop_set_obj_app_name(obj_prop2, "test_app",  pdc_id);
    PDCprop_set_obj_tags(    obj_prop2, "tag0=1",    pdc_id);

	PDCprop_set_obj_buf(obj_prop3, &myArray3[0][0], pdc_id);
    PDCprop_set_obj_time_step(obj_prop3, rank, pdc_id);
    PDCprop_set_obj_user_id( obj_prop3, getuid(),    pdc_id);
    PDCprop_set_obj_app_name(obj_prop3, "test_app",  pdc_id);
    PDCprop_set_obj_tags(    obj_prop3, "tag0=1",    pdc_id);

    pdcid_t obj1 = PDCobj_create(pdc_id, cont_id, obj_name1, obj_prop1);
    if (obj1 < 0) { 
        printf("Error getting an object id of %s from server, exit...\n", obj_name1);
        exit(-1);
    }

	pdcid_t obj2 = PDCobj_create(pdc_id, cont_id, obj_name2, obj_prop2);
    if (obj2 < 0) {    
        printf("Error getting an object id of %s from server, exit...\n", obj_name2);
        exit(-1);
    }

	pdcid_t obj3 = PDCobj_create(pdc_id, cont_id, obj_name3, obj_prop3);
    if (obj3 < 0) {
        printf("Error getting an object id of %s from server, exit...\n", obj_name3);
        exit(-1);
    }

	uint64_t offset[2] = {0, 0};
    // create a region
    pdcid_t r1 = PDCregion_create(2, offset, dims, pdc_id);
//    printf("first region id: %lld\n", r1);
    pdcid_t r2 = PDCregion_create(2, offset, dims, pdc_id);
//    printf("second region id: %lld\n", r2);
	pdcid_t r3 = PDCregion_create(2, offset, dims, pdc_id);
//    printf("second region id: %lld\n", r3);

    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif

    gettimeofday(&ht_total_start, 0);

	PDCobj_map(obj1, r1, obj2, r2, pdc_id);
	PDCobj_map(obj1, r1, obj3, r3, pdc_id);
//	PDCobj_map(obj2, r2, obj3, r3, pdc_id);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) { 
//        printf("Time to create %d obj/rank with %d ranks: %.6f\n", count, size, ht_total_sec);
		printf("Time to create obj %.6f\n",  ht_total_sec);
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
    if(PDCcont_close(cont_id, pdc_id) < 0)
        printf("fail to close container %lld\n", cont_id);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("successfully close container # %lld\n", cont); */

    // close a container property
    if(PDCprop_close(cont_prop, pdc_id) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("successfully close container property # %lld\n", cont_prop); */

    if(PDC_close(pdc_id) < 0)
       printf("fail to close PDC\n");
    /* else */
    /*    printf("PDC is closed\n"); */

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
