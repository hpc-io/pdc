#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include "string_utils.h"
#include "timer_utils.h"
#include "dart_core.h"

// #define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"

int
main(int argc, char **argv)
{

    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    // println("MPI enabled!\n");
    fflush(stdout);
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // if (rank == 0) {
    pdcid_t pdc = PDCinit("pdc");

    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    pdcid_t cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    int sid = 0;
    // FIXME: This is a hack to make sure that the server is ready to accept the connection.
    for (sid = 0; sid < size; sid++) {
        // server_lookup_connection(sid, 2);
    }

    dart_object_ref_type_t ref_type  = REF_PRIMARY_ID;
    dart_hash_algo_t       hash_algo = DART_HASH;

    char *   key   = "abcd";
    char *   value = "1234";
    uint64_t data  = 12341234;
    // if (rank == 0) {
    PDC_Client_insert_obj_ref_into_dart(hash_algo, key, value, ref_type, data);
    println("[Client_Side_Insert] Insert '%s=%s' for ref %llu", key, value, data);

    // This is for testing exact search
    char *     exact_query = "abcd=1234";
    uint64_t **out1;
    int        rest_count1 = 0;
    PDC_Client_search_obj_ref_through_dart(hash_algo, exact_query, ref_type, &rest_count1, &out1);

    println("[Client_Side_Exact] Search '%s' and get %d results : %llu", exact_query, rest_count1,
            out1[0][0]);

    // This function test is for testing the prefix search
    char *     prefix_query = "ab*=12*";
    uint64_t **out2;
    int        rest_count2 = 0;
    PDC_Client_search_obj_ref_through_dart(hash_algo, prefix_query, ref_type, &rest_count2, &out2);

    println("[Client_Side_Prefix] Search '%s' and get %d results : %llu", prefix_query, rest_count2,
            out2[0][0]);

    // This function test is for testing the suffix search.
    char *     suffix_query = "*cd=*34";
    uint64_t **out3;
    int        rest_count3 = 0;
    PDC_Client_search_obj_ref_through_dart(hash_algo, suffix_query, ref_type, &rest_count3, &out3);

    println("[Client_Side_Suffix] Search '%s' and get %d results : %llu", suffix_query, rest_count3,
            out3[0][0]);

    // This is for testing infix search.
    char *     infix_query = "*bc*=*23*";
    uint64_t **out4;
    int        rest_count4 = 0;
    PDC_Client_search_obj_ref_through_dart(hash_algo, infix_query, ref_type, &rest_count4, &out4);

    println("[Client_Side_Infix] Search '%s' and get %d results : %llu", infix_query, rest_count4,
            out4[0][0]);

    // }

    // done:

    if (PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");
        // }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
