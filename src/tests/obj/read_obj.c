#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/time.h>
#include "pdc.h"
#include "pdc_client_server_common.h"
#include "test_helper.h"

void
print_usage()
{
    LOG_JUST_PRINT("Usage: srun -n ./write_obj obj_name size_MB type\n");
}

int
main(int argc, char **argv)
{
    int      rank = 0, size = 1;
    uint64_t size_MB, size_B;
    perr_t   ret;
    int      ndim      = 1;
    int      ret_value = TSUCCEED;
#ifdef ENABLE_MPI
    MPI_Comm comm;
#else
    int comm   = 1;
#endif
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         write_time = 0.0;
    pdcid_t        global_obj = 0;
    pdcid_t        local_region, global_region;
    pdcid_t        pdc, cont_prop, cont, obj_prop;

    uint64_t *offset, *local_offset;
    uint64_t *mysize;
    int       i, j;
    char *    mydata;
    char      obj_name[128], cont_name[128];

    uint64_t my_data_size;
    uint64_t dims[1];

    pdc_var_type_t var_type  = PDC_UNKNOWN;
    size_t         type_size = 1;

    pdcid_t transfer_request;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
#endif

    if (argc != 4) {
        print_usage();
#ifdef ENABLE_MPI
        MPI_Finalize();
#endif
        TGOTO_DONE(TFAIL);
    }

    snprintf(obj_name, sizeof(obj_name), "%s_%d", argv[1], rank);

    size_MB = atoi(argv[2]);

    if (!strcmp(argv[3], "float")) {
        var_type  = PDC_FLOAT;
        type_size = sizeof(float);
    }
    else if (!strcmp(argv[3], "int")) {
        var_type  = PDC_INT;
        type_size = sizeof(int);
    }
    else if (!strcmp(argv[3], "double")) {
        var_type  = PDC_DOUBLE;
        type_size = sizeof(double);
    }
    else if (!strcmp(argv[3], "char")) {
        var_type  = PDC_CHAR;
        type_size = sizeof(char);
    }
    else if (!strcmp(argv[3], "uint")) {
        var_type  = PDC_UINT;
        type_size = sizeof(unsigned);
    }
    else if (!strcmp(argv[3], "int64")) {
        var_type  = PDC_INT64;
        type_size = sizeof(int64_t);
    }
    else if (!strcmp(argv[3], "uint64")) {
        var_type  = PDC_UINT64;
        type_size = sizeof(uint64_t);
    }
    else if (!strcmp(argv[3], "int16")) {
        var_type  = PDC_INT16;
        type_size = sizeof(int16_t);
    }
    else if (!strcmp(argv[3], "int8")) {
        var_type  = PDC_INT8;
        type_size = sizeof(int8_t);
    }

    LOG_INFO("Writing a %" PRIu64 " MB object [%s] with %d clients.\n", size_MB, obj_name, size);
    // size_B = 1;
    size_B = size_MB * 1048576;

    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");

    // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
    sprintf(cont_name, "c%d", rank);
    TASSERT((cont = PDCcont_create_col(cont_name, cont_prop)) != 0, "Call to PDCcont_create_col succeeded",
            "Call to PDCcont_create_col failed");
    // create an object property
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    dims[0]      = size_B;
    my_data_size = size_B / size;
    LOG_INFO("my_data_size at rank %d is %llu\n", rank, (long long unsigned)my_data_size);

    mydata = (char *)malloc(my_data_size * type_size);

    TASSERT(PDCprop_set_obj_type(obj_prop, var_type) >= 0, "Call to PDCprop_set_obj_type succeeded",
            "Call to PDCprop_set_obj_type failed");
    TASSERT(PDCprop_set_obj_dims(obj_prop, 1, dims) >= 0, "Call to PDCprop_set_obj_dims succeeded",
            "Call to PDCprop_set_obj_dims failed");
    TASSERT(PDCprop_set_obj_user_id(obj_prop, getuid()) >= 0, "Call to PDCprop_set_obj_user_id succeeded",
            "Call to PDCprop_set_obj_user_id failed");
    TASSERT(PDCprop_set_obj_time_step(obj_prop, 0) >= 0, "Call to PDCprop_set_obj_time_step succeeded",
            "Call to PDCprop_set_obj_time_step failed");
    TASSERT(PDCprop_set_obj_app_name(obj_prop, "DataServerTest") >= 0,
            "Call to PDCprop_set_obj_user_id succeeded", "Call to PDCprop_set_obj_user_id failed");
    TASSERT(PDCprop_set_obj_tags(obj_prop, "tag0=1") >= 0, "Call to PDCprop_set_obj_tags succeeded",
            "Call to PDCprop_set_obj_tags failed");

    // Create a object
#ifdef ENABLE_MPI
    global_obj = PDCobj_create_mpi(cont, obj_name, obj_prop, 0, comm);
#else
    global_obj = PDCobj_create(cont, obj_name, obj_prop);
#endif

    if (global_obj <= 0)
        TGOTO_ERROR(TFAIL, "Error creating an object [%s]\n", obj_name);

    offset          = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    local_offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize          = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0]       = rank * my_data_size;
    local_offset[0] = 0;
    mysize[0]       = my_data_size;

    TASSERT((local_region = PDCregion_create(ndim, local_offset, mysize)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");
    TASSERT((global_region = PDCregion_create(ndim, offset, mysize)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    for (i = 0; i < (int)my_data_size; i++) {
        for (j = 0; j < (int)type_size; ++j) {
            mydata[i * type_size + j] = i;
        }
    }

    TASSERT((transfer_request =
                 PDCregion_transfer_create(mydata, PDC_WRITE, global_obj, local_region, global_region)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&pdc_timer_start, 0);
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&pdc_timer_end, 0);
    write_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    if (rank == 0)
        LOG_INFO("Time to process write data with %d ranks: %.5e\n", size, write_time);

    TASSERT((transfer_request =
                 PDCregion_transfer_create(mydata, PDC_READ, global_obj, local_region, global_region)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&pdc_timer_start, 0);
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    for (i = 0; i < (int)my_data_size; i++) {
        for (j = 0; j < (int)type_size; ++j) {
            if (mydata[i * type_size + j] != (char)i) {
                LOG_ERROR("Wrong value detected %d != %d\n", mydata[i * type_size + j], i);
                ret_value = 1;
                i         = my_data_size;
                break;
            }
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&pdc_timer_end, 0);
    write_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    if (rank == 0)
        LOG_INFO("Time to process read data with %d ranks: %.5e\n", size, write_time);

    free(offset);
    free(local_offset);
    free(mysize);
    free(mydata);

    TASSERT(PDCobj_close(global_obj) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCregion_close(local_region) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");
    TASSERT(PDCregion_close(global_region) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return ret_value;
}
