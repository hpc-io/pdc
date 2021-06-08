#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/time.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

void
print_usage()
{
    printf("Usage: srun -n ./write_obj obj_name size_MB type\n");
}

int
main(int argc, char **argv)
{
    int      rank = 0, size = 1;
    uint64_t size_MB, size_B;
    perr_t   ret;
    int      ndim      = 1;
    int      ret_value = 0;
#ifdef ENABLE_MPI
    MPI_Comm comm;
#else
    int comm = 1;
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
    char *    mydata, *obj_data;
    char      obj_name[128], cont_name[128];

    uint64_t my_data_size;
    uint64_t dims[1];

    pdc_var_type_t var_type  = PDC_UNKNOWN;
    size_t         type_size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
#endif

    if (argc != 4) {
        print_usage();
        ret_value = 1;
#ifdef ENABLE_MPI
        MPI_Finalize();
#endif
        return ret_value;
    }

    sprintf(obj_name, "%s", argv[1]);

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

    printf("Writing a %" PRIu64 " MB object [%s] with %d clients.\n", size_MB, obj_name, size);
    // size_B = 1;
    size_B = size_MB * 1048576;

    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);
    if (cont <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0) {
        printf("Fail to create object property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    dims[0]      = size_B;
    my_data_size = size_B / size;
    printf("my_data_size at rank %d is %llu\n", rank, (long long unsigned)my_data_size);

    obj_data = (char *)malloc(my_data_size * type_size);
    mydata   = (char *)malloc(my_data_size * type_size);

    PDCprop_set_obj_type(obj_prop, var_type);
    PDCprop_set_obj_buf(obj_prop, obj_data);
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // Create a object
    global_obj = PDCobj_create_mpi(cont, obj_name, obj_prop, 0, comm);
    // global_obj = PDCobj_create(cont, obj_name, obj_prop);

    if (global_obj <= 0) {
        printf("Error creating an object [%s], exit...\n", obj_name);
        ret_value = 1;
    }

    offset       = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize       = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    local_offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);

    offset[0]       = rank * my_data_size;
    local_offset[0] = 0;
    mysize[0]       = my_data_size;
    printf("rank %d offset = %lu, length = %lu, unit size = %ld\n", rank, offset[0], mysize[0], type_size);

    local_region  = PDCregion_create(ndim, local_offset, mysize);
    global_region = PDCregion_create(ndim, offset, mysize);

    for (i = 0; i < (int)my_data_size; i++) {
        for (j = 0; j < (int)type_size; ++j) {
            mydata[i * type_size + j] = i;
        }
    }
    ret = PDCbuf_obj_map(mydata, var_type, local_region, global_obj, global_region);
    if (ret != SUCCEED) {
        printf("PDCbuf_obj_map failed\n");
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&pdc_timer_start, 0);
    ret = PDCreg_obtain_lock(global_obj, global_region, PDC_WRITE, PDC_BLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region\n");
        ret_value = 1;
        goto done;
    }

    ret = PDCreg_release_lock(global_obj, global_region, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region\n");
        ret_value = 1;
        goto done;
    }

    ret = PDCbuf_obj_unmap(global_obj, global_region);
    if (ret != SUCCEED) {
        printf("PDCbuf_obj_unmap failed\n");
        ret_value = 1;
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&pdc_timer_end, 0);
    write_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    if (rank == 0) {
        printf("Time to lock and release data with %d ranks: %.6f\n", size, write_time);
        fflush(stdout);
    }
done:
    if (PDCobj_close(global_obj) < 0) {
        printf("fail to close global obj\n");
        ret_value = 1;
    }

    if (PDCregion_close(local_region) < 0) {
        printf("fail to close local region\n");
        ret_value = 1;
    }

    if (PDCregion_close(global_region) < 0) {
        printf("fail to close global region\n");
        ret_value = 1;
    }
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container\n");
        ret_value = 1;
    }
    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    if (PDCclose(pdc) < 0) {
        printf("fail to close PDC\n");
        ret_value = 1;
    }
    free(obj_data);
    free(mydata);
    free(offset);
    free(local_offset);
    free(mysize);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return ret_value;
}
