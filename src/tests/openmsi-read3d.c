
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <ctype.h>

#include "mpi.h"
#include "hdf5.h"
#include "pdc.h"

/* Hyperslab layout styles */
#define BYROW     1 /* divide into slabs of rows */
#define BYCOL     2 /* divide into blocks of columns */
#define BYLASTDIM 3 /* For 3D and higher, we get contiguous blocks */
#define ZROW      4 /* same as BYCOL except process 0 gets 0 rows */
#define ZCOL      5 /* same as BYCOL except process 0 gets 0 columns */

/* File_Access_type bits */
#define FACC_DEFAULT 0x0 /* default */
#define FACC_MPIO    0x1 /* MPIO */
#define FACC_SPLIT   0x2 /* Split File */

#define DXFER_COLLECTIVE_IO  0x1 /* Collective IO*/
#define DXFER_INDEPENDENT_IO 0x2 /* Independent IO collectively */

#ifndef FALSE
#define FALSE 0x0
#endif
#ifndef TRUE
#define TRUE 0x1
#endif

#define SHOW_PROGRESS    1
#define SMALLER_DATASIZE 1

static char *default_dataset = "testd";
static char *default_group   = "testg";

static void
display_usage(char *app)
{
    printf("Usage:  %s <netcdf-filename> [dataset-identifier]\n", app);
}

/*
 * Create the appropriate File access property list
 * Utility function from HDF parallel testing
 */
static hid_t
create_faccess_plist(MPI_Comm comm, MPI_Info info, int l_facc_type)
{
    hid_t  ret_pl = -1;
    herr_t ret;

    ret_pl = H5Pcreate(H5P_FILE_ACCESS);
    if (ret_pl < 1) {
        fprintf(stderr, "H5Pcreate(1) failed\n");
        goto error;
    }

    if (l_facc_type == FACC_DEFAULT)
        return (ret_pl);

    if (l_facc_type == FACC_MPIO) {
        /* set Parallel access with communicator */
        ret = H5Pset_fapl_mpio(ret_pl, comm, info);
        if (ret < 0) {
            fprintf(stderr, "H5Pset_fapl_mpio failed\n");
            goto error;
        }
        ret = H5Pset_all_coll_metadata_ops(ret_pl, TRUE);
        if (ret < 0) {
            fprintf(stderr, "H5Pset_all_coll_metadata_ops failed\n");
            goto error;
        }
        ret = H5Pset_coll_metadata_write(ret_pl, TRUE);
        if (ret < 0) {
            fprintf(stderr, "H5Pset_coll_metadata_write failed\n");
            goto error;
        }
        return (ret_pl);
    }

    if (l_facc_type == (FACC_MPIO | FACC_SPLIT)) {
        hid_t mpio_pl;

        mpio_pl = H5Pcreate(H5P_FILE_ACCESS);
        if (mpio_pl < 0) {
            fprintf(stderr, "H5Pcreate(2) failed\n");
            goto error;
        }
        /* set Parallel access with communicator */
        ret = H5Pset_fapl_mpio(mpio_pl, comm, info);
        if (ret < 0) {
            fprintf(stderr, "H5Pset_fapl_mpio failed\n");
            goto error;
        }
        /* setup file access template */
        ret_pl = H5Pcreate(H5P_FILE_ACCESS);
        if (ret_pl < 0) {
            fprintf(stderr, "H5Pcreate(3) failed\n");
            goto error;
        }
        /* set Parallel access with communicator */
        ret = H5Pset_fapl_split(ret_pl, ".meta", mpio_pl, ".raw", mpio_pl);
        if (ret < 0) {
            fprintf(stderr, "H5Pset_fapl_split failed\n");
            goto error;
        }
        H5Pclose(mpio_pl);
        return (ret_pl);
    }
error:
    /* unknown file access types */
    return (ret_pl);
}

int
do_pdc_analysis_for_output(float *data, size_t dims[3], pdc_var_type_t data_type, int mpi_rank)
{
    perr_t  ret = SUCCEED;
    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t s3d_prop, s3d_result_prop;
    pdcid_t input_object, output_object;
    pdcid_t input3d_iter, output3d_iter;
    pdcid_t rin, rout;

    uint64_t offsets3d[3] = {0, 0, 0};

    char app_name[256];
    char obj_name[256];

    // initialize pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);

    // create a container (collectively)
    cont_id = PDCcont_create_col("c1", cont_prop);
    if (cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create object properties for input and results
    s3d_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

#ifdef ENABLE_MPI
    sprintf(app_name, "S3D_analysis_example.%d", mpi_rank);
#else
    strcpy(app_name, "arrayudf_example");
#endif

    PDCprop_set_obj_dims(s3d_prop, 3, dims);
    PDCprop_set_obj_type(s3d_prop, data_type);
    PDCprop_set_obj_time_step(s3d_prop, 0);
    PDCprop_set_obj_user_id(s3d_prop, getuid());
    PDCprop_set_obj_app_name(s3d_prop, app_name);
    PDCprop_set_obj_tags(s3d_prop, "s3d");
    PDCprop_set_obj_buf(s3d_prop, data);

    // Duplicate the properties from 's3d_prop' into 'weeklysst_average_prop'
    s3d_result_prop = PDCprop_obj_dup(s3d_prop);
    /* Dup doesn't replicate the datatype, but we need to set it in any event
     * to capture the float datatype results.
     */
    PDCprop_set_obj_type(s3d_result_prop, PDC_FLOAT);

    sprintf(obj_name, "din.%d", mpi_rank);
    input_object = PDCobj_create(cont_id, obj_name, s3d_prop);
    if (input_object == 0) {
        printf("Error getting an object id of %s from server, exit...\n", obj_name);
        exit(-1);
    }

    sprintf(obj_name, "dout.%d", mpi_rank);
    output_object = PDCobj_create(cont_id, obj_name, s3d_result_prop);
    if (output_object == 0) {
        printf("Error getting an object id of %s from server, exit...\n", obj_name);
        exit(-1);
    }

    // create regions
    rin  = PDCregion_create(3, offsets3d, dims);
    rout = PDCregion_create(3, offsets3d, dims);

    input3d_iter  = PDCobj_data_iter_create(input_object, rin);
    output3d_iter = PDCobj_data_iter_create(output_object, rout);

    /* The openmsi stencil can be found in the pdc_analysis_lib.c source file
     * which generates the default analysis library: libpdcanalysis.so
     */
    PDCobj_analysis_register("openmsi_stencil", input3d_iter, output3d_iter);

    ret = PDCbuf_obj_map(data, PDC_FLOAT, rin, output_object, rout);
    ret = PDCreg_obtain_lock(input_object, rin, PDC_WRITE, PDC_NOBLOCK);
    ret = PDCreg_release_lock(input_object, rin, PDC_WRITE);
    PDCbuf_obj_unmap(output_object, rout);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Close our regions;
    if (PDCregion_close(rin) < 0)
        printf("Failed to close region(rin)!\n");
    if (PDCregion_close(rout) < 0)
        printf("Failed to close region(rout)!\n");

    // close a container
    if (PDCcont_close(cont_id) < 0)
        printf("fail to close container %ld\n", cont_id);

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc_id) < 0)
        printf("fail to close PDC\n");

    return 0;
}

int
read_client_data(int grank, int gsize, char *filename, char *datasetID, char *groupID)
{
    int i, k, errors = 0;
    int ds_ndims = 0;

    float *data_out = NULL;
    double start_t, end_t, total_t;
    size_t dimsm[3] = {0, 0, 0};

    size_t  type_size        = 0;
    hsize_t ds_dimensions[3] = {0, 0, 0};
    /* for parallel IO */
    int facc_type = FACC_MPIO;

    /* for hyperslab settings */
    hsize_t slice_elements = 0;
    hsize_t last_slice     = 0;
    hsize_t start[3]       = {0, 0, 0};
    hsize_t count[3]       = {0, 0, 0};
    hsize_t stride[3]      = {0, 0, 0};
    hsize_t block[3]       = {0, 0, 0};
    hsize_t total_elements = 0;

    void *data_array   = NULL;
    void *result_array = NULL;

    hid_t  acc_tpl   = -1;
    hid_t  fid       = -1;
    hid_t  gid       = -1;
    hid_t  dset      = -1;
    hid_t  dtype     = -1;
    hid_t  dataspace = -1;
    hid_t  memspace  = -1;
    herr_t status;

    H5T_class_t type_class = H5T_NO_CLASS;

    MPI_Info mpi_info = MPI_INFO_NULL;
    MPI_Comm mpi_comm = MPI_COMM_WORLD;

    H5open();
    acc_tpl = create_faccess_plist(mpi_comm, mpi_info, facc_type);
    /* Make the file access collective */
    if (acc_tpl >= 0)
        H5Pset_fapl_mpio(acc_tpl, mpi_comm, mpi_info);
    fid = H5Fopen(filename, H5F_ACC_RDONLY, acc_tpl);
    if (fid < 0) {
        fprintf(stderr, "Error opening filename: %s\n", filename);
        errors += 1;
        goto done;
    }
    /* Close the access property list */
    if (fid >= 0)
        H5Pclose(acc_tpl);
    acc_tpl = -1;

    if (groupID != NULL) {
        gid = H5Gopen2(fid, groupID, H5P_DEFAULT);
        if (gid < 0) {
            fprintf(stderr, "Unable to open group: %s\n", groupID);
            errors += 1;
            goto done;
        }
        dset = H5Dopen2(gid, datasetID, H5P_DEFAULT);
    }
    else {
        /* Open the specified dataset */
        dset = H5Dopen2(fid, datasetID, H5P_DEFAULT);
    }
    if (dset < 0) {
        fprintf(stderr, "Unable to opening dataset: %s\n", datasetID);
        errors += 1;
        goto done;
    }
    /* Determine the datatype used for this dataset */
    dtype = H5Dget_type(dset);
    if (dtype < 0) {
        fprintf(stderr, "Unable to read the dataset metadata to determine the datatype used\n");
        errors += 1;
        goto done;
    }
    /* Datatype (Continued) */
    type_size  = H5Tget_size(dtype);
    type_class = H5Tget_class(dtype);
    dataspace  = H5Dget_space(dset);
    if (dataspace < 0) {
        fprintf(stderr, "There was a problem reading the dataset metadata : %s\n", datasetID);
        errors += 1;
        goto done;
    }
    ds_ndims = H5Sget_simple_extent_ndims(dataspace);
    if (ds_ndims <= 0) {
        fprintf(stderr, "There was a problem reading the dataset dimensions (ndims = %d)\n", ds_ndims);
        errors += 1;
        goto done;
    }
    else {
        H5Sget_simple_extent_dims(dataspace, ds_dimensions, NULL);
#if defined(SHOW_PROGRESS)
        if (grank == 0) {
            int dim;
            printf("Dataset dtype: %lx\n", dtype);
            printf("Dataset class: %x\n", type_class);
            printf("datatype size: %ld\n", type_size);
            printf("Dataset dimensions:\n");
            for (dim = 0; dim < ds_ndims; dim++) {
                printf("\t%lld\n", ds_dimensions[dim]);
            }
        }
#endif
    }

    if (gsize > 1) {
        MPI_Barrier(MPI_COMM_WORLD);
    }

    if (gsize > 1) {
        size_t size_diff, rank_plane_size, global_plane_size = ds_dimensions[1] * ds_dimensions[2];
        int    number_of_ranks = 0;
        slice_elements         = ds_dimensions[1] / gsize;
        rank_plane_size        = slice_elements * ds_dimensions[2];
        size_diff              = global_plane_size - (rank_plane_size * gsize);
        if (size_diff > 0)
            number_of_ranks = size_diff / ds_dimensions[2];
        if (grank == 0)
            printf("size_diff = %ld, number_of_ranks = %d\n", size_diff, number_of_ranks);
        if (grank < number_of_ranks)
            slice_elements++;
        total_elements = ds_dimensions[0] * slice_elements * ds_dimensions[2];
    }
    else {
        slice_elements = ds_dimensions[1];
        total_elements = ds_dimensions[0] * slice_elements * ds_dimensions[2];
    }

    if (ds_ndims == 3) {
        start[0]  = 0;
        start[1]  = 0;
        start[2]  = 0; /* Maybe 'grank' * 2 (desired row length) */
        stride[0] = 1;
        stride[1] = 1;
        stride[2] = 1;
        count[0]  = ds_dimensions[0]; /* Full full rows x count[0] planes */
        count[1]  = slice_elements;
        count[2]  = ds_dimensions[2];
        block[0]  = 1; /* Describe a single plane */
        block[1]  = slice_elements;
        block[2]  = ds_dimensions[2];

        if (grank == 0) {
            printf("Number of elements in this slice(0) = %lld\n", total_elements);
        }
        /*
         * Define the memory dataspace.
         */
        dimsm[0] = ds_dimensions[0];
        dimsm[1] = slice_elements;
        dimsm[2] = ds_dimensions[2];

        /* For 2 ranks... */
        if (gsize > 1) {
            if (grank == 1)
                start[1] = slice_elements * grank;
        }
        memspace = H5Screate_simple(3, dimsm, NULL);

        if (H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, start, stride, count, NULL) < 0) {
            fprintf(stderr, "H5Sselect_hyperslab returned an error\n");
            goto done;
        }

        data_out = (float *)malloc(total_elements * type_size);
        if (data_out == NULL) {
            perror("malloc failure!\n");
            goto done;
        }

        start_t = MPI_Wtime();
        status  = H5Dread(dset, H5T_NATIVE_FLOAT, memspace, dataspace, H5P_DEFAULT, data_out);

        end_t   = MPI_Wtime();
        total_t = end_t - start_t;
        printf("IO time using H5Dread = %lf seconds\n", total_t);
        i = 0;

#if 0
	for(k=0; k<10; k++) {
            printf("[%4d] %hd %hd %hd %hd %hd %hd %hd %hd %hd %hd\n", i,
		     data_out[i],data_out[i+1],data_out[i+2],data_out[i+3],
                     data_out[i+4],data_out[i+5],data_out[i+6],data_out[i+7],
                     data_out[i+8],data_out[i+9]
		     );
            i +=10;
	}
#endif

        do_pdc_analysis_for_output(data_out, dimsm, PDC_FLOAT, grank);
    }

done:

    if (data_array != NULL)
        free(data_array);
    if (memspace >= 0)
        H5Sclose(memspace);
    if (dataspace >= 0)
        H5Sclose(dataspace);
    if (dset >= 0)
        H5Dclose(dset);
    if (gid >= 0)
        H5Gclose(gid);
    if (fid >= 0)
        H5Fclose(fid);

    H5close();

    return 0;
}

int
main(int argc, char **argv)
{
    int   mpi_rank = 0, mpi_size = 1;
    char *temp = NULL, *dataset = default_dataset, *filename = NULL;
    char *group = default_group;
    if ((MPI_Init(&argc, &argv) != MPI_SUCCESS) ||
        (MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank) != MPI_SUCCESS) ||
        (MPI_Comm_size(MPI_COMM_WORLD, &mpi_size) != MPI_SUCCESS)) {
        printf("MPI_Init failure, aborting!\n");
        return -1;
    }

    if (argc > 1) {
        temp = strdup(argv[1]);
        if (access(temp, (F_OK | R_OK)) == 0)
            filename = temp;
    }
    if (filename == NULL) {
        if (temp) {
            printf("ERROR: Unable to access file - %s\n", temp);
            free(temp);
        }
        else {
            if (mpi_rank == 0)
                display_usage(argv[0]);
        }
        goto finalize;
    }
    if (argc > 2) {
        dataset = strdup(argv[2]);
    }

    read_client_data(mpi_rank, mpi_size, filename, dataset, group);

finalize:
    MPI_Finalize();
    return 0;
}
