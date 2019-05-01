
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
#define BYROW                1       /* divide into slabs of rows */
#define BYCOL                2       /* divide into blocks of columns */
#define BYLASTDIM            3       /* For 3D and higher, we get contiguous blocks */
#define ZROW                 4       /* same as BYCOL except process 0 gets 0 rows */
#define ZCOL                 5       /* same as BYCOL except process 0 gets 0 columns */


/* File_Access_type bits */
#define FACC_DEFAULT         0x0     /* default */
#define FACC_MPIO            0x1     /* MPIO */
#define FACC_SPLIT           0x2     /* Split File */

#define DXFER_COLLECTIVE_IO  0x1  /* Collective IO*/
#define DXFER_INDEPENDENT_IO 0x2 /* Independent IO collectively */

#ifndef FALSE
#define FALSE                0x0
#endif
#ifndef TRUE
#define TRUE                 0x1
#endif

#define SHOW_PROGRESS 1
#define SMALLER_DATASIZE 1

static char *default_temperature_dataset = "WeeklySST";

static void
display_usage(char *app)
{
    printf("Usage:  %s <netcdf-filename> [dataset-identifier]\n", app);
}

static void
slab_set(int mpi_rank, int mpi_size, int ndims, hsize_t dims[], hsize_t start[], hsize_t count[],
	 hsize_t stride[], hsize_t block[], int mode)
{
    int i, lastdim = ndims-1;
    switch (mode){
    case BYROW:
	/* Each process takes a slabs of rows. */
	block[0] = dims[0]/mpi_size;
	start[0] = mpi_rank*block[0];
        for(i=1; i < ndims; i++) {
            block[i]  = dims[i];
            stride[i] = block[i];
	    count[i]  = 1;
	    start[i]  = 0;
	}
	break;
    case BYCOL:
	/* Each process takes a block of columns. */
        for(i=0; i < ndims; i++) {
            if (i == 1) {
                block[1] = dims[1]/mpi_size;
                start[1] = mpi_rank * block[1];
            } 
            else {
                block[i]  = dims[i];
                start[i]  = 0;
            }
            stride[i] = block[i];
	    count[i]  = 1;
	}
	break;
    case BYLASTDIM:
        for(i=0; i < lastdim; i++) {
            block[i]  = dims[i];
            stride[i] = block[i];
	    count[i]  = 1;
	    start[i]  = 0;
	}
	block[lastdim]  = dims[lastdim]/mpi_size;
	stride[lastdim] = block[lastdim];
	count[lastdim]  = 1;
	start[lastdim]  = mpi_rank*block[lastdim];
        break;	
    case ZROW:
	/* Similar to BYROW except process 0 gets 0 row */
	/* Each process takes a slabs of rows. */
        block[0] = (mpi_rank ? dims[0]/mpi_size  : 0);
	start[0] = (mpi_rank ? mpi_rank*block[0] : 1);
        for(i=1; i < ndims; i++) {
            block[i]  = dims[i];
            stride[i] = block[i];
	    count[i]  = 1;
	    start[i]  = 0;
	}
	break;
    case ZCOL:
	/* Similar to BYCOL except process 0 gets 0 column */
	/* Each process takes a block of columns. */
        for(i=0; i < ndims; i++) {
            if (i == 1) {
                block[1] = (mpi_rank ? dims[1]/mpi_size : 0);
                start[1] = (mpi_rank ? mpi_rank * block[1] : 1);
            } 
            else {
                block[i]  = dims[i];
                start[i]  = 0;
            }
            stride[i] = block[i];
	    count[i]  = 1;
	}
	break;
    default:
	/* Unknown mode.  Set it to cover the whole dataset. */
	printf("unknown slab_set mode (%d)\n", mode);
        for(i=0; i < ndims; i++) {
            block[i]  = dims[i];
            stride[i] = block[i];
	    count[i]  = 1;
	    start[i]  = 0;
	}
	break;
    }
}

/*
 * Create the appropriate File access property list
 * Utility function from HDF parallel testing
 */
hid_t
create_faccess_plist(MPI_Comm comm, MPI_Info info, int l_facc_type)
{
    hid_t ret_pl = -1;
    herr_t ret;

    ret_pl = H5Pcreate (H5P_FILE_ACCESS);
    if (ret_pl < 1) {
      fprintf(stderr, "H5Pcreate(1) failed\n");
      goto error;
    }

    if (l_facc_type == FACC_DEFAULT)
	return (ret_pl);

    if (l_facc_type == FACC_MPIO){
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
	return(ret_pl);
    }

    if (l_facc_type == (FACC_MPIO | FACC_SPLIT)){
	hid_t mpio_pl;

	mpio_pl = H5Pcreate (H5P_FILE_ACCESS);
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
	ret_pl = H5Pcreate (H5P_FILE_ACCESS);
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
	return(ret_pl);
    }
 error:
    /* unknown file access types */
    return (ret_pl);
}

#if 0

typedef struct _thread_args_t
{
    int      ThreadRank;
    short   *dataIn;
    float   *dataOut;
    size_t   dims_in[2];
} thread_args_t;

void *do_TheadedAverages(void *thisThread)
{
    thread_args_t *args = (thread_args_t *)thisThread;
    do_monthly_averages(args->dataIn, args->dataOut, args->dims_in);
    return NULL;
}

int calculate_short_monthly_avg(pdcid_t inputIter, pdcid_t outputIter)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    size_t dims_in[2];
    size_t dims_out[2];
    short *dataIn = NULL;
    float *dataOut = NULL;
    void *checkData = NULL;
    size_t elements_per_block = 0;
    int block_count = 0;

    while ((elements_per_block = pdc_getNextBlock(inputIter, &checkData, dims_in)) > 0) {
        dataIn = checkData;

	if ((elements_per_block = pdc_getNextBlock(outputIter, &checkData, dims_out)) <= 0)
	   PGOTO_ERROR(FAIL, "pdc_getNextBlock failed: expected a positive non-zero value");

	dataOut = checkData;
	do_monthly_averages(dataIn, dataOut, dims_in);
	block_count++;
    }

done:
    return ret_value;
}

static int
do_pdc_analysis(void *data_array, void *result_array, uint64_t *ds_dimensions, int ds_ndims, PDC_var_type_t dtype, size_t type_size, int mpi_rank, char *datasetID)
{
    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t result_cont_id;
    pdcid_t weeklysst_prop;
    pdcid_t result_array_prop;

    long long ht_total_elapsed, ht_total_setup;
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    double ht_pdc_setup, ht_total_sec;


    // create a pdc
    pdc_id = PDC_init("pdc");
    gettimeofday(&ht_total_start, 0);

    // ------------
    // create a container property
    // Note:  Strictly speaking, this isn't necessary
    //        if we're dealing with the whole object
    //        as opposed to using subsection(s).
    // -------------
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if(cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
	return -1;
    }
    // create a container (for the input data)
    cont_id = PDCcont_create("surfacetemp", cont_prop);
    if(cont_id <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
	return -1;
    }

    // create a container (for the result data)
    result_cont_id = PDCcont_create("monthlyaveragetemp", cont_prop);
    if(result_cont_id <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
	return -1;
    }

    // create an object property
    // Create the result properties and a result object #1

    weeklysst_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    PDCprop_set_obj_dims(weeklysst_prop, ds_ndims, (uint64_t *)ds_dimensions);

    PDCprop_set_obj_type(weeklysst_prop, dtype);
    PDCprop_set_obj_type_extent(weeklysst_prop, type_size);
    PDCprop_set_obj_buf(weeklysst_prop, data_array);
    PDCprop_set_obj_time_step(weeklysst_prop, mpi_rank   );
    PDCprop_set_obj_user_id( weeklysst_prop, getuid()    );
    PDCprop_set_obj_app_name(weeklysst_prop, "pdc-neon"  );
    PDCprop_set_obj_tags(weeklysst_prop, "tag0=globalsurfacetemp" );
    pdcid_t weeklysst_obj  = PDCobj_create(cont_id, datasetID, weeklysst_prop);
    int block_size = (int)(ds_dimensions[1]/2);
    pdcid_t weeklysst_block_iter = PDCcreate_block_iterator(weeklysst_obj, PDC_REGION_ALL, block_size);

    result_array_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    /* The monthly moving average calculation reads the current
     * weekly datapoint and then creates an average value from
     * the current value and the previous 3 values.  The output
     * "average" is represented as a floating point value, though
     * keeping it as a short integer should be somewhat faster
     * and would require 1/2 as much storage.
     */
    PDCprop_set_obj_dims(result_array_prop, ds_ndims, (uint64_t *)ds_dimensions);
    PDCprop_set_obj_type(result_array_prop, PDC_FLOAT);
    PDCprop_set_obj_type_extent(result_array_prop, sizeof(float));
    PDCprop_set_obj_buf(result_array_prop, result_array);
    /* We set the timestep value to allow all MPI ranks to operate on
     * the "same" named objects.
     */
    PDCprop_set_obj_time_step(result_array_prop, mpi_rank   ); 
    PDCprop_set_obj_user_id(result_array_prop, getuid()     );
    PDCprop_set_obj_app_name(result_array_prop, "pdc-neon"  );
    PDCprop_set_obj_tags(result_array_prop, "tag0=monthlyaveragetmp");
    pdcid_t result_obj = PDCobj_create(cont_id, "monthlyaveragetemp", result_array_prop);
    pdcid_t resultarray_block_iter = PDCcreate_block_iterator(result_obj, PDC_REGION_ALL, block_size);
    gettimeofday(&ht_total_end, 0);
    ht_total_setup  = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_pdc_setup = ht_total_setup / 1000000.0;

    PDCregister_obj_analysis("calculate_short_monthly_avg", weeklysst_block_iter, resultarray_block_iter);

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    if (mpi_rank == 0) {
      printf("pdc setup: elapsed time ... %.4f s\n", ht_pdc_setup);
      printf("calculate_short_monthly_avg: elapsed time ... %.4f s\n", ht_total_sec);
      fflush(stdout);
    }

    if (PDCobj_close(result_obj) < 0) 
	printf("Failed to close PDC object @ line %d\n", __LINE__);

    if (PDCobj_close(weeklysst_obj) < 0) 
	printf("Failed to close PDC object @ line %d\n", __LINE__);

    if(PDCcont_close(cont_id) < 0)
        printf("fail to close container %lld\n", cont_id);

    if(PDCprop_close(cont_prop) < 0)
	printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc_id) < 0)
       printf("fail to close PDC\n");

    return 0;
}
#endif


static int
read_client_data(int grank, int gsize, char *filename, char *datasetID)
{
    int i, errors = 0;
    int ds_ndims = 0;
    int facc_type = FACC_MPIO;
    size_t type_size = 0;
    hsize_t *ds_dimensions = NULL;

    /* for hyperslab settings */
    hsize_t *start = NULL;
    hsize_t *count = NULL;
    hsize_t *stride = NULL;
    hsize_t *block = NULL;
    hsize_t total_elements = 0;
    void *data_array = NULL;
    void *result_array = NULL;

    hid_t acc_tpl = -1;
    hid_t fid = -1;
    hid_t dset = -1;
    hid_t dtype = -1;
    hid_t file_dataspace = -1;
    hid_t mem_dataspace = -1;
    // hid_t fapl_id = -1;
    H5T_class_t type_class = H5T_NO_CLASS;

    MPI_Info mpi_info = MPI_INFO_NULL;
    MPI_Comm mpi_comm = MPI_COMM_WORLD;

    H5open();

    acc_tpl = create_faccess_plist(mpi_comm, mpi_info, facc_type);
    /* Make the file access collective */
    if (acc_tpl >= 0) H5Pset_fapl_mpio(acc_tpl, mpi_comm, mpi_info );
    fid = H5Fopen(filename, H5F_ACC_RDONLY, acc_tpl);
    if (fid < 0) {
        fprintf(stderr, "Error opening filename: %s\n", filename);
	errors += 1;
	goto done;
    }
    /* Close the access property list */
    if (fid >= 0) H5Pclose(acc_tpl);
    acc_tpl = -1;
    /* Open the specified dataset */
    dset = H5Dopen2(fid, datasetID, H5P_DEFAULT);
    if (dset < 0) {
        fprintf(stderr, "Unable to opening dataset: %s\n", datasetID);
	errors += 1;
	goto done;
    }
    /* Determine the datatype used for this dataset */
    dtype = H5Dget_type(dset);
    if (dtype < 0) {
        fprintf(stderr, 
	"Unable to read the dataset metadata to determine the datatype used\n");
	errors += 1;
	goto done;
    }
    /* Datatype (Continued) */
    type_size  = H5Tget_size(dtype);
    type_class = H5Tget_class(dtype);
    file_dataspace = H5Dget_space(dset);
    if (file_dataspace < 0) {
        fprintf(stderr,
	"There was a problem reading the dataset metadata : %s\n", datasetID);
	errors += 1;
	goto done;
    }
    ds_ndims = H5Sget_simple_extent_ndims(file_dataspace);
    if (ds_ndims <= 0) {
        fprintf(stderr,
	"There was a problem reading the dataset dimensions (ndims = %d)\n", ds_ndims);
	errors += 1;
	goto done;
    }
    else {
        ds_dimensions = (hsize_t *)calloc(ds_ndims, sizeof(hsize_t));
	start = (hsize_t *)calloc(ds_ndims * 4, sizeof(hsize_t));
	if (start) {
            count  = &start[ds_ndims];
            block  = &start[ds_ndims*2];
	    stride = &start[ds_ndims*3];
	}
	H5Sget_simple_extent_dims(file_dataspace, ds_dimensions, NULL);
#if defined(SHOW_PROGRESS)
        if (grank == 0) {
            int  dim;
            printf("Dataset dtype: %lx\n", dtype);
            printf("Dataset class: %x\n", type_class);
            printf("datatype size: %ld\n", type_size);
            printf("Dataset dimensions:\n");
            for (dim=0; dim < ds_ndims; dim++) {
                printf("\t%lld\n", ds_dimensions[dim]);
	    }
        }
#endif
    }
    if (gsize > 1) {
        MPI_Barrier(MPI_COMM_WORLD);
    }

    slab_set(grank, gsize, ds_ndims, ds_dimensions, start, count, stride, block, ZROW);
    if (grank == 0) {
    puts("\nstart");
    for(i=0; i< ds_ndims; i++) {
      printf(" %3.2lld",start[i]);
    }
    puts("\ncount");
    for(i=0; i< ds_ndims; i++) {
      printf(" %3.2lld",count[i]);
    }
    puts("\nstride");
    for(i=0; i< ds_ndims; i++) {
      printf(" %3.2lld",stride[i]);
    }
    puts("\nblock");
    for(i=0; i< ds_ndims; i++) {
      printf(" %3.2lld",block[i]);
    }
    puts("");
    }
#if 0
    if (H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride, count, block) < 0) {
        fprintf(stderr,
	"H5Sselect_hyperslab returned an error\n");
	errors += 1;
	goto done;
    }

    mem_dataspace = H5Screate_simple (ds_ndims, block, NULL);
    if (mem_dataspace < 0) {
        fprintf(stderr,
       	"There was a problem creating an in-memory dataspace\n");
	errors += 1;
	goto done;
    }

#if defined(SHOW_PROGRESS)
    total_elements = block[0];
    printf("[%d] block[0] = %lld, block[1] = %lld, block[2] = %lld\n", grank, block[0], block[1], block[2] );
    fflush(stdout);
    for (i=1; i< ds_ndims; i++) {
        total_elements *= block[i];
    }
    printf("[%d] total_elements this rank = %lld\n", grank, total_elements);
    MPI_Barrier(MPI_COMM_WORLD);
#else
    total_elements = block[0];
    for (i=1; i< ds_ndims; i++) {
        total_elements *= block[i];
    }
#endif 
    data_array = calloc(total_elements, type_size);
    if (data_array == NULL) {
        fprintf(stderr,
       	"There was a problem allocating memory to read the dataset\n");
	errors += 1;
	goto done;
    }
    result_array = calloc(total_elements, sizeof(float));
    if (result_array == NULL) {
        fprintf(stderr,
       	"There was a problem allocating memory to save the analysis results\n");
	errors += 1;
	goto done;
    }

    /* Integer class and type size == 2 */
    if ( H5Dread(dset, H5T_NATIVE_INT16, mem_dataspace, file_dataspace,
		 H5P_DEFAULT, data_array) < 0) {
        fprintf(stderr,
       	"There was a problem reading the dataset\n");
	errors += 1;
	goto done;
    }

    do_pdc_analysis(data_array, result_array, (uint64_t *)block, ds_ndims, PDC_INT, type_size, grank, datasetID);    
    // do_pdc_analysis(data_array, result_array, (uint64_t *)ds_dimensions, ds_ndims, PDC_INT, type_size, grank, datasetID);

#endif

 done:

    if (data_array != NULL) free(data_array);
    if (start != NULL) free(start);
    if (ds_dimensions != NULL) free(ds_dimensions);
    if (mem_dataspace >= 0) H5Sclose(mem_dataspace);
    if (file_dataspace >= 0) H5Sclose(file_dataspace);
    if (dset >= 0) H5Dclose(dset);
    if (fid >= 0) H5Fclose(fid);

    H5close();
    return errors;
}

int
main(int argc, char **argv)
{
    int mpi_rank = 0, mpi_size = 1;
    char *temp = NULL, *dataset = default_temperature_dataset, *filename = NULL;
    if ((MPI_Init(&argc, &argv) != MPI_SUCCESS) ||
	(MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank) != MPI_SUCCESS) ||
	(MPI_Comm_size(MPI_COMM_WORLD, &mpi_size) != MPI_SUCCESS)) {
        printf("MPI_Init failure, aborting!\n");
        return -1;
    }

    if (argc > 1) {
        temp = strdup(argv[1]);
	if (access(temp, (F_OK|R_OK)) == 0)
            filename = temp;
    }
    if (filename == NULL) {
        if (temp) {
	    printf("ERROR: Unable to access file - %s\n", temp);
	    free(temp);
	}
	else {
	    if (mpi_rank == 0) display_usage(argv[0]);
	}
	goto finalize;
    }
    if (argc > 2) {
        dataset = strdup(argv[2]);
    }

    read_client_data(mpi_rank, mpi_size, filename, dataset);


finalize:
    MPI_Finalize();
    return 0;
       
}
