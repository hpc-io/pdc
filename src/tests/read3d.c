
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


/*
 * Create the appropriate File access property list
 * Utility function from HDF parallel testing
 */
static hid_t
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


static int
read_client_data(int grank, int gsize, char *filename, char *datasetID)
{
    int i, errors = 0;
    int ds_ndims = 0;
    int facc_type = FACC_MPIO;
    size_t type_size = 0;
    hsize_t ds_dimensions[3] = {0,0,0};

    /* for hyperslab settings */
    hsize_t size2d = 0;
    hsize_t start[3] = {0,};
    hsize_t count[3] = {0,};
    hsize_t stride[3] = {0,};
    hsize_t block[3] = {0,};
    hsize_t total_elements = 0;
    void *data_array = NULL;
    void *result_array = NULL;

    hid_t acc_tpl = -1;
    hid_t fid = -1;
    hid_t dset = -1;
    hid_t dtype = -1;
    hid_t dataspace = -1;
    hid_t memspace = -1;
    herr_t status;                             

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
    dataspace = H5Dget_space(dset);
    if (dataspace < 0) {
        fprintf(stderr,
	"There was a problem reading the dataset metadata : %s\n", datasetID);
	errors += 1;
	goto done;
    }
    ds_ndims = H5Sget_simple_extent_ndims(dataspace);
    if (ds_ndims <= 0) {
        fprintf(stderr,
	"There was a problem reading the dataset dimensions (ndims = %d)\n", ds_ndims);
	errors += 1;
	goto done;
    }
    else {
	H5Sget_simple_extent_dims(dataspace, ds_dimensions, NULL);
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

    /* For this test, we want each process to return a full column of data.
     * For a 10,8,6 for example, each column should be (10,rowsize,colsize)
     * Calulate (rows*cols) / mpi_ranks = elements in a single slice. 
     * EXAMPLE: 2 mpi_ranks => 48/2 = 24 elements/RANK = (4 x 6) or (8 x 3)
     *          4 mpi_ranks => 48/4 = 12 elements/RANK = (4 x 3) or (2 x 6)
     *          6 mpi_ranks => 48/6 = 8  elements/RANK = (4 x 2) or (8 x 1)
     *          8 mpi_ranks => 48/8 = 6  elements/RANK = (3 x 2) or (2 x 3) or (1 x 6)
     *         12 mpi_ranks => 48/12 = 4 elements/RANK = (x x 2) or (4 x 1)
     *         24 mpi_ranks => 48/24 = 2
     *         48 mpi_ranks => 48/48 = 1
     */

    if (ds_ndims == 3) {
        int j,k;
        int perranksize;
	int data_out[2][2][8];
	size_t dimsm[3] = {0,};
        size2d = ds_dimensions[2] * ds_dimensions[1];
	perranksize = (int)(size2d/gsize);
	start[0] = 0;
	start[1] = 0;
	start[2] = 0;		/* Maybe 'grank' * 2 (desired row length) */
        stride[0] = 1;
	stride[1] = 1;
	stride[2] = 1;
	count[0] = 2;		/* Full full rows x count[0] planes */
	count[1] = 2;
	count[2] = 8;
	block[0] = 1;		/* Describe a single plane */
        block[1] = 2;
	block[2] = 2;
        /*
         * Define the memory dataspace.
         */
	dimsm[0] = ds_dimensions[0];
	dimsm[1] = 2;
	dimsm[2] = 8;

	memspace = H5Screate_simple (3, dimsm, NULL);
        if (H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, start, stride, count, NULL) < 0) {
            fprintf(stderr,
            "H5Sselect_hyperslab returned an error\n");
	    goto done;
        }


	status = H5Dread (dset, H5T_NATIVE_INT, memspace, dataspace,
			  H5P_DEFAULT, &data_out[0][0][0]);
	
	if (status < 0) {
	  puts("H5Dread failed!");
	  goto done;
	}
	for(i=0; i < dimsm[0]; i++) {
	  printf("data(%d,%d,%d):\n", i, start[1],start[2]);
	  for(j=0; j < dimsm[1]; j++) {
	    for(k=0; k < dimsm[2]; k++) {
	      printf(" %3.2d", data_out[i][j][k]);
	    }
	    puts("");
	  }
	}
    }
    
#if 0
        if (H5Sselect_hyperslab(memspace, H5S_SELECT_SET, start, stride, count, block) < 0) {
            fprintf(stderr,
            "H5Sselect_hyperslab returned an error\n");
            errors += 1;
            goto done;
        }    
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

#endif	/* #if 0 */

 done:

    if (data_array != NULL) free(data_array);
    if (memspace >= 0) H5Sclose(memspace);
    if (dataspace >= 0) H5Sclose(dataspace);
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
