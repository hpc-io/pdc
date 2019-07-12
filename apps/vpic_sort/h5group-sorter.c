/**
 * *** Copyright Notice ***
 * SDS - Scientific Data Services framework, Copyright (c) 2015, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy).  All rights reserved.
 * If you have questions about your rights to use or distribute this software, 
 * please contact Berkeley Lab's Technology Transfer Department at TTD@lbl.gov.
 * 
 * NOTICE.  This software was developed under funding from the 
 * U.S. Department of Energy.  As such, the U.S. Government has been granted 
 * for itself and others acting on its behalf a paid-up, nonexclusive, 
 * irrevocable, worldwide license in the Software to reproduce, prepare 
 * derivative works, and perform publicly and display publicly.  
 * Beginning five (5) years after the date permission to assert copyright is 
 * obtained from the U.S. Department of Energy, and subject to any subsequent 
 * five (5) year renewals, the U.S. Government is granted for itself and others
 * acting on its behalf a paid-up, nonexclusive, irrevocable, worldwide license
 * in the Software to reproduce, prepare derivative works, distribute copies to
 * the public, perform publicly and display publicly, and to permit others to
 * do so.
 *
*/

/**
 *
 * Email questions to {dbin, sbyna, kwu}@lbl.gov
 * Scientific Data Management Research Group
 * Lawrence Berkeley National Laboratory
 *
*/
/* 
 *
 * Generally, this file sort the dataset inside a HDF5 group
 * 
 */

#include "stdlib.h"
#include "hdf5.h"
#include <unistd.h>
#include <string.h>
#include <float.h>
#include <omp.h>
#include <stdio.h>
#include <math.h>


const double lowest_double = -DBL_MAX;
const double higest_double =  DBL_MAX;


#define  H5GS_INT32    0  // int
#define  H5GS_INT64    1  // long
#define  H5GS_FLOAT32  2  // float 
#define  H5GS_FLOAT64  3  // double


#define  TRUE  1
#define  NAME_MAX      255
char     *filename;
char     *group_name;
char     *filename_sorted;
char     *filename_attribute;

MPI_Status   Stat;

//This is the struct type for orginal data
MPI_Datatype OPIC_DATA_TYPE;
//This is the struct type for sorted metadata table
MPI_Datatype SDS_DATA_TYPE;

//Support up to 48 datasets
#define MAX_DATASET_NUM  48
//Actually number of datasets to sort
int     dataset_num = 0;
//The index of the key in the sort
//Other datasets will be sorted based on the key
int      key_index;
//The value type of the key (e.g, float, int..)
//Used in comparison function
int      key_value_type;

//Only sort the key (no extra values)
int     sort_key_only = 0;
//Data is in skew
int     skew_data = 0;
//Print more infor
int     verbose  = 0;

//Write results to disk
int     write_result = 1;

//collect_data in phase2
int     collect_data = 1;

//This is only for test
int     weak_scale_test       = 0;
int     weak_scale_test_length = 1000000;

//Use the local_sort with openmp 
int     local_sort_threaded = 0;
int     local_sort_threads_num = 16;

//The struct to store the information of datasets
typedef struct{
  char  dataset_name[NAME_MAX];
  hid_t did;
  hid_t type_id;
  int   type_size;
  //int   package_offset;
}dset_name_item;
dset_name_item dname_array[MAX_DATASET_NUM];

//For different data types, we use the one with the maximum size 
//to store the data
int            max_type_size = 0;

//
//This is the buffer to store the data
//Use the package_data to store different types (e/g. int, float)
//
char          *package_data;

//
//The metadata table of the sorted data
//We use float to represent (min, max) for different types
//
typedef struct{
  float   c_min;
  float   c_max;
  unsigned long long c_start_offset;
  unsigned long long c_end_offset;
}SDSSortTableElem;

//Package and unpackage data in different types into global "package_data"
void package(char *p_data, int file_index, size_t my_data_size, char *my_data, int row_size);
void unpackage(char *p_data, int file_index, size_t my_data_size, char *my_data, int row_size);

//Covert the data in "char *" into "long long", "int", and "float"
//"row_data" contains one row of data
//"index" specify the order of data to convert
long long getLongLongValue(int index, char *row_data);
int       getIntValue(int index, char *row_data);
float     getFloatValue(int index, char *row_data);

//Read the data from "dset_id" based on "my_offset" and "my_data_size"
void get_one_file_data(hid_t dset_id, int mpi_rank, int mpi_size, int file_index, void *data,   hsize_t my_offset, hsize_t my_data_size);

//Find the type of dataset
int getDataType (hid_t dtid);
int getIndexDataType(hid_t did);

//Compare the key in "long long" type
int CompareLongLongKey(const void *a, const void *b);

//Write sorted datat to the file
int write_reault_file(int mpi_rank, int mpi_size, char *data, hsize_t my_data_size, int row_size);

//Phase 1 of the parallel sampling sorting
//Sort the data first and select the samples
int phase1(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, char *sample_of_rank0, int row_size);

//Phase 2 of the parallel sampling sorting
int phase2(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, char *pivots, int rest_size, int row_size);

// Master does slave's job, and also gather and sort pivots
int master(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, int rest_size, int row_size);

/*Do sort and sample*/
int slave(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, int rest_size, int row_size);

//Sort the data based on the type
int qsort_type(void *data, int64_t my_data_size, size_t row_size);

//Get the index-th data in the row (row_data) as double
double get_value_double(int index, char *row_data);

//int pivots_replicated(char *pivots, int dest, int *dest_pivot_replicated_size, int *dest_pivot_replicated_rank, int mpi_size, int mpi_rank, int row_size);

int pivots_replicated(char *pivots, int dest, int *dest_pivot_replicated_size, int *dest_pivot_replicated_rank, int mpi_size, int mpi_rank, int row_size, double *p_value_head);

void rank_pivot(char *pivots, char *data,  int64_t my_data_size, int dest_pivot, int *rank_less, int *rank_equal, int row_size, int mpi_size, double p_value_head);

int openmp_sort(char *data, int data_size, int threads, size_t row_size);

void  print_help(){
  char *msg="Usage: %s [OPTION] \n\
      	  -h help (--help)\n\
          -f name of the file to sort \n\
          -g group path within HDF5 file to data set \n\
          -o name of the file to store sorted results \n  \
          -a name of the attribute file to store sort table  \n \
          -k the index key of the file \n\
          -s the data is in skew shape \n  \
          -e only sort the key  \n \
          -v verbose  \n \
          example: ./h5group-sorter -f testf.h5p  -g /testg  -o testg-sorted.h5p -a testf.attribute -k 0 \n";
  fprintf(stdout, msg, "h5group-sorter");
}

//Main of the parallel sampling sort
int main(int argc, char **argv){
  int        mpi_size, mpi_rank;
  hid_t      plist_id, plist2_id;	
  hid_t      file_id, dset_id, x_id, y_id, z_id;
  hid_t      dataspace, memspace, x_space, y_space, z_space;
  int        rank;
  hsize_t    dims_out[1];
  herr_t     status;
  hsize_t    my_data_size, rest_size;
  hsize_t    my_offset;
	
  float     *data, *x_data, *y_data, *z_data;
  double     t1, t2, t20, t0, t4;
  char       obj_name[NAME_MAX + 1];
  int        i, j;

  MPI_Comm   comm = MPI_COMM_WORLD;
  MPI_Info   info = MPI_INFO_NULL;
 
  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &mpi_size);
  MPI_Comm_rank(comm, &mpi_rank);

  key_index = 1;  
  int c;
  static const char *options="f:o:a:g:k:hsveml:t:c";
  extern char *optarg;

  //opterr = 0;
  while ((c = getopt (argc, argv, options)) != -1){
    switch (c){
      case 'f':
	//strncpy(filename, optarg, NAME_MAX);
        filename = strdup(optarg);
	break;
      case 'o':
        //strncpy(filename_sorted, optarg, NAME_MAX);
        filename_sorted = strdup(optarg);
	break;
      case 'a':
        //strncpy(filename_attribute, optarg, NAME_MAX);
        filename_attribute = strdup(optarg);  
	break;
      case 'g':
        //strncpy(group_name, optarg, NAME_MAX);
        group_name = strdup(optarg);
        break;
      case 'k':
        key_index = atoi(optarg);
        break;
      case 's':
        skew_data = 1;
        break;
      case 'm':
        write_result = 0;
        break;
      case 'v':
        verbose = 1;
        break;
      case 'l':
        weak_scale_test = 1; 
        weak_scale_test_length = atoi(optarg);
        break;
      case 'e':
        sort_key_only = 1;
        break;
      case 't':
        local_sort_threaded = 1;
        local_sort_threads_num = atoi(optarg);
        break;
      case 'c':
        collect_data = 0;
        break;
      case 'h':
        print_help();
        return 1;
      default:
	printf("Error option [%s]\n", optarg);
	exit(-1);
    }
  }
 

  /* create a type for SDSSortTableElem*/
  const int    nitems2=4;
  int          blocklengths2[4] = {1,1,1,1};
  MPI_Datatype types2[4] = {MPI_FLOAT, MPI_FLOAT, MPI_UNSIGNED_LONG_LONG,MPI_UNSIGNED_LONG_LONG};
  MPI_Aint     offsets2[4];
    
  offsets2[0] = offsetof(SDSSortTableElem, c_min);
  offsets2[1] = offsetof(SDSSortTableElem, c_max);
  offsets2[2] = offsetof(SDSSortTableElem, c_start_offset);
  offsets2[3] = offsetof(SDSSortTableElem, c_end_offset);
  
    
  MPI_Type_create_struct(nitems2, blocklengths2, offsets2, types2, &SDS_DATA_TYPE);
  MPI_Type_commit(&SDS_DATA_TYPE);

  //Open HDF5 file using parallel HDF5
  hid_t    gid, did, typeid;
  hsize_t  num_obj;
  int      obj_class;
  plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, comm, info);
  MPI_Barrier(MPI_COMM_WORLD);
  t0 =  MPI_Wtime();

  file_id = H5Fopen(filename, H5F_ACC_RDONLY,plist_id);
  t1 = MPI_Wtime();    

  //Find all dataset in "group_name"
  gid = H5Gopen(file_id, group_name, H5P_DEFAULT);
  H5Gget_num_objs(gid, &num_obj);
  for (i = 0; i < num_obj; i++)
  {
    obj_class = H5Gget_objtype_by_idx(gid, i);
    H5Gget_objname_by_idx(gid, i, obj_name, NAME_MAX);
    if (sort_key_only == 1 && i != key_index)
      continue;
    /* Deal with object based on its obj_class. */
    switch(obj_class)
    {
      case H5G_DATASET:
        /* Open the dataset. */
        strncpy(dname_array[dataset_num].dataset_name, obj_name, NAME_MAX);
        /* Open the dataset. */
        did = H5Dopen(gid, obj_name, H5P_DEFAULT);
        dname_array[dataset_num].did        = did;
        typeid                              = H5Dget_type(did);
        dname_array[dataset_num].type_size  = H5Tget_size(typeid);
        //Identify the type with the maximum size
        if (max_type_size < dname_array[dataset_num].type_size){
          max_type_size = dname_array[dataset_num].type_size;
        }
        dname_array[dataset_num].type_id    = getDataType(typeid);
        //We use max_type_size for all type
        //if(dataset_num == 0){
        //  dname_array[dataset_num].package_offset = 0;
        //}else{
        //  dname_array[dataset_num].package_offset = dname_array[dataset_num-1].package_offset + dname_array[dataset_num].type_size;
        //}
        dataset_num++;
        break;
      default:
        printf("Unknown object class %d!", obj_class);
    }
  }
  
  if (sort_key_only == 1){
    if(dataset_num == 1){
      key_index = 0; //The first one is the key
    }else{
      printf("Sort the key only but we found two keys !\n");
      exit(-1);
    }
  }
  //Get the type of key
  if(mpi_rank == 0)
    printf("Key is %s ", dname_array[key_index].dataset_name);
  
  key_value_type = getIndexDataType(dname_array[key_index].did);
  if(mpi_rank == 0)
    switch(key_value_type){
      case H5GS_INT32:
        printf(", in INT32 type !\n");
        break;
      case H5GS_INT64:
        printf(", in INT64 type !\n");
        break;
      case H5GS_FLOAT32:
        printf(", in FLOAT32 type !\n");
        break;
      case H5GS_FLOAT64:
        printf(", in FLOAT64 type !\n");
        break;
    }
  
  //Partition the data evenly among cores based 
  //on the size of key dataset
  //Assume other datasets have the same size and shape
  dataspace      = H5Dget_space(dname_array[key_index].did);
  rank      = H5Sget_simple_extent_ndims(dataspace);
  H5Sget_simple_extent_dims(dataspace, dims_out, NULL);
  if(weak_scale_test == 0){
    rest_size = dims_out[0] % mpi_size;
    if (mpi_rank ==  (mpi_size - 1)){
      my_data_size = dims_out[0]/mpi_size + rest_size;
    }else{
      my_data_size = dims_out[0]/mpi_size;
    }
    my_offset = mpi_rank * (dims_out[0]/mpi_size);
  }else{
    if(my_data_size * mpi_size > dims_out[0]){
      printf("File is too small for scale test (test legnth %lld, mpi size %d, data size %lld) !\n", my_data_size, mpi_size, dims_out[0]);
      exit(-1);
    }
    rest_size    = 0;
    my_data_size = weak_scale_test_length;
    my_offset    = mpi_rank * my_data_size;
  }

  //Compute the size of "package_data" 
  //Use max_type_size to ensure the memory alignment! 
  int    row_size;
  size_t row_count;
  row_size     =   max_type_size * dataset_num;  //The size of each row
  row_count    =   my_data_size;                 //The total size of rows 
  if(row_count != my_data_size || my_data_size > ULONG_MAX){
    printf("Size of too big to size_t type !\n");
    exit(-1);
  }
  

  //Each row is sent/received with a MPI_BYTE array
  //So, we create a MPI type
  MPI_Type_contiguous(row_size, MPI_BYTE, &OPIC_DATA_TYPE);
  MPI_Type_commit(&OPIC_DATA_TYPE);
  
  
  if(mpi_rank == 0 || mpi_rank == (mpi_size -1)){
    printf(" Data patition (based on key) My rank: %d, file size: %lu, my_data_size: %lu, my_offset: %lu , row_size %ld, char size %d , dataset_num = %d row_count = %d, max type size %d, sizeof(MPI_CHAR)=%d \n ", mpi_rank, (unsigned long)dims_out[0],(unsigned long)my_data_size, (unsigned long)my_offset, row_size, sizeof(char), dataset_num, row_count, max_type_size, sizeof(MPI_CHAR));
  }

  //Allocate memory to store the data
  //It is a two-dimenstion array in shape of row_count * row_size
  package_data = (char *)malloc(row_count * row_size * sizeof(char));
  if(package_data == NULL){
    printf("Memory allocation fails for package_data! \n");
    exit(-1);
  }
  
  //Allocate buf for one dataset
  char *temp_data;
  temp_data = (char *)malloc(max_type_size * my_data_size);
  if(temp_data == NULL){
    printf("Memory allocation fails ! \n");
    exit(-1);
  }

  //Read each dataset seperately and then combine each row into a package 
  if(mpi_rank == 0){
    printf("Read and package the data ! \n");
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  t20 = MPI_Wtime();
  for (i = 0 ; i < dataset_num; i++){
    //Read one dataset
    get_one_file_data(dname_array[i].did, mpi_rank, mpi_size, i, temp_data, my_offset, my_data_size);
    //Merge the data into "package_data"
    package(package_data, i, row_count, temp_data, row_size);
    H5Dclose(dname_array[i].did);
    if(mpi_rank == 0)
      printf("%d,  %s , type id (%d), type size (%d)\n ", i, dname_array[i].dataset_name, dname_array[i].type_id, dname_array[i].type_size);
  }
  free(temp_data);
  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  if(mpi_rank == 0){
    printf("Reading data takes [%f]s, for each dataset [%f]s \n", (t2-t20), (t2-t20)/dataset_num);
  }	    


  //master:  also do slave's job. In addition, it is responsible for samples and pivots
  //slave:   (1) sorts. (2) samples (3) sends sample to master
  //         (4) receives pivots    (5) sends/receives data to/from other processes based on pivots
  //         (6) sorts data again   (7) writes data to its location
    
  if (mpi_rank==0){
    printf("Start master of parallel sorting ! \n");
    master(mpi_rank, mpi_size,  package_data,  my_data_size, rest_size, row_size);
  }else{
    slave(mpi_rank,  mpi_size,  package_data,  my_data_size, rest_size, row_size);
  }


  H5Pclose(plist_id);
  H5Sclose(dataspace);
  H5Gclose(gid);
  H5Fclose(file_id);
  MPI_Barrier(MPI_COMM_WORLD);		
  t4 = MPI_Wtime();
  if(mpi_rank == 0)
    printf("Overall time is [%f]s \n", (t4-t0));

  //MPI_Info_free(&info);
  MPI_Finalize();
}



void package(char *p_data, int file_index, size_t my_data_size, char *my_data, int row_size){
  int     i, j, temp;
  int     p_offset, t_size;
  char   *t_address, *s_address;
  
  //p_offset = dname_array[file_index].package_offset;
  t_size   = dname_array[file_index].type_size;
  //t_size   = max_type_size; 
  p_offset = max_type_size * file_index;
  //printf("offset %d,  size %d \n", p_offset, t_size);

  for (i = 0 ;  i < my_data_size; i++){
    t_address = (char *) (p_data + i * row_size + p_offset);
    s_address = (char *) (my_data +  i * t_size);
    memcpy(t_address, s_address,  t_size);
  }
 
}

void unpackage(char *p_data, int file_index, size_t my_data_size, char *my_data, int row_size){
  int     i, j, temp;
  int     p_offset, t_size;
  char   *t_address, *s_address;
  
  t_size   = dname_array[file_index].type_size;
  p_offset = max_type_size * file_index;

  for (i = 0 ;  i < my_data_size; i++){
    //s_address = (char *)(p_data  +  i * row_size + p_offset);
    //t_address = my_data +  i * t_size;
    memcpy(my_data + i * t_size , p_data  +  i * row_size + p_offset,  t_size);
  }
  
}

int getInt32Value(int index, char *row_data){
  int  p_offset, t_size;
  int  value, *p;

  //p_offset  = dname_array[index].package_offset;
  p_offset = max_type_size * index;

  p = (int *)(row_data + p_offset);
  value = *p;
  
  return value;
}


long long getInt64Value(int index, char *row_data){
  int  p_offset, t_size;
  long long value, *p;

  //p_offset  = dname_array[index].package_offset;
  p_offset = max_type_size * index;

  p = (long long *)(row_data + p_offset);
  value = *p;
  
  return value;
} 


float getFloat32Value(int index, char *row_data){
  int   p_offset, t_size;
  float value, *p;
  
  //p_offset  = dname_array[index].package_offset;
  p_offset = max_type_size * index;
  p = (float *)(row_data + p_offset);
  value = *p;
  
  return value;
}


double getFloat64Value(int index, char *row_data){
  int    p_offset, t_size;
  double value, *p;
  
  //p_offset  = dname_array[index].package_offset;
  p_offset = max_type_size * index;
  p = (double *)(row_data + p_offset);
  value = *p;
  
  return value;
}

//Read the data from "dset_id" based on "my_offset" and "my_data_size"
void get_one_file_data(hid_t dset_id, int mpi_rank, int mpi_size, int file_index, void *data,   hsize_t my_offset, hsize_t my_data_size){
  hid_t   dataspace, memspace, typeid;
  hsize_t dims_out[1];
  int     rank;
  hid_t   plist_id, plist2_id;	
 

  //Create the memory space & hyperslab for each process
  dataspace = H5Dget_space(dset_id);
  rank      = H5Sget_simple_extent_ndims(dataspace);
  memspace =  H5Screate_simple(rank, &my_data_size, NULL);
  H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, &my_offset, NULL, &my_data_size, NULL);	

  
  plist2_id = H5Pcreate(H5P_DATASET_XFER);
  H5Pset_dxpl_mpio(plist2_id, H5FD_MPIO_COLLECTIVE);
  //plist2_id = H5P_DEFAULT;
  typeid = H5Dget_type(dset_id);

  switch (H5Tget_class(typeid)){
    case H5T_INTEGER:
      if(H5Tequal(typeid, H5T_STD_I32LE) == TRUE){
        H5Dread(dset_id, H5T_NATIVE_INT,      memspace, dataspace, plist2_id, data);
      }else if(H5Tequal(typeid, H5T_STD_I64LE) == TRUE){
        H5Dread(dset_id,  H5T_NATIVE_LLONG,     memspace, dataspace, plist2_id, data);
      }else if(H5Tequal(typeid, H5T_STD_I8LE) == TRUE){
        H5Dread(dset_id,  H5T_NATIVE_CHAR,      memspace, dataspace, plist2_id, data);
      }else if(H5Tequal(typeid, H5T_STD_I16LE) == TRUE){
        H5Dread(dset_id, H5T_NATIVE_SHORT,      memspace, dataspace, plist2_id, data);
      }
      break;
    case H5T_FLOAT:
      if(H5Tequal(typeid, H5T_IEEE_F32LE) == TRUE){
        H5Dread(dset_id, H5T_NATIVE_FLOAT,  memspace, dataspace, plist2_id, data);
      }else if(H5Tequal(typeid, H5T_IEEE_F64LE) == TRUE){
        H5Dread(dset_id, H5T_NATIVE_DOUBLE,  memspace, dataspace, plist2_id, data);
      }

      //H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, dataspace, plist2_id, data);
      break;
    default:
      break;
  }

  H5Pclose(plist2_id);
  H5Sclose(dataspace);
}

int getIndexDataType(hid_t did)
{
  hid_t dtid  = H5Dget_type(did);
  int sz = H5Tget_size(dtid);
  switch (H5Tget_class(dtid)){
    case H5T_INTEGER:
      if(H5Tequal(dtid, H5T_STD_I32LE) == TRUE){
        //printf("Key is in Int type ! \n");
        return H5GS_INT32;
      }else if(H5Tequal(dtid, H5T_STD_I64LE) == TRUE){
        //printf("Key is in Long Long type ! \n");
        return H5GS_INT64;
      }
      //Only support int and long  long now
      //else if(H5Tequal(dtid, H5T_STD_I8LE) == TRUE){
      //  return H5T_STD_I8LE;
      //}else if(H5Tequal(dtid, H5T_STD_I16LE) == TRUE){
      //  return H5T_STD_I16LE;
      //}
      return H5GS_INT32;
      break;
    case H5T_FLOAT:
      if(H5Tequal(dtid, H5T_IEEE_F32LE) == TRUE){
        //printf("Key is in Float type ! \n");
        return H5GS_FLOAT32;
      }else if(H5Tequal(dtid, H5T_IEEE_F64LE) == TRUE){
        //printf("Key is in Double type ! \n");
        return H5GS_FLOAT64;
      }
      return H5GS_FLOAT32;
      break;
    default:
      printf("Not support this type of key now !");
      exit(-1);
      break;
  }
}



int getDataType(hid_t dtid)
{
  int sz = H5Tget_size(dtid);
  switch (H5Tget_class(dtid)){
    case H5T_INTEGER:
      if(H5Tequal(dtid, H5T_STD_I32LE) == TRUE){
        return H5T_STD_I32LE;
      }else if(H5Tequal(dtid, H5T_STD_I64LE) == TRUE){
        return H5T_STD_I64LE;
      }
      //Only support int and long  long now
      //else if(H5Tequal(dtid, H5T_STD_I8LE) == TRUE){
      //  return H5T_STD_I8LE;
      //}else if(H5Tequal(dtid, H5T_STD_I16LE) == TRUE){
      //  return H5T_STD_I16LE;
      //}
      return H5T_STD_I32LE;
      break;
    case H5T_FLOAT:
      if(H5Tequal(dtid, H5T_IEEE_F32LE) == TRUE){
        return H5T_IEEE_F32LE;
      }else if(H5Tequal(dtid, H5T_IEEE_F64LE) == TRUE){
        return H5T_IEEE_F64LE;
      }
      return H5T_IEEE_F32LE;
      break;
    case H5T_COMPOUND:
      return H5T_COMPOUND;
      break;
    case H5T_STRING:
      return H5T_STRING;
      break;
    default:
      break;
  }
}


//Compare the key in "long long" type
int CompareInt64Key(const void *a, const void *b){
  long long v1, v2;
  v1 = getInt64Value(key_index, (char *)a);
  v2 = getInt64Value(key_index, (char *)b);
      
  if(v1 == v2){
    return 0;
  }
    
  if(v1 < v2){
    return -1;
  }else{
    return 1;
  }
}

//Compare the key in "long long" type
int CompareInt32Key(const void *a, const void *b){
  int v1, v2;
  v1 = getInt32Value(key_index, (char *)a);
  v2 = getInt32Value(key_index, (char *)b);
      
  if(v1 == v2){
    return 0;
  }
    
  if(v1 < v2){
    return -1;
  }else{
    return 1;
  }
}


//Compare the key in "long long" type
int CompareFloat32Key(const void *a, const void *b){
  float v1, v2;
  v1 = getFloat32Value(key_index, (char *)a);
  v2 = getFloat32Value(key_index, (char *)b);
      
  if(v1 == v2){
    return 0;
  }
    
  if(v1 < v2){
    return -1;
  }else{
    return 1;
  }
}


//Compare the key in "long long" type
int CompareFloat64Key(const void *a, const void *b){
  double v1, v2;
  v1 = getFloat64Value(key_index, (char *)a);
  v2 = getFloat64Value(key_index, (char *)b);
      
  if(v1 == v2){
    return 0;
  }
    
  if(v1 < v2){
    return -1;
  }else{
    return 1;
  }
}

//Write the results to the file
int write_reault_file(int mpi_rank, int mpi_size, char *data, hsize_t my_data_size, int row_size){
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;
  hid_t    plist_id, plist_id2, file_id, group_id, dataspace_id, dset_id, filespace, memspace;
  hid_t    x_id, y_id, z_id, x_space,y_space,z_space;
  hsize_t  i, j, *size_vector;
  hsize_t  file_size, my_offset[1], count[1];
  hsize_t  dims[1];
  
  hsize_t                   my_start_address, my_end_address;
  SDSSortTableElem         *global_metadata_array;
  SDSSortTableElem          my_metadata_array;
  unsigned long long       *global_metadata_array2;
  long long                *global_metadata_array1;
  double                    my_min_value, my_max_value;
  //long long                *e_data, *x_data, *y_data, *z_data;


  double t1, t2, t3, t4, t5, t6, t7, t8;
  MPI_Barrier(MPI_COMM_WORLD);
  t1 =  MPI_Wtime();

  // printf("my_rank %d, write file\n ", mpi_rank);
 
  //Gather the size of data belonging to each proccess
  size_vector = malloc(mpi_size * sizeof(hsize_t));
  
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Allgather(&my_data_size, 1, MPI_UNSIGNED_LONG_LONG, size_vector, 1, MPI_UNSIGNED_LONG_LONG, MPI_COMM_WORLD);

  file_size = 0;
  for (i = 0; i < mpi_size; i++) {
    file_size = file_size + size_vector[i];
    //if (mpi_rank == 0)
    //  printf("%d,  size_vector %lu, file size %lu \n ", i, (unsigned long)size_vector[i], (unsigned long)file_size);
  }
  
  my_start_address = 0;
  for (i = 0; i < mpi_rank; i++) {
    my_start_address = my_start_address + size_vector[i];
  }
  
  my_offset[0] = my_start_address;
  my_end_address  = my_start_address + my_data_size;

  if(mpi_rank == 0 || mpi_rank == (mpi_size -1)){
    printf("My rank: %d, my data size %llu, Total size: %llu, SA %llu, EA %llu \n", mpi_rank, my_data_size, (unsigned long long) file_size, my_start_address, my_end_address);
  }
 
  plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id, comm, info);
  //H5Pset_libver_bounds (plist_id, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
    
  file_id = H5Fcreate(filename_sorted,  H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  H5Pclose(plist_id);
    
  group_id = H5Gcreate(file_id, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

  count[0] = my_data_size;
  memspace = H5Screate_simple(1, count, NULL);

  plist_id2 = H5Pcreate(H5P_DATASET_XFER);
  H5Pset_dxpl_mpio(plist_id2, H5FD_MPIO_COLLECTIVE);

  char *temp_data;
  temp_data = (char *)malloc(max_type_size * my_data_size);
  if(temp_data == NULL){
    printf("Memory allocation fails ! \n");
    exit(-1);
  }
    
  hid_t  e_dpid;
  e_dpid = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_attr_phase_change(e_dpid, 0, 0);

  char    dataset_name_t[NAME_MAX];
  //dset_id = H5Dcreate(group_id, "/candidate/Energy", H5T_IEEE_F32LE, dataspace_id, H5P_DEFAULT, e_dpid, H5P_DEFAULT);

  hid_t typeid;
  dataspace_id = H5Screate_simple(1, &file_size, NULL);

  MPI_Barrier(MPI_COMM_WORLD);
  t6 = MPI_Wtime();

  for (i = 0; i < dataset_num; i++){
    //for (i = 17; i < 18; i++){
    //MPI_Barrier(MPI_COMM_WORLD);
    sprintf(dataset_name_t, "%s/%s", group_name, dname_array[i].dataset_name);
    //printf("Dataset name %s \n", dataset_name_t);
    dname_array[i].did = H5Dcreate(file_id, dataset_name_t, H5T_IEEE_F32LE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    /* dname_array[i].did = H5Dcreate(file_id, dataset_name_t, dname_array[i].type_id, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */
    if((dname_array[i].did < 0)){
      printf("Error in creating file ! \n");
      exit(-1);
    }
    filespace = H5Dget_space(dname_array[i].did);
    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, my_offset, NULL, count, NULL);
    
    unpackage(data, i, my_data_size, temp_data, row_size);
    
    typeid = H5Dget_type(dname_array[i].did);
    switch (H5Tget_class(typeid)){
      case H5T_INTEGER:
        if(H5Tequal(typeid, H5T_STD_I32LE) == TRUE){
          H5Dwrite(dname_array[i].did, H5T_NATIVE_INT, memspace, filespace, plist_id2, temp_data);
        }else if(H5Tequal(typeid, H5T_STD_I64LE) == TRUE){
          H5Dwrite(dname_array[i].did, H5T_NATIVE_LLONG, memspace, filespace, plist_id2, temp_data);
        }else if(H5Tequal(typeid, H5T_STD_I8LE) == TRUE){
          H5Dwrite(dname_array[i].did, H5T_NATIVE_CHAR, memspace, filespace, plist_id2, temp_data);
        }else if(H5Tequal(typeid, H5T_STD_I16LE) == TRUE){
          H5Dwrite(dname_array[i].did, H5T_NATIVE_SHORT, memspace, filespace, plist_id2, temp_data);
        }
        break;
      case H5T_FLOAT:
        if(H5Tequal(typeid, H5T_IEEE_F32LE) == TRUE){
          H5Dwrite(dname_array[i].did, H5T_NATIVE_FLOAT, memspace, filespace, plist_id2, temp_data);
        }else if(H5Tequal(typeid, H5T_IEEE_F64LE) == TRUE){
          H5Dwrite(dname_array[i].did, H5T_NATIVE_DOUBLE, memspace, filespace, plist_id2, temp_data);
        }
        break;
      default:
        break;
    }
    H5Sclose(filespace);
    H5Dclose(dname_array[i].did);
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  t7 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Write file, takes [%f]s, for each dataset takes [%f]s )\n", (t7-t6), (t7-t6)/dataset_num);
  }
  
  H5Sclose(dataspace_id);
  H5Pclose(e_dpid);
  H5Pclose(plist_id2);
  H5Sclose(memspace);
  H5Gclose(group_id);
  H5Fclose(file_id);
  
  MPI_Barrier(MPI_COMM_WORLD);
  t8 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Close output file,  takes [%f]s)\n", (t8-t7));
  }


  free(temp_data);
  free(size_vector);

  // my_min_value = getLongLongValue(key_index, data);
  //my_max_value = getLongLongValue(key_index, data + (my_data_size - 1) * row_size);
  my_min_value = get_value_double(key_index, data);
  my_max_value = get_value_double(key_index, data + (my_data_size - 1) * row_size);
  
  //printf("Start SDS meta data ! \n");
  /* Write SDS metadata to the file */
  global_metadata_array = (SDSSortTableElem *)malloc( mpi_size * sizeof(SDSSortTableElem));
  global_metadata_array2 = (unsigned long long *)malloc(mpi_size * 2 * sizeof(unsigned long long));
  global_metadata_array1 = (long long *) malloc(mpi_size * 2 * sizeof(long long));

  my_metadata_array.c_min = my_min_value;
  my_metadata_array.c_max = my_max_value;
  my_metadata_array.c_start_offset = my_start_address;
  my_metadata_array.c_end_offset   = my_end_address;
  

  MPI_Barrier(MPI_COMM_WORLD);
  if (mpi_rank == (mpi_size -1) || mpi_rank == 0){
    printf("rank %d:  %f, %f, start: %llu,  enb: %llu \n", mpi_rank, my_min_value, my_max_value, (unsigned long long)my_start_address, (unsigned long long)my_end_address);
  }
  MPI_Allgather(&my_metadata_array, 1, SDS_DATA_TYPE, global_metadata_array, 1, SDS_DATA_TYPE, MPI_COMM_WORLD);

  for (i=0; i < mpi_size; i++){
    global_metadata_array1[i*2]   = global_metadata_array[i].c_min;
    global_metadata_array1[i*2+1] = global_metadata_array[i].c_max;
    global_metadata_array2[i*2]   = global_metadata_array[i].c_start_offset;
    global_metadata_array2[i*2+1] = global_metadata_array[i].c_end_offset;
  }

  if (mpi_rank == 0){
    FILE *file_ptr;
    file_ptr =fopen(filename_attribute, "w");
    if (!file_ptr){
      printf("Can't create attribute file [%s]. \n", filename_attribute);
      return -1;
    }else{
      printf("Create attribute file successful ! \n");
    }
    
    for (i=0; i < mpi_size; i++){
      printf("%f %f %llu %llu, count %d \n", global_metadata_array[i].c_min, global_metadata_array[i].c_max, global_metadata_array[i].c_start_offset, global_metadata_array[i].c_end_offset, (global_metadata_array[i].c_end_offset - global_metadata_array[i].c_start_offset) ); 
      fprintf(file_ptr,"%f %f %llu %llu\n", global_metadata_array[i].c_min, global_metadata_array[i].c_max, global_metadata_array[i].c_start_offset, global_metadata_array[i].c_end_offset); 
    } 
    fclose(file_ptr);
  }
  free(global_metadata_array);
  
  MPI_Barrier(MPI_COMM_WORLD);
  t4 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Write metadata takes [%f]s)\n", (t4-t8));
  }

  //MPI_Info_free(&info);
}


//Phase 1 on paralle sampling sorting
//Sort the data first and select the samples
int phase1(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, char *sample_of_rank0, int row_size){
  char *my_sample;
  int   i, pass, temp_index;
    

  double t1, t2;
  t1 =  MPI_Wtime();
  
  //Sort data based on the type of key
  if(local_sort_threaded == 0){
    qsort_type(data, my_data_size, row_size);
  }else{
    openmp_sort(data, my_data_size, local_sort_threads_num, row_size);
  }
  
  t2 = MPI_Wtime();
  if(mpi_rank == 0){
    printf("Master : qsort take %f secs to sort %d data \n", (t2-t1), my_data_size);
  }
  //Choose sample
  my_sample = malloc(mpi_size * row_size);
  if(my_sample == NULL){
    printf("Memory allocation  fails ! \n");
    exit(-1);
  }
  pass      = my_data_size/mpi_size;
  for(i = 0; i < mpi_size; i++){
    //my_sample[i] = data[pass*(i)];
    temp_index = pass*(i);
    memcpy(my_sample+i*row_size, data+temp_index*row_size, row_size);
  }
    
  //If it is master, just pass my_sample through parameter
  //If it is slave, use MPI_send
  if (mpi_rank != 0){
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Send(my_sample, mpi_size, OPIC_DATA_TYPE, 0, 2, MPI_COMM_WORLD);
  }else{
    //  sample_of_rank0[i] = my_sample[i];
    memcpy(sample_of_rank0, my_sample, row_size * mpi_size);
    printf("First 5 samples at root :");
    for (i=0; i< 5; i++) {
      printf("%f ",  get_value_double(key_index, my_sample + i*row_size ));
    }
    printf("\n");
  }
  
  free(my_sample);
  return 0;
}

//Phase 2 of the parallel sampling sorting
// 1, Reseive the pilots from the master
// 2, Exchange the data 
// 3, Sort the data again 
int phase2(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, char *pivots, int rest_size, int row_size){
  int           dest = 0;
  //long long     p_pivot, n_pivot, cur_value, t_value;
  double         p_pivot, n_pivot, cur_value, t_value;

  int           i = 0, t;
  int           j = 0, k;
  int           cont;
  char         *send_buff, *recv_buff, *final_buff, *temp_final_buff;
  double        t1, t2; 
  hsize_t       max_size;
  hsize_t       temp_my_data_size;
  int          *scount, *sdisp;
  int          *rcount, *rdisp;

  scount = malloc(mpi_size * sizeof(int));
  rcount = malloc(mpi_size * sizeof(int));
  sdisp  = malloc(mpi_size * sizeof(int));
  rdisp  = malloc(mpi_size * sizeof(int));
  
  
  //Last process may have more data
  if ((mpi_rank != (mpi_size-1) )){
    temp_my_data_size = my_data_size + rest_size;
  }else{
    temp_my_data_size = my_data_size;
  }
  
  max_size   = temp_my_data_size * 3;
  //send_buff  = malloc((temp_my_data_size )*row_size);
  //recv_buff  = malloc((temp_my_data_size )*row_size);
  //final_buff = malloc(max_size * row_size);  
  

  //Start exchanging the data  
  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime(); 	 
  //if(mpi_rank == 0 || mpi_rank == (mpi_size -1)){
  //  printf("   Exchange data is running.... max_size: %lu, temp_my_size: %lu \n", (unsigned long)max_size, (unsigned long)temp_my_data_size);
  //}


  int ii, jj, replicated_jj, replicated_jj_start, previous_ii,  pp_ii;
  int dest_pivot_replicated      = 0, dest_pivot_has_same_pivot_as_me = 0; 
  int dest_pivot_replicated_size;
  int dest_pivot_replicated_rank;
  double p_value_head;

  for(k = 0; k < mpi_size; k++)
    scount[k] = 0;
  
  //Start from dest 0 (mpi_rank 0)
  dest   = 0;
  
  //my_pivot starts from zero
  double my_pivot, dest_pivot;
  if(mpi_rank != (mpi_size -1)){
    my_pivot   = get_value_double(key_index, pivots + mpi_rank *row_size);;
  }else{
    my_pivot   = higest_double; //This is the maximum value
  }
  
  int rank_equal, rank_less, partition_size;

  previous_ii = 0;
  for(k = 0; k < mpi_size; k++){
    if(dest != (mpi_size -1)){
      dest_pivot   = get_value_double(key_index, pivots + dest*row_size);
    }else{
      dest_pivot   = higest_double;
    }
    jj = 0; pp_ii = previous_ii;
    for (ii=previous_ii; ii < my_data_size; ii++){
      previous_ii =  jj;
      cur_value = get_value_double(key_index, data+ii*row_size);
      if(cur_value <= dest_pivot){  //memcpy(send_buff+jj*row_size, data+ii*row_size, row_size);
        jj++;
      }else{
        break;
      }
    }
    
    //Does the data is skewed 
    dest_pivot_replicated  = pivots_replicated(pivots, dest, &dest_pivot_replicated_size, &dest_pivot_replicated_rank, mpi_size, mpi_rank, row_size, &p_value_head);
    if(skew_data == 1 && dest_pivot_replicated){ 
      //replicated_size = previous_ii - pp_ii;
      //replicated_chunk_size  = replicated_size/mpi_size;
      //if()
      rank_pivot(pivots, data, my_data_size, dest, &rank_less, &rank_equal, row_size, mpi_rank, p_value_head);
      if(rank_less == 0){
        //replciated pivots are the smallest values. do equally parition the data
        if(rank_equal % dest_pivot_replicated_size == 0){
            partition_size = rank_equal / dest_pivot_replicated_size;
        }else{
          if(dest_pivot_replicated_rank == (dest_pivot_replicated_size - 1)) {
            partition_size = rank_equal / dest_pivot_replicated_size + rank_equal % dest_pivot_replicated_size;
          }else{
            partition_size = rank_equal / dest_pivot_replicated_size;
          }
        }
      }else{
        if(dest_pivot_replicated_rank == 0){
          if(rank_less < rank_equal/(dest_pivot_replicated_size - 1)){
            partition_size = (rank_equal + rank_less)/(dest_pivot_replicated_size);
          }else{
            partition_size = rank_less;
          }
        }else{
          if(dest_pivot_replicated_rank != (dest_pivot_replicated_size - 1)){
            if(rank_less < rank_equal/(dest_pivot_replicated_size - 1)){
              partition_size      =  (rank_equal + rank_less)/(dest_pivot_replicated_size);
            }else{
              partition_size      =  rank_equal / dest_pivot_replicated_size;
            }
          }else{
            if(rank_less < rank_equal/(dest_pivot_replicated_size - 1)){
              partition_size      =  (rank_equal + rank_less)/(dest_pivot_replicated_size) + (rank_equal + rank_less)%(dest_pivot_replicated_size);
            }else{
              partition_size      =  rank_equal / dest_pivot_replicated_size + rank_equal % dest_pivot_replicated_size;
            }
          }
        }
      }
      previous_ii = pp_ii + partition_size;
      jj = partition_size;
    }//end of replicated = 1 and  skew_data = 1
    
    scount[dest] = jj;
    dest = (dest + 1) % mpi_size;
    if(previous_ii >= my_data_size )
      break;
  }//End of for(k
  
  
  
  //Figure out all-to-all vector parameters
  MPI_Alltoall(scount, 1, MPI_INT, rcount, 1,MPI_INT, MPI_COMM_WORLD);
  if(mpi_rank == 0)
    printf("Done for assigning data based on pivots !\n");
  
  sdisp[0] = 0;
  for(i = 1; i <= mpi_size; i++){
    sdisp[i] = sdisp[i-1] + scount[i-1];
    if(verbose && mpi_rank == 1 ) //|| mpi_rank  == (mpi_size -1))
      printf("mpi rank (%d) scount[%d]   %d rcount[%d] %d \n ", mpi_rank, i-1,  scount[i-1], i-1, rcount[i-1]);
  }
  
  //if(mpi_rank == 0 || mpi_rank  == (mpi_size -1))
  //  printf("mpi rank (%d) scount[0]=%d rcount[0] %d \n ", mpi_rank, scount[0], rcount[0]);
  
  rdisp[0] = 0;
  for(i = 1; i <= mpi_size; i++)
    rdisp[i] = rdisp[i-1] + rcount[i-1];
  
  unsigned long long rsize = 0;
  unsigned long long ssize = 0;
  for (i = 0; i < mpi_size; i++){
    rsize = rsize + (unsigned long long)rcount[i];
    ssize = ssize + (unsigned long long)scount[i];
  }
  
  //if(ssize != my_data_size){
  //  printf("At rank %d, Size mismatch after assigning on pivots: send size (%d), my_data_size (%llu) ! \n", mpi_rank, ssize, my_data_size);
  //  exit(-1);
  //}
  
  if(collect_data == 1){
    final_buff = malloc(rsize * row_size);  
    if(final_buff == NULL){
      printf("Allocation for final_buff fails !\n");
      exit(0);
    }
    //mpi_err = MPI_Alltoallv(sray,scounts,sdisp,MPI_INT, rray,rcounts,rdisp,MPI_INT, MPI_COMM_WORLD);
    MPI_Alltoallv(data, scount, sdisp, OPIC_DATA_TYPE, final_buff, rcount, rdisp, OPIC_DATA_TYPE, MPI_COMM_WORLD);
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  
  unsigned long long load, sum_load, sq_load, sumsq_load, max_load, min_load;
  double var_load;
  load = rsize;
  MPI_Allreduce(&load, &max_load, 1, MPI_UNSIGNED_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
  MPI_Allreduce(&load, &min_load, 1, MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
  MPI_Allreduce(&load, &sum_load, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
  sq_load = load - (sum_load / mpi_size);
  sq_load = sq_load * sq_load;
  MPI_Allreduce(&sq_load, &sumsq_load, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
  if(mpi_size > 1) {
    var_load  = sumsq_load / (mpi_size-1);
  }else {
    var_load = 0;
  }
  
  if(mpi_rank == 0 || mpi_rank == (mpi_size -1))
    printf("Exchange data ends, my_rank %d, orgi_size %d, new_size %llu, taking [%f]s, (max %llu, min %llu, var %f) \n", mpi_rank, my_data_size, rsize, (t2-t1), max_load, min_load, var_load);
   
  free(rdisp);
  free(sdisp);
  free(rcount);
  free(scount);

  //free(recv_buff);
  //free(send_buff);
  
  if(package_data != NULL){
    free(package_data);
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime();
  
  
  if(collect_data == 1){
    if(local_sort_threaded == 0){
      qsort_type(final_buff, rsize, row_size);
    }else{
      openmp_sort(final_buff, rsize, local_sort_threads_num, row_size);
    }
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf("Sorting aftering takes [%f]s)\n", (t2-t1));
  }

  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime();
  
  /* For test, we only consider the sorting time */
  if(write_result == 1)
    write_reault_file(mpi_rank, mpi_size, final_buff, rsize, row_size);
  
  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf("Write result file takes [%f]s)\n", (t2-t1));
  }
  
  if(collect_data == 1)
    free(final_buff);
  return 0;
}


// Master does slave's job, and also gather and sort pivots
int master(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, int rest_size, int row_size){
  int i,j,pass;
    
  char        *all_samp, *temp_samp;
  char        *pivots;
  double       t1, t2; 

  /*All samples*/
  all_samp  = malloc(mpi_size * mpi_size * row_size);
  temp_samp = malloc(mpi_size * row_size);
  pivots    = malloc((mpi_size - 1)*row_size);
   	
  t1 = MPI_Wtime();
  printf("Phase1 is running .... \n"); 
  /*Sort its own data, and select samples*/
  phase1(0, mpi_size, data, my_data_size, temp_samp, row_size);
  t2 = MPI_Wtime();
  printf("Master's phase1 taking [%f]s \n", (t2-t1));

  /*Store its own sample into all_samp */
  //for (i=0; i<mpi_size; i++) {
  //all_samp[i]=temp_samp[i];
  //}
  memcpy(all_samp, temp_samp, mpi_size * row_size);
  
  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime();
  printf("Master receives samples ....\n");
  //Gather sample from slaves
  for(i = 1; i < mpi_size; i++){
    //printf("rank 0 -> I am waiting %d \n", i);
    MPI_Recv(temp_samp, mpi_size, OPIC_DATA_TYPE, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &Stat);
    //for(j = 0; j < mpi_size; j++){
    //all_samp[i*mpi_size + j] = temp_samp[j];
    memcpy(all_samp + i * mpi_size * row_size, temp_samp,  mpi_size * row_size);
    //}
  }
  free(temp_samp);
  t2 = MPI_Wtime();
  printf("Receiving samples taks [%f]s \n", (t2-t1));

  //Sort samples and choose pivots
  qsort_type(all_samp, (mpi_size * mpi_size), row_size);

  pass = mpi_size * mpi_size/(mpi_size);

  if(verbose){
    printf("All samples: ");
    for(i = 0; i < mpi_size * mpi_size; i++){
      printf("%lf, ", get_value_double(key_index, all_samp + row_size*i));
    }
    printf("\n");
  }
  
  int rou;
  double previous_pivot = lowest_double, cur_pivot;
  rou = mpi_size / 2;
  printf("Pivots : ");
  for(i = 0; i < (mpi_size-1); i++){
    //pivots[i] = all_samp[pass*(i+1)]; 
    //pivots[i] = all_samp[mpi_size*(i+1) + rou -1]; 
    memcpy(pivots+row_size*i, all_samp + (mpi_size*(i+1) + rou -1)*row_size, row_size);
    cur_pivot = get_value_double(key_index,   pivots+row_size*i);
    //cur_rank  = get_value_double(key_index+1, pivots+row_size*i);
    if(previous_pivot ==  cur_pivot && i < 60){
      printf(" (Same pivot)");
    }
    previous_pivot = cur_pivot;
    //memcpy(pivots+row_size*i, all_samp + pass *(i+1)*row_size, row_size);
    if(i < 60)
      printf("%f, ", cur_pivot);
  }
  printf("\n");
  free(all_samp);
  
  //To all slaves
  MPI_Bcast(pivots, (mpi_size-1), OPIC_DATA_TYPE, 0, MPI_COMM_WORLD);
	
  printf("Phase2 is running... \n");
  //To sort and write sorted file
 
  phase2(0, mpi_size, data, my_data_size, pivots, rest_size, row_size);
  free(pivots);
    
  return 0;
}

/*Do sort and sample*/
int slave(int mpi_rank, int mpi_size, char *data, int64_t my_data_size, int rest_size, int row_size){
  char  *pivots;
    
  phase1(mpi_rank, mpi_size, data, my_data_size, NULL, row_size);

  //Receive pivots from masters
  pivots = malloc((mpi_size-1) * row_size);
  MPI_Bcast(pivots, (mpi_size-1), OPIC_DATA_TYPE, 0, MPI_COMM_WORLD);
    
  phase2(mpi_rank, mpi_size, data, my_data_size, pivots, rest_size, row_size);
  free(pivots);
  
  return 0;
}

//Sort the data based on the type
int qsort_type(void *data, int64_t my_data_size, size_t row_size){
  switch(key_value_type){
    case H5GS_INT64:
      qsort(data, my_data_size, row_size, CompareInt64Key);
      break;
    case H5GS_INT32:
      qsort(data, my_data_size, row_size, CompareInt32Key);
      break;
    case H5GS_FLOAT32:
      qsort(data, my_data_size, row_size, CompareFloat32Key);
      break;
    case H5GS_FLOAT64:
      qsort(data, my_data_size, row_size, CompareFloat64Key);
      break;
    default:
      printf("Un-supported data type ! \n");
      exit(-1);
      break;
  }
}

//Get the index-th data in the row (row_data) as double
double get_value_double(int index, char *row_data){
  double value;
  switch(key_value_type){
    case H5GS_INT64:
      value = (double) getInt64Value(index, row_data);
      break;
    case H5GS_INT32:
      value = (double) getInt32Value(index, row_data);
      break;
    case H5GS_FLOAT32:
      value = (double) getFloat32Value(index, row_data);
      break;
    case H5GS_FLOAT64:
      value = (double) getFloat64Value(index, row_data);
      break;
    default:
      printf("Un-supported data type ! \n");
      break;
  }
  return value;
}

int pivots_replicated(char *pivots, int dest, int *dest_pivot_replicated_size, int *dest_pivot_replicated_rank, int mpi_size, int mpi_rank, int row_size, double *p_value_head){
  int replicated = 0;
  int replicated_size = 1; //At least have one 
  double dest_pivot;
  if(dest != (mpi_size - 1)){
    dest_pivot= get_value_double(key_index, pivots + dest*row_size);
  }else{
    dest_pivot = higest_double;
  }
  
  double next_pivot;
  int i;
  i = dest - 1;
  while(i >= 0){
    if(i != (mpi_size - 1)){
      next_pivot = get_value_double(key_index, pivots + i*row_size);
    }else{
      next_pivot = higest_double;
    }
    
    if(next_pivot == dest_pivot){
      i--;
      replicated = 1;
      replicated_size++;
    }else{
      break;
    }
  }

  if(i >= 0){
    *p_value_head = next_pivot;
  }else{
    *p_value_head = lowest_double;
  }

  //Rank starts from 0
  *dest_pivot_replicated_rank = replicated_size - 1;
  
  i= dest + 1;
  while(i < mpi_size){
    if(i != (mpi_size - 1)){
      next_pivot = get_value_double(key_index, pivots + i*row_size);
    }else{
      next_pivot = higest_double;
    }
    if(next_pivot == dest_pivot){
      i++;
      replicated = 1;
      replicated_size++;
    }else{
      break;
    }
  }
  
  *dest_pivot_replicated_size = replicated_size;
  
  return replicated;
}

void rank_pivot(char *pivots, char *data,  int64_t my_data_size, int dest_pivot, int *rank_less, int *rank_equal, int row_size, int mpi_size, double p_value_head){
  double dest_pivot_value;    
  if(dest_pivot != (mpi_size - 1)){
    dest_pivot_value = get_value_double(key_index, pivots+dest_pivot*row_size);
  }else{
    dest_pivot_value = higest_double;
  }
  int    less_count = 0 , equal_count = 0;
  double cur_value;
  
  int i;
  for (i = 0 ; i < my_data_size; i++){
    cur_value = get_value_double(key_index, data+i*row_size);
    
    if(cur_value < dest_pivot_value && cur_value > p_value_head ){
      less_count++;
    }
    
    if(cur_value == dest_pivot_value){
      equal_count++;
    }
    
    if( cur_value > dest_pivot_value){
      break;
    }
  }

  *rank_less  =  less_count;
  *rank_equal =  equal_count;
  
}


/* Merge sorted lists A and B into list A.  A must have dim >= m+n */
void merge(char *A, char *B, int m, int n, int row_size) 
{
  int   i=0, j=0, k=0, p;
  int   size = m+n;
  double v1, v2;
  
  char *C = (char *)malloc(size * row_size);
  v1 = getFloat64Value(key_index, A+i*row_size);
  v2 = getFloat64Value(key_index, B+j*row_size);
  while (i < m && j < n) {
    if (v1 <= v2){
      memcpy(C+k*row_size, A+i*row_size, row_size);
      i++;
      v1 = getFloat64Value(key_index, A+i*row_size);
    }else {
      memcpy(C+k*row_size, B+i*row_size, row_size);
      j++;
      v2 = getFloat64Value(key_index, B+j*row_size);
    }
    k++;
  }

  if (i < m){
    //for (p = i; p < m; p++,k++){
    //  memcpy(C+k*row_size, A+p*row_size, row_size);
    //}
    memcpy(C+k*row_size, A+i*row_size, (m-i)*row_size);
  }else{
    //for(p = j; p < n; p++,k++){
    //  memcpy(C+k*row_size, B+p*row_size, row_size);
    //}
    memcpy(C+k*row_size, B+j*row_size, (n-j)*row_size);
  }
  
  //for( i=0; i<size; i++ ) 
  //  memcpy(A+i*row_size, C+i*row_size, row_size);
  memcpy(A, C, size * row_size);
  free(C);
}
  
/* Merges N sorted sub-sections of array a into final, fully sorted array a */
void arraymerge(char *a, int size, int *index, int N, int row_size)
{
  /*
  int i,   thread_size; 
  
  while(N>1){
    thread_size = size/N; //Check (size % N != 0)
    for( i=0; i< N; i++ ){ 
      index[i]=i * thread_size; 
    }
    index[N]=size;
    
#pragma omp parallel for private(i) 
    for( i=0; i<N; i+=2 ) {
      merge(a+(index[i]*row_size), a+(index[i+1]*row_size), index[i+1]-index[i], index[i+2]-index[i+1], row_size);
    }
    N /= 2;
  }
  */
}

int openmp_sort(char *data, int size, int threads, size_t row_size)
{
  /*
  int i;
  omp_set_num_threads(threads);
  
  //int threads = mic_threads * mpi_size;
  //printf("Using [%d] threads each node \n", threads);
  
  //omp_set_num_threads(threads);
  int *index       = (int *)malloc((threads+1)*sizeof(int));
  int  thread_size = size/threads; //We might comeback to check data_size%threads
  printf(" size of each thread %d \n", thread_size);
  
  for(i=0; i<threads; i++){
    index[i]=i * thread_size; 
    printf("%d ", index[i]);
  }
  index[threads]=size;
  printf("%d (index)\n", index[threads]);

  
  // Main parallel sort loop 
  double start = omp_get_wtime();
  
#pragma omp parallel for private(i)
  for(i=0; i<threads; i++){ 
    //qsort(a+index[i], index[i+1]-index[i], sizeof(int), CmpInt);
    qsort_type(data+(index[i]*row_size), index[i+1]-index[i], row_size);
    //qsort(data+index[i], index[i+1]-index[i], row_size, CompareInt32Key);
  }
  
  //printf("Sorting is done ! \n");
  double middle = omp_get_wtime();
  
  // Merge sorted array pieces 
  if(threads>1 ) 
    arraymerge(data, size, index, threads, row_size);

  double end = omp_get_wtime();
  printf("sort time = %g s, of which %g is sort time , %g s is merge time\n",end-start, middle-start, end-middle);
  
   */
  return 0;
}
