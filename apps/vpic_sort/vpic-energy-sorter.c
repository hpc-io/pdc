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
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  */
/*                                                                              */
/*   Files:         Have to list the files                                      */
/*                                                                              */
/*   Description:  Sort HDF5 datasets based on a HDF5 dataset		       	*/
/*		   Read HDF5 datasets, sort them according to one dataset	*/
/*		   Write all the datasets to a new file			        */
/*                                                                              */
/*   Author:  Bin Dong								*/
/*            Lawrence Berkeley National Lab                			*/
/*            email: DBin@lbl.gov                                               */
/*                                                                              */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  */

#include "stdlib.h"
#include "hdf5.h"
#include <unistd.h>
#include <string.h>

#ifdef SDS_HDF5_VOL
#include <sds-vol-external-native.h>
#endif

#define NAME_MAX      255
#define MAX_DATASET   20
char    filename[NAME_MAX];
char    groupname[NAME_MAX];
char    dsetname[NAME_MAX];
char    filename_sorted[NAME_MAX];
char    filename_attribute[NAME_MAX];
char    cb_buffer_size[NAME_MAX];
char    cb_nodes[NAME_MAX];
char    dataset_id[NAME_MAX];
char    dataset_array[MAX_DATASET][NAME_MAX];


MPI_Status   Stat;
MPI_Datatype OPIC_DATA_TYPE;
MPI_Datatype SDS_DATA_TYPE;


typedef struct{
  float e;
  float x;
  float y;
  float z;
}OpicData;

typedef struct{
  float   c_min;
  float   c_max;
  unsigned long long c_start_offset;
  unsigned long long c_end_offset;
}SDSSortTableElem;

void CombineXYZE(float *x, float *y, float *z, float *e, OpicData *all, hsize_t len){
  int i;
  for (i = 0; i < len; i++) {
    all[i].e = e[i];
    all[i].x = x[i];
    all[i].y = y[i];
    all[i].z = z[i];
  }
}

void SplitXYZE(float *x, float *y, float *z, float *e, OpicData *all, hsize_t len){
  int i;
  for (i = 0; i < len; i++) {
    e[i] = all[i].e;
    x[i] = all[i].x;
    y[i] = all[i].y;
    z[i] = all[i].z;
  }
}

int CompareXYZE(const void *a, const void *b){
  if(((OpicData *)a)->e == ((OpicData *)b)->e){
    return 0;
  }
    
  if(((OpicData *)a)->e < ((OpicData *)b)->e){
    return -1;
  }else{
    return 1;
  }
}

int CompareE(const void *a, const void *b){
  if(*(float *)a ==  *(float *)b)
    return 0;
    
  if(*(float *)a <  *(float *)b){
    return -1;
  }else{
    return 1;
  }
        
    
}

int write_result_file(int mpi_rank, int mpi_size, OpicData *data, hsize_t my_data_size){
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info;
  hid_t    plist_id, plist_id2, file_id, group_id, dataspace_id, dset_id, filespace, memspace;
  hid_t    x_id, y_id, z_id, x_space,y_space,z_space;
  hsize_t  i, j, *size_vector;
  hsize_t  file_size, my_offset[1], count[1];
  hsize_t  dims[1];
  float    *e_data, *x_data, *y_data, *z_data;
    
  float    my_min_value, my_max_value;
  hsize_t  my_start_address, my_end_address;
  SDSSortTableElem    *global_metadata_array;
  SDSSortTableElem    my_metadata_array;
  float               *global_metadata_array1;
  unsigned long long  *global_metadata_array2;

  double t1, t2, t3, t4, t5, t6, t7, t8;
  MPI_Barrier(MPI_COMM_WORLD);
  t1 =  MPI_Wtime();


  MPI_Info_create(&info); 
  MPI_Info_set(info, "cb_buffer_size", "157810672");
  MPI_Info_set(info, "cb_nodes", "128");

  // printf("my_rank %d, write file\n ", mpi_rank);
 
  //Gather the size of data belonging to each proccess
  size_vector = malloc(mpi_size*sizeof(hsize_t));

  
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Allgather(&my_data_size,1, MPI_UNSIGNED_LONG_LONG, size_vector, 1, MPI_UNSIGNED_LONG_LONG, MPI_COMM_WORLD);

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
  H5Pset_libver_bounds (plist_id, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  
  //Register SDS external native plugin
  hid_t acc_tpl;
  acc_tpl = H5Pcreate(H5P_FILE_ACCESS);

#ifdef SDS_HDF5_VOL
  hid_t under_dapl;
  hid_t vol_id;
  under_dapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(under_dapl, comm, info);
  vol_id  = H5VLregister(&H5VL_sds_external_native_g);
  external_native_plugin_id = H5VLget_plugin_id("native");
  assert(external_native_plugin_id > 0);
  H5Pset_vol(acc_tpl, vol_id, &under_dapl);
  //End of Register SDS external native plugin
#else
  plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(acc_tpl, comm, info);
#endif

  //file_id = H5Fcreate(filename_sorted,H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
  file_id = H5Fcreate(filename_sorted,H5F_ACC_TRUNC, H5P_DEFAULT, acc_tpl);
  H5Pclose(plist_id);
    
  group_id = H5Gcreate(file_id, "/Step#0", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    
    
  dataspace_id = H5Screate_simple(1, &file_size, NULL);
  hid_t  e_dpid;
  e_dpid = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_attr_phase_change (e_dpid, 0, 0);

  dset_id = H5Dcreate(group_id, "/Step#0/Energy", H5T_IEEE_F32LE, dataspace_id, H5P_DEFAULT, e_dpid, H5P_DEFAULT);
  x_id = H5Dcreate(group_id, "/Step#0/x", H5T_IEEE_F32LE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  y_id = H5Dcreate(group_id, "/Step#0/y", H5T_IEEE_F32LE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  z_id = H5Dcreate(group_id, "/Step#0/z", H5T_IEEE_F32LE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    
  H5Sclose(dataspace_id);
    
    
  count[0] = my_data_size;
  filespace = H5Dget_space(dset_id);
  x_space = H5Dget_space(x_id);
  y_space = H5Dget_space(y_id);
  z_space = H5Dget_space(z_id);
    
  H5Sselect_hyperslab(filespace, H5S_SELECT_SET, my_offset, NULL, count, NULL);
  H5Sselect_hyperslab(x_space, H5S_SELECT_SET, my_offset, NULL, count, NULL);
  H5Sselect_hyperslab(y_space, H5S_SELECT_SET, my_offset, NULL, count, NULL);
  H5Sselect_hyperslab(z_space, H5S_SELECT_SET, my_offset, NULL, count, NULL);
    
    
  memspace = H5Screate_simple(1, count, NULL);
    
  plist_id2 = H5Pcreate(H5P_DATASET_XFER);
  H5Pset_dxpl_mpio(plist_id2, H5FD_MPIO_COLLECTIVE);
    
  e_data = malloc(my_data_size*sizeof(float));
  x_data = malloc(my_data_size*sizeof(float));
  y_data = malloc(my_data_size*sizeof(float));
  z_data = malloc(my_data_size*sizeof(float));
  
  if (mpi_rank == 0){
    printf("Writing result file and meta data... \n");
  }
  SplitXYZE(x_data,y_data,z_data,e_data, data,my_data_size);
  my_min_value = e_data[0];
  my_max_value = e_data[my_data_size -1]; 

  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Gather offeset,create file and other prepare work  takes [%f]s)\n", (t2-t1));
  }
  

  H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, plist_id2, e_data);
  H5Dwrite(x_id, H5T_NATIVE_FLOAT, memspace, x_space, plist_id2, x_data);
  H5Dwrite(y_id, H5T_NATIVE_FLOAT, memspace, y_space, plist_id2, y_data);
  H5Dwrite(z_id, H5T_NATIVE_FLOAT, memspace, z_space, plist_id2, z_data);

  MPI_Barrier(MPI_COMM_WORLD);
  t3 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Write data takes [%f]s)\n", (t3-t2));
  }
    
  free(size_vector);
  free(e_data);
  free(x_data);
  free(y_data);
  free(z_data);

  //printf("Start SDS meta data ! \n");
  
  /* Write SDS metadata to the file */
  global_metadata_array = (SDSSortTableElem *)malloc( mpi_size * sizeof(SDSSortTableElem));
  global_metadata_array2 = (unsigned long long *)malloc(mpi_size * 2 * sizeof(unsigned long long));
  global_metadata_array1 = (float *) malloc(mpi_size * 2 * sizeof(float));

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
      printf("%f %f %llu %llu\n", global_metadata_array[i].c_min, global_metadata_array[i].c_max, global_metadata_array[i].c_start_offset, global_metadata_array[i].c_end_offset); 
      fprintf(file_ptr,"%f %f %llu %llu %llu\n", global_metadata_array[i].c_min, global_metadata_array[i].c_max, global_metadata_array[i].c_start_offset, global_metadata_array[i].c_end_offset, (global_metadata_array[i].c_end_offset - global_metadata_array[i].c_start_offset)); 
    } 
    fclose(file_ptr);
  }
  
  free(global_metadata_array);
  
  
  //hsize_t  sds_attr_dim[1], sort_attr_dim[2], sort_attr_dim2[2];
  //hid_t    sds_dataspace_id, sort_dataspace_id, sds_attr_id, sort_attr_id, sort_attr_id2, sort_dataspace_id2;
  //int      sds_attr_data[SDS_FILE_ATT_MAX_LENGTH];

  /* Create first dataset attribute: SDS FLAG 
  sds_attr_dim[0]    =  SDS_FILE_ATT_MAX_LENGTH;
  sds_dataspace_id   =  H5Screate_simple(1, sds_attr_dim, NULL);
  sds_attr_id        =  H5Acreate(dset_id, SDS_FILE_ATT_NAME, H5T_STD_I32LE, sds_dataspace_id, H5P_DEFAULT, H5P_DEFAULT);
  sds_attr_data[0] = SDS_FILE;
  sds_attr_data[1] = SDS_TYPE_SORT;
  sds_attr_data[2] = mpi_size;
  sds_attr_data[3] = 4;
  H5Awrite(sds_attr_id, H5T_NATIVE_INT, sds_attr_data);
  H5Sclose(sds_dataspace_id);
  H5Aclose(sds_attr_id);*/

 

  /* Create second dataset attribute: write SDS_SORT_VALUE 
  sort_attr_dim[0] = mpi_size;
  sort_attr_dim[1] = 2;
  sort_dataspace_id = H5Screate_simple(2, sort_attr_dim, NULL);
  sort_attr_id      = H5Acreate (dset_id, SDS_TYPE_SORT_ATT_NAME, H5T_NATIVE_FLOAT, sort_dataspace_id, H5P_DEFAULT, H5P_DEFAULT);
  H5Awrite(sort_attr_id, H5T_NATIVE_FLOAT, global_metadata_array1);
  free(global_metadata_array1);
  H5Sclose(sort_dataspace_id);
  H5Aclose(sort_attr_id);

  sort_attr_dim2[0] = mpi_size;
  sort_attr_dim2[1] = 2;
  sort_dataspace_id2 = H5Screate_simple(2, sort_attr_dim2, NULL);
  sort_attr_id2      = H5Acreate (dset_id, SDS_TYPE_SORT_ATT_NAME2, H5T_NATIVE_ULLONG, sort_dataspace_id2, H5P_DEFAULT, H5P_DEFAULT);
  H5Awrite(sort_attr_id2, H5T_NATIVE_ULLONG, global_metadata_array2);
  free(global_metadata_array2);
  H5Sclose(sort_dataspace_id2);
  H5Aclose(sort_attr_id2);
  */
  MPI_Barrier(MPI_COMM_WORLD);
  t4 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Write metadata takes [%f]s)\n", (t4-t3));
  }


  H5Pclose(e_dpid);
  H5Pclose(plist_id2);
  H5Sclose(filespace);
  H5Sclose(x_space);
  H5Sclose(y_space);
  H5Sclose(z_space);
  H5Sclose(memspace);

  MPI_Barrier(MPI_COMM_WORLD);
  t5 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Close file space,  takes [%f]s)\n", (t5-t4));
  }

  H5Dclose(dset_id);
  H5Dclose(x_id);
  H5Dclose(y_id);
  H5Dclose(z_id);

  MPI_Barrier(MPI_COMM_WORLD);
  t6 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Close output dataset,  takes [%f]s)\n", (t6-t5));
  }

   
  H5Gclose(group_id);

  MPI_Barrier(MPI_COMM_WORLD);
  t7 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Close output group,  takes [%f]s)\n", (t7-t6));
  }

  H5Fclose(file_id);

  H5Pclose(acc_tpl);
#ifdef SDS_HDF5_VOL
  //Unregistered the plugin-id
  H5Pclose(under_dapl);
  H5VLunregister(vol_id);
#endif
  //End of Unregistered the plugin-id


  MPI_Barrier(MPI_COMM_WORLD);
  t8 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf(" ---Close output file,  takes [%f]s)\n", (t8-t7));
  }

  MPI_Info_free(&info);
}

int phase1(int mpi_rank, int mpi_size, OpicData *data,int my_data_size, OpicData *sample_of_rank0){
  OpicData *my_sample;
  int i, pass;
    
  //ShellSort(data,my_data_size);
  double t1, t2;
  t1 =  MPI_Wtime();
  qsort(data, my_data_size, sizeof(OpicData),CompareXYZE);
  t2 = MPI_Wtime();
  if(mpi_rank == 0){
    printf("Master : qsort take %f secs to sorr %d data \n", (t2-t1), my_data_size);
  }
  //Choose sample
  my_sample = malloc(mpi_size * sizeof(OpicData));
  pass = my_data_size/mpi_size;
  for(i = 0; i < mpi_size; i++){
    my_sample[i] = data[pass*(i)];
  }
    
  //It it is master, just pass my_sample through parameter
  //It it is slave, use MPI_send
  if (mpi_rank != 0){
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Send(my_sample, mpi_size, OPIC_DATA_TYPE, 0, 2, MPI_COMM_WORLD);
  }else{
    for (i=0; i<mpi_size; i++) {
      sample_of_rank0[i] = my_sample[i];
    }
  }
    
  free(my_sample);
  return 0;
}

int phase2(int mpi_rank, int mpi_size, OpicData *data, int my_data_size, OpicData *pivots, int rest_size){
  int    dest = 0;
  float p_pivot, n_pivot;
  int   i = 0, t;
  int   j = 0, k;
  hsize_t   m;
  int   cont;
  OpicData *send_buff, *recv_buff, *final_buff, *temp_final_buff;
  double t1, t2; 
  hsize_t   max_size;
  hsize_t   temp_my_data_size;
  
  
  //Last process may have more data
  if ((mpi_rank != (mpi_size-1) )){
    temp_my_data_size = my_data_size + rest_size;
  }else{
    temp_my_data_size = my_data_size;
  }

  max_size =  temp_my_data_size * 3;
  send_buff = malloc((temp_my_data_size )*sizeof(OpicData));
  recv_buff = malloc((temp_my_data_size )*sizeof(OpicData));
  final_buff = malloc(max_size * sizeof(OpicData));  
  
  if(final_buff == NULL){
    printf("ALlocation for final_buff is aboted !\n");
    exit(0);
  }

  /*Buffer initialization*/
  for(i = 0; i < temp_my_data_size; i++) {
    send_buff[i].e = -1;
    recv_buff[i].e = -1;
    //final_buff[i].e = 0;
  }
 

  //Start exchanging the data  
  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime(); 	 
  if(mpi_rank == 0 || mpi_rank == (mpi_size -1)){
    printf("   Exchange data is running.... max_size: %lu, temp_my_size: %lu \n", (unsigned long)max_size, (unsigned long)temp_my_data_size);
  }


  int source_jj;
  int source;
  int ii, jj;
  
  m = 0;
  source  = mpi_rank;
  dest    = mpi_rank;
  if (dest == 0){
    p_pivot = 0; 
  }else{
    p_pivot  = pivots[dest-1].e;
  }

  for(k = 0; k < mpi_size; k++){
    jj = 0;
    for (ii = 0 ; ii < my_data_size; ii++){
      if( (data[ii].e > p_pivot && data[ii].e <= pivots[dest].e) || ( ((dest == (mpi_size-1)) && data[ii].e > p_pivot) ) ){
	//   if(( (data[ii].e > p_pivot) && (data[ii].e <= pivots[dest].e || dest == (mpi_size-1)) )){
	send_buff[jj] = data[ii];
	jj++;
      }
    }
    
    //MPI_Barrier(MPI_COMM_WORLD);
    
    if(mpi_rank != dest){
      MPI_Sendrecv(&jj, 1, MPI_INT, dest, 4, &source_jj, 1, MPI_INT, source, 4, MPI_COMM_WORLD,&Stat);
      MPI_Sendrecv(send_buff, jj, OPIC_DATA_TYPE, dest, 3, recv_buff, source_jj, OPIC_DATA_TYPE,source,3,MPI_COMM_WORLD,&Stat);
    }else{
      //If it belongs to itself, don't use MPI_Sendrecv
      source_jj = jj;
      for (t=0; t < source_jj; t++) {
	recv_buff[t] = send_buff[t];
      }
    }
    
    if((mpi_rank == 0) && ((k%400) == 0))
      printf("   Exchange at [%d] stage, source jj [%d], \n ", k, source_jj);
    
    for(i = 0; i < source_jj; i++){
      if (recv_buff[i].e != -1){
	final_buff[m] = recv_buff[i];
	m++;
	if( m >= max_size ){
	  //printf("Final_buff is too small, Try to increase it !\n");
	  temp_final_buff = malloc(max_size * sizeof(OpicData));  
	  if(temp_final_buff == NULL){
	    printf("ALlocation for final_buff is aboted for temp_final_buff !\n");
	    exit(0);
	  }
	  memcpy(temp_final_buff, final_buff, max_size * sizeof(OpicData));
	  free(final_buff);
	  
	  hsize_t temp_max_size = max_size;
	  max_size = max_size + 2 * temp_my_data_size;
	  
	  final_buff =  malloc(max_size * sizeof(OpicData));
	  if(final_buff == NULL){
	    printf("ALlocation for final_buff is aboted when increase its size!\n");
	    exit(0);
	  }
	  memcpy(final_buff,temp_final_buff, temp_max_size * sizeof(OpicData));
	  free(temp_final_buff);
	}
      }
      recv_buff[i].e = -1;
      send_buff[i].e = -1;
    }
    
    dest = (dest + 1) % (mpi_size); //dest++;
    if (dest == 0){
      p_pivot = 0;
    }else{	
      p_pivot  = pivots[dest-1].e;
    }

    if(source != 0){
      source =  source - 1;
    }else{
      source =  mpi_size -1;
    }

  }
        
  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  if(mpi_rank == 0 || mpi_rank == (mpi_size - 1)){
    printf("Exchange data ends, my_rank %d, orgi_size %d, new_size %llu, taking [%f]s, each round is [%f]s \n", mpi_rank, my_data_size, m, (t2-t1), ((t2-t1)/mpi_size));
  }

  free(recv_buff);
  free(send_buff);
  
  
  /* float mine =1000; */
  /* float maxe = 0; */
  /* for (i =0 ; i<jj; i++){ */
  /*   if(send_buff[i].e < mine){ */
  /*     mine = send_buff[i].e; */
  /*   } */

  /*   if(send_buff[i].e > maxe){ */
  /*     maxe = send_buff[i].e; */
  /*   } */

  /* } */
  /* printf("my rank %d (m=%d), mine is %8.8f, max %8.8f \n ", mpi_rank, jj, mine, maxe); */

    
  //Sort each process's data
  qsort(final_buff, m, sizeof(OpicData), CompareXYZE);
  
  //Write the final data into the file
  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime();
  
  /* Comment for parallel sorting */
  write_result_file(mpi_rank, mpi_size, final_buff, m);

  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  if(mpi_rank == 0 ){
    printf("Write result file takes [%f]s)\n", (t2-t1));
  }
  free(final_buff);
  return 0;
}


/* 
 * Master does slave's job, and also gather and sort pivots
 */
int master(int mpi_rank, int mpi_size, OpicData *data, int my_data_size, int rest_size){
  int i,j,pass;
    
  OpicData *all_samp, *temp_samp;
  OpicData *pivots;
  double t1, t2; 

  /*All samples*/
  all_samp = malloc(mpi_size * mpi_size * sizeof(OpicData));
  temp_samp = malloc(mpi_size * sizeof(OpicData));
  pivots = malloc((mpi_size - 1)*sizeof(OpicData));
   	
  t1 = MPI_Wtime();
  printf("Phase1 is running .... \n"); 
  /*Sort its own data, and select samples*/
  phase1(0, mpi_size, data, my_data_size, temp_samp);
  t2 = MPI_Wtime();
  printf("Master's phase1 taking [%f]s \n", (t2-t1));

  /*Store its own sample into all_samp */
  for (i=0; i<mpi_size; i++) {
    all_samp[i]=temp_samp[i];
  }
    
  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime();
  printf("Master receives samples ....\n");
  //Gather sample from slaves
  for(i = 1; i < mpi_size; i++){
    //printf("rank 0 -> I am waiting %d \n", i);
    MPI_Recv(temp_samp, mpi_size, OPIC_DATA_TYPE, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &Stat);
    for(j = 0; j < mpi_size; j++){
      all_samp[i*mpi_size + j] = temp_samp[j];
    }
  }
  free(temp_samp);
  t2 = MPI_Wtime();
  printf("Receiving samples taks [%f]s \n", (t2-t1));

  //Sort samples and choose pivots
  qsort(all_samp, (mpi_size * mpi_size), sizeof(OpicData),CompareXYZE);

  pass = mpi_size * mpi_size/(mpi_size);
  int rou;
  rou = mpi_size / 2;
  printf("All sample : ");
  for(i = 0; i < (mpi_size * mpi_size); i++){
    //pivots[i] = all_samp[pass*(i+1)]; 
    //pivots[i] = all_samp[mpi_size*(i+1) + rou -1]; 
    printf("%8.8f, ", all_samp[i].e);
  }
  printf("\n \n \n");

  printf("Pivots : ");
  for(i = 0; i < (mpi_size-1); i++){
    //pivots[i] = all_samp[pass*(i+1)]; 
    pivots[i] = all_samp[mpi_size*(i+1) + rou -1]; 
    printf("%8.8f, ", pivots[i].e);
  }
  printf("\n");
  free(all_samp);

  //To all slaves
  MPI_Bcast(pivots, (mpi_size-1), OPIC_DATA_TYPE, 0, MPI_COMM_WORLD);
	
  printf("Phase2 is running... \n");
  //To sort and write sorted file
  phase2(0, mpi_size, data, my_data_size, pivots, rest_size);
  free(pivots);
    
  return 0;
}

/*Do sort and sample*/
int slave(int mpi_rank, int mpi_size, OpicData *data, int my_data_size, int rest_size){
  OpicData  *pivots;
    
  phase1(mpi_rank, mpi_size, data, my_data_size,NULL);

  //Receive pivots from masters
  pivots = malloc((mpi_size-1)*sizeof(OpicData));
  MPI_Bcast(pivots, (mpi_size-1), OPIC_DATA_TYPE, 0, MPI_COMM_WORLD);
    
  phase2(mpi_rank, mpi_size, data, my_data_size, pivots, rest_size);
  free(pivots);
    
  return 0;
}



int main(int argc, char **argv)
{
  int     mpi_size, mpi_rank;
  hid_t   plist_id, plist2_id;	
  hid_t   file_id, dset_id, x_id, y_id, z_id;
  hid_t   dataspace, memspace, x_space, y_space, z_space;
  int     rank;
  hsize_t dims_out[1];
  herr_t  status;
  hsize_t my_data_size, rest_size;
  hsize_t my_offset;
	
  float    *data, *x_data, *y_data, *z_data;
  OpicData *all_data;
  double    t1, t2, t0, t4;


  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info = MPI_INFO_NULL;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &mpi_size);
  MPI_Comm_rank(comm, &mpi_rank);
  //MPI_Info_create(&info); 

  int c;
  opterr = 0;
  strncpy(filename, "/global/cscratch1/sd/houhun/vpic_allOST_16MB/eparticle_T11430_1.1_filter.h5p", NAME_MAX);
  strncpy(filename_sorted, "/global/cscratch1/sd/houhun/vpic_allOST_16MB/eparticle_T11430_1.1_filter_sort.h5", NAME_MAX);
  strncpy(filename_attribute, "/global/cscratch1/sd/houhun/vpic_allOST_16MB/eparticle_T11430_1.1_filter.attribute", NAME_MAX);
  strncpy(cb_buffer_size,"134217728", NAME_MAX);
  strncpy(cb_nodes, "128", NAME_MAX);

  //char    groupname[NAME_MAX];
  //char    dsetname[NAME_MAX];
  
  while ((c = getopt (argc, argv, "f:g:d:o:a:b:n:")) != -1)
    switch (c)
    {
      case 'f':
	strncpy(filename, optarg, NAME_MAX);
	break;
      case 'g':
        strncpy(groupname, optarg, NAME_MAX);
	break;
      case 'd':
        strncpy(dsetname, optarg, NAME_MAX);
	break;
      case 'o':
        strncpy(filename_sorted, optarg, NAME_MAX);
	break;
      case 'a':
        strncpy(filename_attribute, optarg, NAME_MAX);
	break;
      case 'b':
	strncpy(cb_buffer_size,optarg, NAME_MAX);
	break;
      case 'n':
	strncpy(cb_nodes, optarg, NAME_MAX);
	break;
      default:
	printf("Error option [%s]\n", optarg);
	exit(-1);
    }
  
  /* create a type for OPIC */
  const int    nitems=4;
  int          blocklengths[4] = {1,1,1,1};
  MPI_Datatype types[4] = {MPI_FLOAT, MPI_FLOAT,MPI_FLOAT,MPI_FLOAT};
  MPI_Aint     offsets[4];
    
  offsets[0] = offsetof(OpicData, e);
  offsets[1] = offsetof(OpicData, x);
  offsets[2] = offsetof(OpicData, y);
  offsets[3] = offsetof(OpicData, z);
    
    
  MPI_Type_create_struct(nitems, blocklengths, offsets, types, &OPIC_DATA_TYPE);
  MPI_Type_commit(&OPIC_DATA_TYPE);


  /* create a type for SDSSortTableElem*/
  const int    nitems2=4;
  int          blocklengths2[4] = {1,1,1,1};
  MPI_Datatype types2[4] = {MPI_FLOAT, MPI_FLOAT,MPI_UNSIGNED_LONG_LONG,MPI_UNSIGNED_LONG_LONG};
  MPI_Aint     offsets2[4];
    
  offsets2[0] = offsetof(SDSSortTableElem, c_min);
  offsets2[1] = offsetof(SDSSortTableElem, c_max);
  offsets2[2] = offsetof(SDSSortTableElem, c_start_offset);
  offsets2[3] = offsetof(SDSSortTableElem, c_end_offset);
  
    
  MPI_Type_create_struct(nitems2, blocklengths2, offsets2, types2, &SDS_DATA_TYPE);
  MPI_Type_commit(&SDS_DATA_TYPE);

    
  // MPI_Info_set(info, "cb_buffer_size", "157810672");
  //MPI_Info_set(info, "cb_nodes", "513");

  //MPI_Info_set(info, "cb_buffer_size", cb_buffer_size);// "157810672");
  //MPI_Info_set(info, "cb_nodes", cb_nodes);
			
  // plist_id = H5Pcreate(H5P_FILE_ACCESS);
  //H5Pset_fapl_mpio(plist_id, comm, info);
	
  MPI_Barrier(MPI_COMM_WORLD);
  t0 =  MPI_Wtime();


  hid_t acc_tpl;
  acc_tpl = H5Pcreate(H5P_FILE_ACCESS);

#ifdef SDS_HDF5_VOL
  //Register SDS external native plugin
  hid_t under_dapl;
  hid_t vol_id;
  under_dapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(under_dapl, comm, info);
  vol_id  = H5VLregister(&H5VL_sds_external_native_g);
  external_native_plugin_id = H5VLget_plugin_id("native");
  assert(external_native_plugin_id > 0);
  H5Pset_vol(acc_tpl, vol_id, &under_dapl);
  //End of Register SDS external native plugin
#else
  plist_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(acc_tpl, comm, info);
#endif
  
  //Open HDF5 file
  file_id = H5Fopen(filename, H5F_ACC_RDONLY, acc_tpl);
  dset_id = H5Dopen(file_id,"/Step#0/Energy",H5P_DEFAULT);
  x_id    = H5Dopen(file_id,"/Step#0/x",H5P_DEFAULT);
  y_id    = H5Dopen(file_id,"/Step#0/y",H5P_DEFAULT);
  z_id    = H5Dopen(file_id,"/Step#0/z",H5P_DEFAULT);
  t1 = MPI_Wtime();    

  if(mpi_rank == 0){
    printf("Openning file [%s] and dataset, takes [%f]s\n      cb_buffer_size [%s], cb_nodes [%s] \n",filename, (t1-t0), cb_buffer_size, cb_nodes);
  }

  //Obtain dimension of the array
  dataspace = H5Dget_space(dset_id);
  rank      = H5Sget_simple_extent_ndims(dataspace);
  status    = H5Sget_simple_extent_dims(dataspace, dims_out, NULL);
  x_space   = H5Dget_space(dset_id);
  y_space   = H5Dget_space(dset_id);
  z_space   = H5Dget_space(dset_id);
	

  //Compute the size each process is responsilbe for
  rest_size = dims_out[0] % mpi_size;
  if (mpi_rank ==  (mpi_size - 1)){
    my_data_size = dims_out[0]/mpi_size + rest_size;
  }else{
    my_data_size = dims_out[0]/mpi_size;
  }
  my_offset = mpi_rank * (dims_out[0]/mpi_size);
  if(mpi_rank == 0 || mpi_rank == (mpi_size -1)){
    printf("My rank: %d, file size: %lu, my_data_size: %lu, my_offset: %lu \n ", mpi_rank, (unsigned long)dims_out[0],(unsigned long)my_data_size, (unsigned long)my_offset);
  }

  //Create the memory space & hyperslab for each process
  memspace =  H5Screate_simple(rank, &my_data_size, NULL);
  H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, &my_offset, NULL, &my_data_size, NULL);
  H5Sselect_hyperslab(x_space,   H5S_SELECT_SET, &my_offset, NULL, &my_data_size, NULL);
  H5Sselect_hyperslab(y_space,   H5S_SELECT_SET, &my_offset, NULL, &my_data_size, NULL);
  H5Sselect_hyperslab(z_space,   H5S_SELECT_SET, &my_offset, NULL, &my_data_size, NULL);
	

  plist2_id = H5Pcreate(H5P_DATASET_XFER);
  H5Pset_dxpl_mpio(plist2_id, H5FD_MPIO_COLLECTIVE);

  //Read the data belonging to each process into memory      
  data   = (float *)malloc(sizeof(float) *my_data_size);
  x_data = (float *)malloc(sizeof(float) *my_data_size);
  y_data = (float *)malloc(sizeof(float) *my_data_size);
  z_data = (float *)malloc(sizeof(float) *my_data_size);

  MPI_Barrier(MPI_COMM_WORLD);
  t1 =  MPI_Wtime();

  H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, dataspace,plist2_id, data);
  H5Dread(x_id,    H5T_NATIVE_FLOAT, memspace, x_space,plist2_id, x_data);
  H5Dread(y_id,    H5T_NATIVE_FLOAT, memspace, y_space,plist2_id, y_data);
  H5Dread(z_id,    H5T_NATIVE_FLOAT, memspace, z_space,plist2_id, z_data);

  MPI_Barrier(MPI_COMM_WORLD);
  t2 = MPI_Wtime();
  if(mpi_rank == 0){
    printf("Reading data takes [%f]s \n", (t2-t1));
  }	    
	
    
  all_data =  malloc(sizeof(OpicData)*my_data_size);
  CombineXYZE(x_data, y_data, z_data, data, all_data, my_data_size);
  free(data);
  free(x_data);
  free(y_data);
  free(z_data);
	
  //master:  also do slave's job. In addition, it is responsible for samples and pivots
  //slave:   (1) sorts. (2) samples (3) sends sample to master
  //         (4) receives pivots    (5) sends/receives data to/from other processes based on pivots
  //         (6) sorts data again   (7) writes data to its location
    
  if (mpi_rank==0){
    //printf("Start master ! \n");
    master(mpi_rank, mpi_size,  all_data, my_data_size, rest_size);
  }else{
    slave(mpi_rank,  mpi_size, all_data, my_data_size, rest_size);
  }
     
  //H5Pclose(plist_id);
  H5Pclose(plist2_id);	
  H5Dclose(dset_id);
  H5Dclose(x_id);
  H5Dclose(y_id);
  H5Dclose(z_id);
	
  H5Sclose(memspace);
  H5Sclose(dataspace);
  H5Sclose(x_space);
  H5Sclose(y_space);
  H5Sclose(z_space);
  H5Fclose(file_id);

  // H5Pclose(under_fapl);

  //Unregistered the plugin-id
  H5Pclose(acc_tpl);
#ifdef SDS_HDF5_VOL
  H5Pclose(under_dapl);
  H5VLunregister(vol_id);
#endif
  //End of Unregistered the plugin-id

  MPI_Barrier(MPI_COMM_WORLD);		
  t4 = MPI_Wtime();
  if(mpi_rank == 0)
    printf("Overall time is [%f]s \n", (t4-t0));

  //MPI_Info_free(&info);
  MPI_Finalize();
}
