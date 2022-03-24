
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <ctype.h>

#include <blosc.h>

#include "mpi.h"
#include "hdf5.h"
#include "pdc.h"

#undef NDEBUG
#include <assert.h>

#define BOX_AXIS_DIM (8)

#define FACC_DEFAULT 0x0 /* default */
#define FACC_MPIO    0x1 /* MPIO */
#define FACC_SPLIT   0x2 /* Split File */

static hid_t create_faccess_plist(MPI_Comm comm, MPI_Info info, int l_facc_type)
{
  hid_t  ret_pl = -1;
  herr_t ret;

  ret_pl = H5Pcreate(H5P_FILE_ACCESS);
  if (ret_pl < 1) {
    fprintf(stderr, "H5Pcreate(1) failed\n");
  }

  if (l_facc_type == FACC_DEFAULT)
    return (ret_pl);

  if (l_facc_type == FACC_MPIO) {
    /* set Parallel access with communicator */
    ret = H5Pset_fapl_mpio(ret_pl, comm, info);
    if (ret < 0) {
      fprintf(stderr, "H5Pset_fapl_mpio failed\n");
    }
    ret = H5Pset_all_coll_metadata_ops(ret_pl, true);
    if (ret < 0) {
      fprintf(stderr, "H5Pset_all_coll_metadata_ops failed\n");
    }
    ret = H5Pset_coll_metadata_write(ret_pl, true);
    if (ret < 0) {
      fprintf(stderr, "H5Pset_coll_metadata_write failed\n");
    }
    return (ret_pl);
  }

  if (l_facc_type == (FACC_MPIO | FACC_SPLIT)) {
    hid_t mpio_pl;

    mpio_pl = H5Pcreate(H5P_FILE_ACCESS);
    if (mpio_pl < 0) {
      fprintf(stderr, "H5Pcreate(2) failed\n");
    }
    /* set Parallel access with communicator */
    ret = H5Pset_fapl_mpio(mpio_pl, comm, info);
    if (ret < 0) {
      fprintf(stderr, "H5Pset_fapl_mpio failed\n");
    }
    /* setup file access template */
    ret_pl = H5Pcreate(H5P_FILE_ACCESS);
    if (ret_pl < 0) {
      fprintf(stderr, "H5Pcreate(3) failed\n");
    }
    /* set Parallel access with communicator */
    ret = H5Pset_fapl_split(ret_pl, ".meta", mpio_pl, ".raw", mpio_pl);
    if (ret < 0) {
      fprintf(stderr, "H5Pset_fapl_split failed\n");
    }
    H5Pclose(mpio_pl);
    return (ret_pl);
  }

  return -1;
}

void get_offsets_array(hid_t grp_id, void **buf, size_t *len)
{
  hid_t dset_id = H5Dopen(grp_id, "data:offsets=0", H5P_DEFAULT);
  hid_t dspace = H5Dget_space(dset_id);
  const int ndims = H5Sget_simple_extent_ndims(dspace);
  assert(ndims == 1);
  // printf("%d\n", ndims);
  hsize_t dims[ndims];
  H5Sget_simple_extent_dims(dspace, dims, NULL);
  // printf("%lld\n", dims[0]);

  *len = dims[0];
  *buf = malloc(*len*sizeof(int64_t));
  hid_t dset_type = H5Dget_type(dset_id);
  assert(dset_type != H5I_INVALID_HID);
  H5Dread(dset_id, dset_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, *buf);
  H5Dclose(dset_id);
}

void pdc_write_offsets_array(pdcid_t pdc_id, pdcid_t cont_id, int64_t *offsets_array, size_t offsets_array_len)
{
  // create an object property
  pdcid_t obj_prop_offsets = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

  uint64_t offsets_array_dims[1];
  offsets_array_dims[0] = offsets_array_len;
  PDCprop_set_obj_dims(obj_prop_offsets, 1, offsets_array_dims);
  PDCprop_set_obj_type(obj_prop_offsets, PDC_INT64);
  PDCprop_set_obj_time_step(obj_prop_offsets, 0);
  PDCprop_set_obj_user_id(obj_prop_offsets, getuid());
  PDCprop_set_obj_app_name(obj_prop_offsets, "Nyx");
  PDCprop_set_obj_tags(obj_prop_offsets, "tag0=1");

  pdcid_t obj_offsets = PDCobj_create_mpi(cont_id, "obj-var-offsets", obj_prop_offsets, 0, MPI_COMM_WORLD);
  if (obj_offsets == 0) {
    printf("Error getting an object id of %s from server, exit...\n", "obj-var-offsets");
    exit(EXIT_FAILURE);
  }

  uint64_t offset = 0;
  uint64_t region_size = offsets_array_len;
  pdcid_t region_offsets = PDCregion_create(1, &offset, &region_size);

  uint64_t offset_remote = 0;
  pdcid_t regions_offsets_remote = PDCregion_create(1, &offset_remote, &region_size);

  pdcid_t transfer_request_offsets = PDCregion_transfer_create(&offsets_array[0], PDC_WRITE, obj_offsets, region_offsets, regions_offsets_remote);
  assert(transfer_request_offsets != 0);

  // init arrray here
  perr_t ret;
  ret = PDCregion_transfer_start(transfer_request_offsets);
  assert(ret == SUCCEED);

  ret = PDCregion_transfer_wait(transfer_request_offsets);
  assert(ret == SUCCEED);

  ret = PDCregion_transfer_close(transfer_request_offsets);
  assert(ret == SUCCEED);

  if (PDCregion_close(region_offsets) < 0) {
    printf("fail to close region region_offsets\n");
    exit(EXIT_FAILURE);
  }

  if (PDCregion_close(regions_offsets_remote) < 0) {
    printf("fail to close region regions_offsets_remote\n");
    exit(EXIT_FAILURE);
  }


  if (PDCobj_close(obj_offsets) < 0) {
    printf("fail to close obj_offsets\n");
    exit(EXIT_FAILURE);
  }

  if (PDCprop_close(obj_prop_offsets) < 0) {
    printf("Fail to close obj property obj_prop_offsets\n");
    exit(EXIT_FAILURE);
  }
}

void print_3d_box(double *b)
{
  for(int k = 0; k < BOX_AXIS_DIM; k++)
  {
    fprintf(stderr, "~~~ k = %d ~~~\n", k);
    for(int i = 0; i < BOX_AXIS_DIM; i++)
    {
      for(int j = 0; j < BOX_AXIS_DIM; j++)
      {
        // 2d index
        // printf("%lf ", b[i*BOX_AXIS_DIM + j]);

        // 3d index
        // fprintf(stderr, "%.2lf ", b[k*BOX_AXIS_DIM*BOX_AXIS_DIM + (i*BOX_AXIS_DIM + j)]);
        fprintf(stderr, "%lf ", b[k*BOX_AXIS_DIM*BOX_AXIS_DIM + (i*BOX_AXIS_DIM + j)]);
      }
      fprintf(stderr, "\n");
    }
  }
}

void pdc_write_3d_box(pdcid_t pdc_id, pdcid_t cont_id, double **component_patch_array, int n_patches)
{
  pdcid_t obj_prop[13];
  obj_prop[0] = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
  uint64_t  dims[1];
  dims[0] = BOX_AXIS_DIM * BOX_AXIS_DIM * BOX_AXIS_DIM * n_patches;

  PDCprop_set_obj_dims(obj_prop[0], 1, dims);
  PDCprop_set_obj_type(obj_prop[0], PDC_DOUBLE);
  PDCprop_set_obj_time_step(obj_prop[0], 0);
  PDCprop_set_obj_user_id(obj_prop[0], getuid());
  PDCprop_set_obj_app_name(obj_prop[0], "Nyx");
  PDCprop_set_obj_tags(obj_prop[0], "tag0=1");
  for(int i = 1; i < 13; i++)
  {
    obj_prop[i] = PDCprop_obj_dup(obj_prop[0]);
    PDCprop_set_obj_type(obj_prop[i], PDC_DOUBLE);
  }

  pdcid_t obj[13];
  for(int i = 0; i < 13; i++)
  {
    char obj_name[32];
    sprintf(obj_name, "obj-component%d", i);
    obj[i] = PDCobj_create_mpi(cont_id, obj_name, obj_prop[i], 0, MPI_COMM_WORLD);
  }

  pdcid_t region[13];
  pdcid_t region_remote[13];
  int ndim = 1;
  uint64_t *offset = (uint64_t *)calloc(ndim, sizeof(uint64_t));
  uint64_t *offset_remote = (uint64_t *)calloc(ndim, sizeof(uint64_t));
  uint64_t *mysize = (uint64_t *)calloc(ndim, sizeof(uint64_t));

  mysize[0] = BOX_AXIS_DIM * BOX_AXIS_DIM * BOX_AXIS_DIM * n_patches;

  for(int i = 0; i < 13; i++)
  {
    region[i] = PDCregion_create(ndim, offset, mysize);
    region_remote[i] = PDCregion_create(ndim, offset_remote, mysize);
  }

  pdcid_t xfer_request[13];
  for(int i = 0; i < 13; i++)
  {
    xfer_request[i] = PDCregion_transfer_create(&component_patch_array[i][0],
      PDC_WRITE, obj[i], region[i], region_remote[i]);
    assert(xfer_request[i] != 0);
  }

  // printf("printing out component_patch_array: \n");
  // for(int i = 0; i < n_patches; i++)
  // {
  //   fprintf(stderr, "%4d. [%ld, %ld)\n", i, offsets_array[i], offsets_array[i+1]);
  //   for(int component_id = 0; component_id < 13; component_id++)
  //   {
  //     fprintf(stderr, "component %d\n", component_id);
  //     // fprintf(stderr, "patch_offset %d\n", i*ngrid);
  //     uint64_t patch_offset = i * ngrid;

  //     print_3d_box(component_patch_array[component_id] + patch_offset);
  //   }
  // }
  // printf("done printing out component_patch_array\n");

  for(int i = 0; i < 13; i++)
  {
    perr_t ret = PDCregion_transfer_start(xfer_request[i]);
    assert(ret == SUCCEED);
  }

  for(int i = 0; i < 13; i++)
  {
    perr_t ret = PDCregion_transfer_wait(xfer_request[i]);
    assert(ret == SUCCEED);
  }

  for(int i = 0; i < 13; i++)
  {
    perr_t ret = PDCregion_transfer_close(xfer_request[i]);
    assert(ret == SUCCEED);
  }

  for(int i = 0; i < 13; i++)
  {
    perr_t ret;

    ret = PDCregion_close(region[i]);
    assert(ret == SUCCEED);

    ret = PDCregion_close(region_remote[i]);
    assert(ret == SUCCEED);

    ret = PDCobj_close(obj[i]);
    assert(ret == SUCCEED);

    ret = PDCprop_close(obj_prop[i]);
    assert(ret == SUCCEED);
  }
}

int main(int argc, char **argv)
{
  int mpi_rank = 0;
  int mpi_size = 1;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  printf("rank %d of %d\n", mpi_rank, mpi_size);

  if (argc < 2)
  {
    if (mpi_rank == 0) printf("Usage: %s <plotfile>\n", argv[0]);
    return EXIT_FAILURE;
  }

  H5open();

  hid_t acc_tpl = create_faccess_plist(MPI_COMM_WORLD, MPI_INFO_NULL, FACC_MPIO);
  if (acc_tpl >= 0)
  {
    if (mpi_rank == 0) printf("using collective I/O\n");
    H5Pset_fapl_mpio(acc_tpl, MPI_COMM_WORLD, MPI_INFO_NULL);
  }

  hid_t file_id = H5Fopen(argv[1], H5F_ACC_RDONLY, acc_tpl);
  if (file_id < 0)
  {
    fprintf(stderr, "Error opening filename: %s\n", argv[1]);
    return EXIT_FAILURE;
  }
  H5Pclose(acc_tpl);

  hid_t grp_id = H5Gopen(file_id, "/level_0", H5P_DEFAULT);

  int64_t *offsets_array;
  size_t offsets_array_len = 0;
  get_offsets_array(grp_id, (void **)&offsets_array, &offsets_array_len);

  pdcid_t pdc_id = PDCinit("pdc");

  // create a container property
  pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
  if (cont_prop <= 0)
  {
    printf("Fail to create container property @ line  %d!\n", __LINE__);
    return EXIT_FAILURE;
  }

  // create a container
  pdcid_t cont_id = PDCcont_create_col("c1", cont_prop);
  if (cont_id <= 0) {
    printf("Fail to create container @ line  %d!\n", __LINE__);
    return EXIT_FAILURE;
  }

  pdc_write_offsets_array(pdc_id, cont_id, offsets_array, offsets_array_len);

  int32_t ngrid = -1;
  {
    hid_t attr = H5Aopen(grp_id, "ngrid", H5P_DEFAULT);
    herr_t ret  = H5Aread(attr, H5T_NATIVE_INT, &ngrid);
    // printf("The value of the attribute \"ngrid\" is %d \n", ngrid);
    ret = H5Aclose(attr);
  }

  double *component_patch_array[13];
  int n_patches = -1;

  {
    hid_t dset_id = H5Dopen(grp_id, "data:datatype=0", H5P_DEFAULT);
    hid_t dspace = H5Dget_space(dset_id);
    const int ndims = H5Sget_simple_extent_ndims(dspace);
    assert(ndims == 1);
    // printf("%d\n", ndims);
    hsize_t dims[ndims];
    H5Sget_simple_extent_dims(dspace, dims, NULL);
    // printf("%lld\n", dims[0]);

    double *full_data_array = (double*)malloc(sizeof(double)*dims[0]);
    hid_t dset_type = H5Dget_type(dset_id);
    assert(dset_type != H5I_INVALID_HID);
    H5Dread(dset_id, dset_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, full_data_array);

    for(int i = 0; i < 13; i++)
    {
      component_patch_array[i] = (double*)malloc( (dims[0] / 13) * sizeof(double));
    }

    int patch_start = 0;
    int patch_end = offsets_array_len - 1;
    int patch_subset = patch_end - patch_start;
    n_patches = patch_subset;
    if(mpi_rank == 0) printf("before patch_subset: %d\n", patch_subset);

    //////////////////////////////////////////////////////////////
    // MPI PARALLEL
    if( patch_subset % mpi_size != 0 )
    {
      printf("WARNING: Please launch # mpi ranks that is divisible by %ld\n", offsets_array_len-1);
    }

    patch_subset = patch_subset / mpi_size;

    if (mpi_rank == 0) printf("after patch_subset: %d\n", patch_subset);

    patch_start = patch_subset * mpi_rank;
    patch_end = patch_subset * (mpi_rank + 1);
    // printf("[%d, %d)\n", patch_start, patch_end);

    // NOTE: I think patches are amrex boxes, so in an ideal case all
    // patches should be uniformly serviced by the available MPI Ranks
    //
    // parallel: iterate box offsets (patches)
    for(int i = patch_start; i < patch_end; i++)
    {
      double *box_data = (double*)full_data_array + offsets_array[i];
      size_t box_data_size = offsets_array[i+1] - offsets_array[i];
      // fprintf(stderr, "%4d. [%ld, %ld)\n", i, offsets_array[i], offsets_array[i+1]);
      // printf("box_data_size: %ld\n", box_data_size);
      // iterate components
      for(int component_id = 0; component_id < 13; component_id++)
      {
        // printf("%s: \n", component_names[component_id].c_str());
        // fprintf(stderr, "component %d\n", component_id);
        double *patch_component_data = box_data + component_id*ngrid;

        // printf("offsets_array[i] = %ld\n", offsets_array[i]);
        uint64_t patch_offset = i * ngrid;
        memcpy(component_patch_array[component_id] + patch_offset, patch_component_data, BOX_AXIS_DIM*BOX_AXIS_DIM*BOX_AXIS_DIM*sizeof(double));

        // print_3d_box(patch_component_data);
      }
    }

    //////////////////////////////////////////////////////////////
    // // serial: iterate box offsets (patches)
    // for(int i = 0; i < offsets_array_len-1; i++)
    // {
    //   double *box_data = (double*)full_data_array + offsets_array[i];
    //   size_t box_data_size = offsets_array[i+1] - offsets_array[i];
    //   printf("%4d. [%ld, %ld)\n", i, offsets_array[i], offsets_array[i+1]);
    //   printf("box_data_size: %ld\n", box_data_size);
    //   // iterate components
    //   for(int component_id = 0; component_id < 13; component_id++)
    //   {
    //     // printf("%s: \n", component_names[component_id].c_str());
    //     printf("component %d\n", component_id);
    //     print_3d_box(box_data + component_id*ngrid);
    //     break;
    //   }
    //   break;
    // }
    //////////////////////////////////////////////////////////////

    printf("freeing full_data_array\n");
    free(full_data_array);
    printf("done freeing full_data_array\n");
    H5Dclose(dset_id);
  }

  H5Gclose(grp_id);
  H5Fclose(file_id);
  H5close();
  
  free(offsets_array);

  pdc_write_3d_box(pdc_id, cont_id, component_patch_array, n_patches);

  for(int component_id = 0; component_id < 13; component_id++)
  {
    free(component_patch_array[component_id]);
  }

  // close a container
  if (PDCcont_close(cont_id) < 0) {
    printf("fail to close container c1\n");
    return EXIT_FAILURE;
  }
  // close a container property
  if (PDCprop_close(cont_prop) < 0) {
    printf("Fail to close property @ line %d\n", __LINE__);
    return EXIT_FAILURE;
  }
  if (PDCclose(pdc_id) < 0) {
    printf("fail to close PDC\n");
    return EXIT_FAILURE;
  }

  MPI_Finalize();
  return EXIT_SUCCESS;
}
