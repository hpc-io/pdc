#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <unistd.h>
#include <hdf5.h>
#include <H5VLpdc_public.h>

double uniform_random_number()
{   
    return (((double)rand())/((double)(RAND_MAX)));
}

int main(int argc, char *argv[]) {
    hid_t pdc_vol_id, file_id, fapl_id;
    hid_t dset_id1, dset_id2, dset_id3, dset_id4, dset_id5, dset_id6, dset_id7, dset_id8;
    hid_t filespace, memspace;
    herr_t ierr;
    MPI_Comm comm;
    int my_rank, num_procs;
    H5VL_pdc_info_t pdc_vol;

    // Variables and dimensions
//    long numparticles = 8388608;    // 8  meg particles per process
    long numparticles = 4;
    long long total_particles, offset;

    float *x, *y, *z;
    float *px, *py, *pz;
    int *id1, *id2;
    int x_dim = 64;
    int y_dim = 64;
    int z_dim = 64;
    int i;

    MPI_Init(&argc, &argv);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
    MPI_Comm_dup(MPI_COMM_WORLD, &(pdc_vol.comm));
 
    MPI_Comm_rank (comm, &my_rank);
    MPI_Comm_size (comm, &num_procs);
   
    MPI_Allreduce(&numparticles, &total_particles, 1, MPI_LONG_LONG, MPI_SUM, comm);
    MPI_Scan(&numparticles, &offset, 1, MPI_LONG_LONG, MPI_SUM, comm);
    offset -= numparticles;

    if (my_rank == 0)
        printf ("Number of paritcles: %ld \n", numparticles);

    x=(float *)malloc(numparticles*sizeof(float));
    y=(float *)malloc(numparticles*sizeof(float));
    z=(float *)malloc(numparticles*sizeof(float));

    px=(float *)malloc(numparticles*sizeof(float));
    py=(float *)malloc(numparticles*sizeof(float));
    pz=(float *)malloc(numparticles*sizeof(float));

    id1=(int *)malloc(numparticles*sizeof(int));
    id2=(int *)malloc(numparticles*sizeof(int));

    for (i=0; i<numparticles; i++)
    {
        id1[i] = i;
        id2[i] = i*2;
        x[i] = uniform_random_number()*x_dim;
        y[i] = uniform_random_number()*y_dim;
        z[i] = ((float)id1[i]/numparticles)*z_dim;
        px[i] = uniform_random_number()*x_dim;
        py[i] = uniform_random_number()*y_dim;
        pz[i] = ((float)id2[i]/numparticles)*z_dim;
    }

    /* Set up FAPL */
    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        printf("H5Pcreate() error\n");

    /* Initialize VOL */
    pdc_vol_id = H5VLregister_connector_by_name("pdc", H5P_DEFAULT);
//    pdc_vol_id = H5VLregister_connector(&H5VL_pdc_g, H5P_DEFAULT);

    H5Pset_vol(fapl_id, pdc_vol_id, &pdc_vol);

    /* Create file */
    if((file_id = H5Fcreate(argv[1], H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0)
        printf("H5Fcreate() error\n");

    memspace = H5Screate_simple (1, (hsize_t *) &numparticles, NULL);
    filespace = H5Screate_simple (1, (hsize_t *) &total_particles, NULL);

    /* Create dataset */
    dset_id1 = H5Dcreate(file_id, "x", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_id2 = H5Dcreate(file_id, "y", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_id3 = H5Dcreate(file_id, "z", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_id4 = H5Dcreate(file_id, "px", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_id5 = H5Dcreate(file_id, "py", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_id6 = H5Dcreate(file_id, "pz", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_id7 = H5Dcreate(file_id, "id1", H5T_NATIVE_INT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    dset_id8 = H5Dcreate(file_id, "id2", H5T_NATIVE_INT, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    fapl_id = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(fapl_id, H5FD_MPIO_COLLECTIVE);
    H5Sselect_hyperslab (filespace, H5S_SELECT_SET, (hsize_t *) &offset, NULL, (hsize_t *) &numparticles, NULL);

    MPI_Barrier (comm);
    ierr = H5Dwrite(dset_id1, H5T_NATIVE_FLOAT, memspace, filespace, fapl_id, x);
    if(ierr < 0)
        printf("write x failed\n");
    ierr = H5Dwrite(dset_id2, H5T_NATIVE_FLOAT, memspace, filespace, fapl_id, y);
    if(ierr < 0)
        printf("write y failed\n");
    ierr = H5Dwrite(dset_id3, H5T_NATIVE_FLOAT, memspace, filespace, fapl_id, z);
    if(ierr < 0)
        printf("write z failed\n");
    ierr = H5Dwrite(dset_id4, H5T_NATIVE_FLOAT, memspace, filespace, fapl_id, px);
    if(ierr < 0)
        printf("write px failed\n");
    ierr = H5Dwrite(dset_id5, H5T_NATIVE_FLOAT, memspace, filespace, fapl_id, py);
    if(ierr < 0)
        printf("write py failed\n");
    ierr = H5Dwrite(dset_id6, H5T_NATIVE_FLOAT, memspace, filespace, fapl_id, pz);
    if(ierr < 0)
        printf("write pz failed\n");
    ierr = H5Dwrite(dset_id7, H5T_NATIVE_INT, memspace, filespace, fapl_id, id1);
    if(ierr < 0)
        printf("write id1 failed\n");
    ierr = H5Dwrite(dset_id8, H5T_NATIVE_INT, memspace, filespace, fapl_id, id2);
    if(ierr < 0)
        printf("write id2 failed\n");

    /* Close */
    if(H5Dclose(dset_id1) < 0)
        printf("H5Dclose dataset1 error\n");
    if(H5Dclose(dset_id2) < 0)
        printf("H5Dclose dataset2 error\n");
    if(H5Dclose(dset_id3) < 0)
        printf("H5Dclose dataset3 error\n");
    if(H5Dclose(dset_id4) < 0)
        printf("H5Dclose dataset4 error\n");
    if(H5Dclose(dset_id5) < 0)
        printf("H5Dclose dataset5 error\n");
    if(H5Dclose(dset_id6) < 0)
        printf("H5Dclose dataset6 error\n");
    if(H5Dclose(dset_id7) < 0)
        printf("H5Dclose dataset7 error\n");
    if(H5Dclose(dset_id8) < 0)
        printf("H5Dclose dataset8 error\n");

    if(H5Sclose(memspace) < 0)
        printf("H5Sclose memspace error\n");
    if(H5Sclose(filespace) < 0)
        printf("H5Sclose filespace error\n");
    if(H5Fclose(file_id) < 0)
        printf("H5Fclose error\n");
    if(H5Pclose(fapl_id) < 0)
        printf("H5Pclose error\n");

    if(my_rank == 0) {
        printf("Success\n");
    }

    free(x);
    free(y);
    free(z);
    free(px);
    free(py);
    free(pz);
    free(id1);
    free(id2);
    (void)MPI_Finalize();
    return 0;
}
