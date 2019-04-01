#include <stdio.h>
#include <stdlib.h>
#include <float.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>

#include <hdf5.h>
#include "dtcmp.h"

#define ENABLE_MPI 1
#define NVAR 7

struct timeval start_time[5];
float          elapse[5];

typedef struct vpic_data_t {
    size_t nparticle;
    float  *energy;
    float  *x;
    float  *y;
    float  *z;
    float  *ux;
    float  *uy;
    float  *uz;
} vpic_data_t;

typedef struct vpic_aos_data_t {
    float  energy;
    float  x;
    float  y;
    float  z;
    float  ux;
    float  uy;
    float  uz;
} vpic_aos_data_t;


#define timer_on(id) gettimeofday (&start_time[id], NULL)
#define timer_off(id) 	\
		{	\
		     struct timeval result, now; \
		     gettimeofday (&now, NULL);  \
		     timeval_subtract(&result, &now, &start_time[id]);	\
		     elapse[id] += result.tv_sec+ (double) (result.tv_usec)/1000000.;	\
		}

#define timer_msg(id, msg) \
	printf("%f seconds elapsed in %s\n", (double)(elapse[id]), msg);  \

#define timer_reset(id) elapse[id] = 0

int
timeval_subtract (struct timeval *result, struct timeval *x, struct timeval *y)
{
    /* Perform the carry for the later subtraction by updating y. */
    if (x->tv_usec < y->tv_usec) {
        int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
        y->tv_usec -= 1000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_usec - y->tv_usec > 1000000) {
        int nsec = (y->tv_usec - x->tv_usec) / 1000000;
        y->tv_usec += 1000000 * nsec;
        y->tv_sec -= nsec;
    }

    result->tv_sec = x->tv_sec - y->tv_sec;
    result->tv_usec = x->tv_usec - y->tv_usec;

    return x->tv_sec < y->tv_sec;
}

void print_usage(char *name)
{
    printf("Usage: %s /path/to/file /sorted/file\n", name);
}

int main (int argc, char* argv[])
{
    int         my_rank = 0, num_procs = 1, ndim;
    char        *file_name, *group_name, *out_fname;
    char        *dset_names[NVAR] = {"Energy", "x", "y", "z", "Ux", "Uy", "Uz"};
    hid_t       file_id, group_id, dset_ids[NVAR], filespace, memspace, fapl;
    hid_t       out_file_id, out_group_id, out_dset_ids[NVAR], out_filespace, out_memspace;
    vpic_data_t vpic_data;
    hsize_t     dims[1], my_offset, my_size, i;

    memset(&vpic_data, 0, sizeof(vpic_data_t));

    #ifdef ENABLE_MPI
    MPI_Info    info = MPI_INFO_NULL;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank (MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size (MPI_COMM_WORLD, &num_procs);
    #endif

    DTCMP_Init();

    if(argc < 3) {
        print_usage(argv[0]);
        return 0;
    }

    file_name  = argv[1];
    out_fname  = argv[2];
    group_name = "Step#0";

    if (my_rank == 0) {
        printf ("Reading from file [%s], output file [%s]\n", file_name, out_fname);
    }

    if((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        printf("Error with fapl create!\n");
        goto error;
    }

    #ifdef ENABLE_MPI
    if(H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        printf("Error with H5Pset_fapl_mpio!\n");
        goto error;
    }
    #endif

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on (0);

    file_id = H5Fopen(file_name, H5F_ACC_RDONLY, fapl);
    if(file_id < 0) {
        printf("Error opening file [%s]!\n", file_name);
        goto error;
    }

    group_id = H5Gopen(file_id, group_name, H5P_DEFAULT);
    if(group_id < 0) {
        printf("Error opening group [%s]!\n", group_name);
        goto error;
    }

    for (i = 0; i < NVAR; i++) {
        dset_ids[i] = H5Dopen(group_id, dset_names[i], H5P_DEFAULT);
        if(dset_ids[i] < 0) {
            printf("Error opening dataset [%s]!\n", dset_names[i]);
            goto error;
        }
    }

    filespace = H5Dget_space(dset_ids[0]);
    ndim      = H5Sget_simple_extent_ndims(filespace);
    H5Sget_simple_extent_dims(filespace, dims, NULL);

    my_offset = my_rank * (dims[0] / num_procs);
    my_size   = dims[0] / num_procs;
    if (my_rank == num_procs - 1) 
        my_size += (dims[0] % my_size);
    
    vpic_data.nparticle = my_size;

    vpic_data.energy = (float*)malloc(my_size * sizeof(float));
    vpic_data.x      = (float*)malloc(my_size * sizeof(float));
    vpic_data.y      = (float*)malloc(my_size * sizeof(float));
    vpic_data.z      = (float*)malloc(my_size * sizeof(float));
    vpic_data.ux     = (float*)malloc(my_size * sizeof(float));
    vpic_data.uy     = (float*)malloc(my_size * sizeof(float));
    vpic_data.uz     = (float*)malloc(my_size * sizeof(float));

    memspace =  H5Screate_simple(1, (hsize_t *) &my_size, NULL);
    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &my_offset, NULL, &my_size, NULL);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on(1);

    if (my_rank == 0) {
        printf ("Reading data \n");
        fflush(stdout);
    }

    H5Dread(dset_ids[0], H5T_IEEE_F32LE, memspace, filespace, H5P_DEFAULT, vpic_data.energy);
    H5Dread(dset_ids[1], H5T_IEEE_F32LE, memspace, filespace, H5P_DEFAULT, vpic_data.x);
    H5Dread(dset_ids[2], H5T_IEEE_F32LE, memspace, filespace, H5P_DEFAULT, vpic_data.y);
    H5Dread(dset_ids[3], H5T_IEEE_F32LE, memspace, filespace, H5P_DEFAULT, vpic_data.z);
    H5Dread(dset_ids[4], H5T_IEEE_F32LE, memspace, filespace, H5P_DEFAULT, vpic_data.ux);
    H5Dread(dset_ids[5], H5T_IEEE_F32LE, memspace, filespace, H5P_DEFAULT, vpic_data.uy);
    H5Dread(dset_ids[6], H5T_IEEE_F32LE, memspace, filespace, H5P_DEFAULT, vpic_data.uz);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(1);


    H5Sclose(memspace);
    H5Sclose(filespace);

    for (i = 0; i < NVAR; i++) 
        H5Dclose(dset_ids[i]);
    H5Gclose(group_id);
    H5Fclose(file_id);


    vpic_aos_data_t *aos_data = (vpic_aos_data_t*)malloc(my_size * sizeof(vpic_aos_data_t));
    if (aos_data == NULL) {
        printf("Error with malloc!\n");
    }
    for (i = 0; i < my_size; i++) {
        aos_data[i].energy = vpic_data.energy[i];
        aos_data[i].x      = vpic_data.x[i];
        aos_data[i].y      = vpic_data.y[i];
        aos_data[i].z      = vpic_data.z[i];
        aos_data[i].ux     = vpic_data.ux[i];
        aos_data[i].uy     = vpic_data.uy[i];
        aos_data[i].uz     = vpic_data.uz[i];
    }

    free(vpic_data.energy);
    free(vpic_data.x);     
    free(vpic_data.y);     
    free(vpic_data.z);     
    free(vpic_data.ux);    
    free(vpic_data.uy);    
    free(vpic_data.uz);    


    vpic_aos_data_t *out= (vpic_aos_data_t*)malloc(my_size * sizeof(vpic_aos_data_t));

    MPI_Datatype sort_dtype;
    MPI_Type_contiguous(7, MPI_FLOAT, &sort_dtype);
    MPI_Type_commit(&sort_dtype);

    if (my_rank == 0) {
        printf ("Sorting data \n");
        fflush(stdout);
    }

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on(2);


    DTCMP_Sortv(aos_data, out, my_size, MPI_FLOAT, sort_dtype,
               DTCMP_OP_FLOAT_ASCEND, DTCMP_FLAG_NONE, MPI_COMM_WORLD);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(2);


    free(aos_data);
    MPI_Type_free(&sort_dtype);

    vpic_data.energy = (float*)malloc(my_size * sizeof(float));
    vpic_data.x      = (float*)malloc(my_size * sizeof(float));
    vpic_data.y      = (float*)malloc(my_size * sizeof(float));
    vpic_data.z      = (float*)malloc(my_size * sizeof(float));
    vpic_data.ux     = (float*)malloc(my_size * sizeof(float));
    vpic_data.uy     = (float*)malloc(my_size * sizeof(float));
    vpic_data.uz     = (float*)malloc(my_size * sizeof(float));

    for (i = 0; i < my_size; i++) {
        vpic_data.energy[i] = out[i].energy; 
        vpic_data.x[i]      = out[i].x; 
        vpic_data.y[i]      = out[i].y; 
        vpic_data.z[i]      = out[i].z; 
        vpic_data.ux[i]     = out[i].ux;
        vpic_data.uy[i]     = out[i].uy;
        vpic_data.uz[i]     = out[i].uz;
    }



    if (my_rank == 0) {
        printf ("Writing data \n");
        fflush(stdout);
    }
    // Writing
    out_file_id = H5Fcreate(out_fname, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    if(out_file_id < 0) {
        printf("Error opening file [%s]!\n", out_fname);
        goto error;
    }

    out_group_id = H5Gcreate(out_file_id, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if(out_group_id < 0) {
        printf("Error opening group [%s]!\n", group_name);
        goto error;
    }

    out_filespace = H5Screate_simple(1, dims, NULL);
    for (i = 0; i < NVAR; i++) {
        out_dset_ids[i] = H5Dcreate(out_group_id, dset_names[i], H5T_IEEE_F32LE, out_filespace, 
                                    H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if(out_dset_ids[i] < 0) {
            printf("Error opening dataset [%s]!\n", dset_names[i]);
            goto error;
        }
    }

    out_memspace =  H5Screate_simple(1, (hsize_t *) &my_size, NULL);
    H5Sselect_hyperslab(out_filespace, H5S_SELECT_SET, &my_offset, NULL, &my_size, NULL);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on(3);

    if (my_rank == 0) 
        printf ("Reading data \n");

    H5Dwrite(out_dset_ids[0], H5T_IEEE_F32LE, out_memspace, out_filespace, H5P_DEFAULT, vpic_data.energy);
    H5Dwrite(out_dset_ids[1], H5T_IEEE_F32LE, out_memspace, out_filespace, H5P_DEFAULT, vpic_data.x);
    H5Dwrite(out_dset_ids[2], H5T_IEEE_F32LE, out_memspace, out_filespace, H5P_DEFAULT, vpic_data.y);
    H5Dwrite(out_dset_ids[3], H5T_IEEE_F32LE, out_memspace, out_filespace, H5P_DEFAULT, vpic_data.z);
    H5Dwrite(out_dset_ids[4], H5T_IEEE_F32LE, out_memspace, out_filespace, H5P_DEFAULT, vpic_data.ux);
    H5Dwrite(out_dset_ids[5], H5T_IEEE_F32LE, out_memspace, out_filespace, H5P_DEFAULT, vpic_data.uy);
    H5Dwrite(out_dset_ids[6], H5T_IEEE_F32LE, out_memspace, out_filespace, H5P_DEFAULT, vpic_data.uz);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(3);

    H5Pclose(fapl);
    H5Sclose(out_memspace);
    H5Sclose(out_filespace);

    for (i = 0; i < NVAR; i++) 
        H5Dclose(out_dset_ids[i]);
    H5Gclose(out_group_id);
    H5Fclose(out_file_id);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(0);



    if (my_rank == 0) {
        timer_msg (1, "reading data");
        timer_msg (2, "sorting data");
        timer_msg (3, "writing data");
        timer_msg (0, "total");
        printf ("\n");
    }

    goto done;

error:
    H5Pclose(fapl);
    H5Fclose(file_id);

done:
    if(vpic_data.x) free(vpic_data.x);
    if(vpic_data.y) free(vpic_data.y);
    if(vpic_data.z) free(vpic_data.z);
    if(vpic_data.ux) free(vpic_data.ux);
    if(vpic_data.uy) free(vpic_data.uy);
    if(vpic_data.uz) free(vpic_data.uz);
    if(vpic_data.energy) free(vpic_data.energy);

    H5close();

    DTCMP_Finalize();
    #ifdef ENABLE_MPI
    MPI_Finalize();
    #endif

    return 0;
}
