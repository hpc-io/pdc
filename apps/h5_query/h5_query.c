#include <stdio.h>
#include <stdlib.h>
#include <float.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>

#include <hdf5.h>

/* #define ENABLE_MPI 1 */
#define NVAR 7

struct timeval start_time[3];
float          elapse[3];

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

typedef struct query_constraint_t {
    float energy_lo;
    float energy_hi;
    float x_lo;
    float x_hi;
    float y_lo;
    float y_hi;
    float z_lo;
    float z_hi;
    float ux_lo;
    float ux_hi;
    float uy_lo;
    float uy_hi;
    float uz_lo;
    float uz_hi;
} query_constraint_t;

query_constraint_t query_constraints[] = {
    {
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
    },
    // Energy > 1.7
    {
        1.7,     FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
    },
    // Energy < 1.3&&308 < X < 309&&149 < Y < 150
    {
        -FLT_MAX, 1.3, 
        308.0,   309.0, 
        149.0,   150.0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
    },
    // Energy > 1.3&&300 < X < 310&&140 < Y < 150
    {
        1.3,     FLT_MAX, 
        300.0,   310.0, 
        140.0,   150.0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
    }
};

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
    printf("Usage: %s /path/to/file [query_id]\n", name);
}


void do_query(int qid, vpic_data_t *vpic_data)
{
    size_t i, nhits = 0;
    int    is_satisfy;
    query_constraint_t *constraint = &query_constraints[qid];

    for (i = 0; i < vpic_data->nparticle; i++) {
        is_satisfy = 1;
        if (vpic_data->energy[i] < constraint->energy_lo || vpic_data->energy[i] > constraint->energy_hi ||
            vpic_data->x[i] < constraint->x_lo || vpic_data->x[i] > constraint->x_hi     ||
            vpic_data->y[i] < constraint->y_lo || vpic_data->y[i] > constraint->y_hi     ||
            vpic_data->z[i] < constraint->z_lo || vpic_data->z[i] > constraint->z_hi     ||
            vpic_data->ux[i] < constraint->ux_lo || vpic_data->ux[i] > constraint->ux_hi ||
            vpic_data->uy[i] < constraint->uy_lo || vpic_data->uy[i] > constraint->uy_hi ||
            vpic_data->uz[i] < constraint->uz_lo || vpic_data->uz[i] > constraint->uz_hi 
           ) {
            is_satisfy = 0;
        }
        else
            nhits++;
    }

    printf("Query %d has %lu hits\n", qid, nhits);
}

int main (int argc, char* argv[])
{
    int         my_rank = 0, num_procs = 1, i, ndim, query_id = 0;
    char        *file_name, *group_name;
    char        *dset_names[NVAR] = {"Energy", "x", "y", "z", "Ux", "Uy", "Uz"};
    hid_t       file_id, group_id, dset_ids[NVAR], filespace, memspace, fapl;
    vpic_data_t vpic_data;
    hsize_t     dims[1], my_offset, my_size;

    memset(&vpic_data, 0, sizeof(vpic_data_t));

    #ifdef ENABLE_MPI
    MPI_Info    info = MPI_INFO_NULL;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank (MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size (MPI_COMM_WORLD, &num_procs);
    #endif


    if(argc < 2) {
        print_usage(argv[0]);
        return 0;
    }

    file_name  = argv[1];
    group_name = "Step#0";
    if (argc == 3) 
        query_id = atoi(argv[2]);

    if (my_rank == 0) {
        printf ("Query id = %d, reading from file [%s], \n", query_id, file_name);
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
        if(group_id < 0) {
            printf("Error opening dataset [%s]!\n", dset_names[i]);
            goto error;
        }
    }

    filespace = H5Dget_space(dset_ids[0]);
    ndim      = H5Sget_simple_extent_ndims(filespace);
    H5Sget_simple_extent_dims(filespace, dims, NULL);

    vpic_data.nparticle = dims[0];

    vpic_data.energy = (float*)malloc(vpic_data.nparticle * sizeof(float));
    vpic_data.x      = (float*)malloc(vpic_data.nparticle * sizeof(float));
    vpic_data.y      = (float*)malloc(vpic_data.nparticle * sizeof(float));
    vpic_data.z      = (float*)malloc(vpic_data.nparticle * sizeof(float));
    vpic_data.ux     = (float*)malloc(vpic_data.nparticle * sizeof(float));
    vpic_data.uy     = (float*)malloc(vpic_data.nparticle * sizeof(float));
    vpic_data.uz     = (float*)malloc(vpic_data.nparticle * sizeof(float));


    my_offset = my_rank * (vpic_data.nparticle / num_procs);
    my_size   = vpic_data.nparticle / num_procs;
    if (my_rank == num_procs - 1) 
        my_size += (vpic_data.nparticle % my_size);
    

    memspace =  H5Screate_simple(1, (hsize_t *) &my_size, NULL);
    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &my_offset, NULL, &my_size, NULL);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on(1);

    if (my_rank == 0) 
        printf ("Reading data \n");

    H5Dread(dset_ids[0], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.energy);
    H5Dread(dset_ids[1], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.x);
    H5Dread(dset_ids[2], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.y);
    H5Dread(dset_ids[3], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.z);
    H5Dread(dset_ids[4], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.ux);
    H5Dread(dset_ids[5], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.uy);
    H5Dread(dset_ids[6], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.uz);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(1);


    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on(2);

    if (my_rank == 0) 
        printf ("Processing query\n");
    do_query(query_id, &vpic_data);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(2);


    H5Sclose(memspace);
    H5Sclose(filespace);
    for (i = 0; i < NVAR; i++) 
        H5Dclose(dset_ids[i]);
    H5Gclose(group_id);
    H5Pclose(fapl);
    H5Fclose(file_id);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(0);

    if (my_rank == 0) {
        timer_msg (1, "reading data");
        timer_msg (2, "querying data");
        timer_msg (0, "opening, reading, querying, and closing file");
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

    #ifdef ENABLE_MPI
    MPI_Finalize();
    #endif

    return 0;
}
