#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

/*
inline double uniform_random_number()
{
        return (((double)rand())/((double)(RAND_MAX)));
}
*/
int main(int argc, char **argv)
{
    int rank = 0, size = 1;
    uint64_t size_MB;
#ifdef ENABLE_MPI	
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    char *obj_name = argv[1];
    size_MB = atoi(argv[2]);
    size_MB *= 1048576;
    struct PDC_prop p;
    
    pdcid_t pdc = PDC_init(p);

    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    pdcid_t cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

	long numparticles=8388608;

	float *xx, *yy, *zz, *vx, *vy, *vz, *phi;
  	int64_t* pid;
  	uint16_t* mask;

	xx=(float*)malloc(numparticles*sizeof(float));
        yy=(float*)malloc(numparticles*sizeof(float));
        zz=(float*)malloc(numparticles*sizeof(float));

        vx=(float*)malloc(numparticles*sizeof(float));
        vy=(float*)malloc(numparticles*sizeof(float));
        vz=(float*)malloc(numparticles*sizeof(float));

        phi=(float*)malloc(numparticles*sizeof(float));
        pid=(int64_t*)malloc(numparticles*sizeof(int64_t));
	mask=(uint16_t*)malloc(numparticles*sizeof(uint16_t));

	inline double uniform_random_number()
	{
        return (((double)rand())/((double)(RAND_MAX)));
	}
		
	for (int i=0; i<numparticles; i++)
        {
                xx[i] = i;
                yy[i] = i;
                zz[i] = i;
                vx[i] = i;
                vy[i] = i;
                vz[i] = i;
                phi[i] = i;
                pid[i] = i;
		mask[i] = rank;
        }
    
    const int my_data_size = 8388608;
    uint64_t dims[1] = {my_data_size};  // {8388608};
    PDCprop_set_obj_dims(obj_prop, 1, dims, pdc);

    pdcid_t test_obj = -1;

   
    PDCprop_set_obj_user_id( obj_prop, getuid(),pdc);
    PDCprop_set_obj_time_step( obj_prop, 0, pdc);
    PDCprop_set_obj_app_name(obj_prop, "VPICIO", pdc);
    PDCprop_set_obj_tags(obj_prop, "tag0=1", pdc);

    struct PDC_region_info region1;
    struct PDC_region_info region2;
    struct PDC_region_info region3;
    struct PDC_region_info region4;
    struct PDC_region_info region5;
    struct PDC_region_info region6;
    struct PDC_region_info region7;
    struct PDC_region_info region8;
    struct PDC_region_info region9;
#ifdef ENABLE_MPI 
    MPI_Barrier(MPI_COMM_WORLD);
#endif

//perr_t PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf, pdcid_t pdc_id);	
//	PDCprop_set_obj_buf(obj_prop, x, pdc );
       
       
	for(int i=1; i<=9; i++){
         sprintf(obj_name, "obj-var-%d", i );

          test_obj = PDCobj_create(pdc, cont, obj_name, obj_prop);
            if (test_obj < 0) {
                printf("Error getting an object id of %s from server, exit...\n", obj_name);
                exit(-1);
		}        
	}
    
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

		// Query the created object
    pdc_metadata_t *metadata1, *metadata2, *metadata3, *metadata4, *metadata5, *metadata6, *metadata7, *metadata8, *metadata9;
    PDC_Client_query_metadata_name_timestep("obj-var-1", 0, &metadata1);
    PDC_Client_query_metadata_name_timestep("obj-var-2", 0, &metadata2);
    PDC_Client_query_metadata_name_timestep("obj-var-3", 0, &metadata3);
    PDC_Client_query_metadata_name_timestep("obj-var-4", 0, &metadata4);
    PDC_Client_query_metadata_name_timestep("obj-var-5", 0, &metadata5);
    PDC_Client_query_metadata_name_timestep("obj-var-6", 0, &metadata6);
    PDC_Client_query_metadata_name_timestep("obj-var-7", 0, &metadata7);
    PDC_Client_query_metadata_name_timestep("obj-var-8", 0, &metadata8);
    PDC_Client_query_metadata_name_timestep("obj-var-9", 0, &metadata9);
    int ndim =1;
    region1.ndim = ndim;
    region1.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region1.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region1.offset[0]= rank * my_data_size;
    region1.size[0]= my_data_size;

    region2.ndim = ndim;
    region2.offset =(uint64_t*)malloc(ndim*sizeof(uint64_t));
    region2.size = (uint64_t*)malloc(ndim*sizeof(uint64_t));
    region2.offset[0]= rank * my_data_size;
    region2.size[0]= my_data_size;

    region3.ndim = ndim;
    region3.offset =(uint64_t*)malloc(ndim*sizeof(uint64_t));
    region3.size = (uint64_t*)malloc(ndim*sizeof(uint64_t));
    region3.offset[0]= rank * my_data_size;
    region3.size[0]= my_data_size;

    region4.ndim = ndim;
    region4.offset =(uint64_t*)malloc(ndim*sizeof(uint64_t));
    region4.size = (uint64_t*)malloc(ndim*sizeof(uint64_t));
    region4.offset[0]= rank * my_data_size;
    region4.size[0]= my_data_size;

    region5.ndim = ndim;
    region5.offset =(uint64_t*)malloc(ndim*sizeof(uint64_t));
    region5.size = (uint64_t*)malloc(ndim*sizeof(uint64_t));
    region5.offset[0]= rank * my_data_size;
    region5.size[0]= my_data_size;

    region6.ndim = ndim;
    region6.offset =(uint64_t*)malloc(ndim*sizeof(uint64_t));
    region6.size = (uint64_t*)malloc(ndim*sizeof(uint64_t));
    region6.offset[0]= rank * my_data_size;
    region6.size[0]= my_data_size;

    region7.ndim = ndim;
    region7.offset = (uint64_t*)malloc(ndim*sizeof(uint64_t));
    region7.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region7.offset[0]= rank *  my_data_size;
    region7.size[0]= my_data_size;

    region8.ndim = ndim;
    region8.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region8.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region8.offset[0]= rank *  my_data_size;
    region8.size[0]= my_data_size;

    region9.ndim = ndim;
    region9.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region9.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region9.offset[0]= rank *  my_data_size;
    region9.size[0]= my_data_size;


    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);
    
   PDC_Client_write(metadata1, &region1, xx);
   PDC_Client_write(metadata2, &region2, yy);
   PDC_Client_write(metadata3, &region3, zz);
   PDC_Client_write(metadata4, &region4, vx);
   PDC_Client_write(metadata5, &region5, vy);
   PDC_Client_write(metadata6, &region6, vz);
   PDC_Client_write(metadata7, &region7, phi);
   PDC_Client_write(metadata8, &region8, pid);
   PDC_Client_write(metadata9, &region9, mask);
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
     gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    if (rank == 0) {
        printf("Time to write data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }
done:
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);

    if(PDCprop_close(cont_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
