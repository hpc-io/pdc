#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "hdf5.h"

#define NDSET 2500
#define NAMELEN 128

void print_usage()
{
    printf("Usage:\nsrun -n N ./h5boss_v1_query_read_h5.exe BOSS_file_list query_list n_query\n");
}

int assign_work_to_rank(int rank, int size, int nwork, int *my_count, int *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        printf("assign_work_to_rank(): Error with input!\n");
        return -1;
    }
    if (nwork < size) {
        if (rank < nwork) 
            *my_count = 1;
        else
            *my_count = 0;
        (*my_start) = rank * (*my_count);
    }
    else {
        (*my_count) = nwork / size;
        (*my_start) = rank * (*my_count);

        // Last few ranks may have extra work
        if (rank >= size - nwork % size) {
            (*my_count)++; 
            (*my_start) += (rank - (size - nwork % size));
        }
    }

    return 1;
}

int main(int argc, char *argv[])
{
    int size=1, my_rank=0;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    if (argc != 4) {
        print_usage();
        goto done;
    }

    char *manifest_fname = argv[1];
    char *query_fname    = argv[2];
    int   n_query        = atoi(argv[3]);

    int count, i, j, k, my_count, my_start;
    char **all_h5_filenames, *all_h5_filenames_1d;
    all_h5_filenames    = (char**)calloc(NDSET, sizeof(char*));
    all_h5_filenames_1d = (char*)calloc(NDSET*NAMELEN, sizeof(char));
    for (i = 0; i < NDSET; i++) 
        all_h5_filenames[i] = all_h5_filenames_1d + i*NAMELEN;

    char **all_query_names, *all_query_names_1d;
    all_query_names    = (char**)calloc(n_query+1, sizeof(char*));
    all_query_names_1d = (char*)calloc((n_query+1)*NAMELEN, sizeof(char));
    for (i = 0; i < n_query+1; i++) 
        all_query_names[i] = all_query_names_1d + i*NAMELEN;

    int *my_query_exist  = (int*)calloc(n_query, sizeof(int));
    int *all_query_exist = (int*)calloc(n_query, sizeof(int));

    // Rank 0 read from pm file
    if (my_rank == 0) {

        printf("Reading from manifest file [%s]\n", manifest_fname);
        FILE *manifest_fp = fopen(manifest_fname, "r");
        if (NULL == manifest_fp) {
            fprintf(stderr, "Error opening file: %s\n", manifest_fname);
            goto done;
        }

        i = 0;
        while (NULL != fgets(all_h5_filenames[i], NAMELEN-1, manifest_fp)) {
            all_h5_filenames[i][strlen(all_h5_filenames[i]) - 1] = '\0';
            i++;
            if (i > NDSET) {
                printf("Proc %d: number of files in menifest exceeds %d\n", my_rank, NDSET);
                break;
            }
        }
        fclose(manifest_fp);
        count = i;

        /* for (i = 0; i < count; i++) { */
        /*     printf("[%s]\n", all_h5_filenames[i]); */
        /* } */

        printf("Reading from query file [%s]\n", query_fname);
        FILE *query_fp = fopen(query_fname, "r");
        if (NULL == query_fp) {
            fprintf(stderr, "Error opening file: %s\n", query_fname);
            goto done;
        }

        i = 0;
        while (NULL != fgets(all_query_names[i], NAMELEN-1, query_fp)) {
            all_query_names[i][strlen(all_query_names[i]) - 1] = '\0';
            i++;
            if (i > n_query) {
                printf("Proc %d: number of querys in query file exceeds %d\n", my_rank, n_query);
                break;
            }
        }
        fclose(query_fp);
    }


    fflush(stdout);
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(all_h5_filenames_1d,  count*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast(all_query_names_1d, n_query*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);

    // Distribute work evenly
    assign_work_to_rank(my_rank, size, count, &my_count, &my_start);

    /* printf("Proc %d: my_count: %d, my_start: %d\n", my_rank, my_count, my_start); */
    /* fflush(stdout); */

    double start_time, end_time, query_time;
    hid_t file_id, grp_id, dset_id, lapl;
    char grp_id_name[128] = {0};
    int is_exist;
    lapl = H5Pcreate(H5P_LINK_ACCESS);

    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();

    // Query objects
    // Each rank opens HDF5 files and checking if the queried object exist in current opened file

    /* Save old error handler */
    /* H5E_auto_t (*old_func)(void*); */
    /* void *old_client_data; */
    /* H5Eget_auto(H5E_DEFAULT, &old_func, &old_client_data); */

    /* Turn off HDF5 error handling to avoid massive error prints when checking an object's existence */
    H5Eset_auto(H5E_DEFAULT, NULL, NULL);

    for (i = 0; i < my_count; i++) {
        if (i+my_start >= count || i+my_start<0) {
            printf("Proc %d: error with filename array %d\n", i+my_start);
            continue;
        }
        /* printf("Proc %d: opening file id %d\n", my_rank, i+my_start); */
        /* fflush(stdout); */

        file_id = H5Fopen(all_h5_filenames[i+my_start], H5F_ACC_RDONLY, H5P_DEFAULT);
        if (file_id < 0) {
            printf("Proc %d: error opening file [%s]\n", my_rank, all_h5_filenames[i+my_start]);
            continue;
        }

        for (j = 0; j < n_query; j++) {
            /* printf("Proc %d: querying [%s]\n", my_rank, all_query_names[j]); */

            is_exist = 1;
            for (k = 5; k < strlen(all_query_names[j]); k++) {
                if(all_query_names[j][k] == '/') {
                    strncpy(grp_id_name, all_query_names[j], k);
                    grp_id_name[k] = '\0';
                    /* printf("== Checking %s\n", grp_id_name); */
                    /* fflush(stdout); */
                    if ( H5Lexists(file_id, grp_id_name, lapl) <= 0) {
                        is_exist = -1;
                        break;
                    }
                }
            }
            is_exist = H5Lexists(file_id, all_query_names[j], lapl);

            if (is_exist > 0) {
                // Mark the dataset exist
                my_query_exist[j] = i+my_start;
                /* printf("Proc %d: [%s] has [%s]\n", my_rank,all_h5_filenames[i+my_start],all_query_names[j]); */
                /* fflush(stdout); */
            }

        }
        H5Fclose(file_id);
    }
    H5Pclose(lapl);

    MPI_Allreduce(my_query_exist, all_query_exist, n_query, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    /* /1* Restore previous HDF5 error handler *1/ */
    /* H5Eset_auto(H5E_DEFAULT, old_func, old_client_data); */

    fflush(stdout);
    end_time = MPI_Wtime();
    query_time = end_time - start_time;

    if (my_rank == 0) {
        printf("Total query time: %.4f\n", query_time);
        fflush(stdout);
    }
    int my_read_count, my_read_start;
    assign_work_to_rank(my_rank, size, n_query, &my_read_count, &my_read_start);

    /* printf("Proc %d: my_read_count: %d, my_read_start: %d\n", my_rank, my_read_count, my_read_start); */
    /* fflush(stdout); */

    char *fname, *query_name;
    void **data = (void**)calloc(my_read_count, sizeof(void*));
    hid_t dspace_id, dtype_id, plist_id; 
    herr_t status;
    hsize_t dims[3], dtype_size, dset_size;
    double total_size = 0, all_size;
    int ndim;

    // Read objects
    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();


    for (i = 0; i < my_read_count; i++) {
        fname = all_h5_filenames[all_query_exist[i+my_read_start]];
        query_name = all_query_names[i+my_read_start];
        /* printf("Proc %d: reading [%s] from [%s]\n", my_rank, query_name, fname); */
        /* fflush(stdout); */
        file_id = H5Fopen(fname, H5F_ACC_RDONLY, H5P_DEFAULT);
        if (file_id < 0) {
            printf("Proc %d: error opening file [%s]\n", my_rank, all_h5_filenames[i+my_start]);
            continue;
        }

        dset_id = H5Dopen2(file_id, query_name, H5P_DEFAULT);
        if (dset_id < 0) {
             printf("Proc %d: error opening dset_id [%s]\n", my_rank, query_name);
             continue;
        }
        dspace_id  = H5Dget_space(dset_id);
        dtype_id   = H5Dget_type(dset_id);
        dtype_size = H5Tget_size(dtype_id);
        ndim       = H5Sget_simple_extent_ndims(dspace_id);
        H5Sget_simple_extent_dims(dspace_id, dims, NULL);
        dset_size = dtype_size;
        for (j = 0; j < ndim; j++) 
            dset_size *= dims[j];

        /* printf("Total size for dset [%s] is %lu\n", query_name, dset_size); */

        data[i] = (void*)malloc(dset_size);

        status = H5Dread(dset_id, dtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, data[i]);
        if (status < 0) {
            printf("Proc %d: error reading dataset [%s]\n", my_rank, query_name);
            continue;
        }
        total_size += (double)dset_size;

        H5Dclose(dset_id);
        H5Fclose(file_id);
    }

    fflush(stdout);
    MPI_Barrier(MPI_COMM_WORLD);
    end_time = MPI_Wtime();

    MPI_Reduce(&total_size, &all_size, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    all_size /= 1048576.0;

    if (my_rank == 0) {
        /* printf("Proc %d: read data %.2f MB\n", my_rank, total_read_size/1048576.0); */
        printf("Total read data %.2f MB, time: %.4f\n", all_size, end_time - start_time);
        fflush(stdout);
        /* float *data_ptr = (float*)data[0]; */
        /* for (i = 0; i < 20; i++) { */
        /*     printf(" %f,", *data_ptr); */ 
        /*     data_ptr++; */
        /* } */
        /* printf("\n"); */
    }

    for (i = 0; i < my_read_count; i++) 
        free(data[i]);
    free(data);

done:
    fflush(stdout);

    if (all_h5_filenames) 
        free(all_h5_filenames);
    if (all_h5_filenames_1d) 
        free(all_h5_filenames_1d);
    if (all_query_names) 
        free(all_query_names);
    if (all_query_names_1d) 
        free(all_query_names_1d);
    if (my_query_exist) 
        free(my_query_exist);
    if (all_query_exist) 
        free(all_query_exist);

    
    
    MPI_Finalize();
    return 0;
}
