#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hdf5.h>

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#define NDSET 25304802
#define NAMELEN 64

void print_usage() {
    printf("Usage: srun -n n_proc ./h5boss_read_write_h5 /path/to/read_list.txt n_read output_file \n");
}

int main(int argc, const char *argv[])
{
    int size, rank;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif

    char *in_filename = NULL, *out_filename = NULL;
    char **all_dset_names = NULL, *all_dset_names_1d = NULL;
    char **target_h5_names = NULL, *target_h5_names_1d = NULL;

    char tmp_input[512];
    int i, j, n_write = NDSET, my_n_write = 0, my_write_start;
    
    if (argc != 4 && argc != 3 ) {
        print_usage();
        goto done;
    }

    in_filename  = argv[1];
    n_write      = atoi(argv[2]);

    if (argc == 4) 
        out_filename = argv[3];

    if (rank == 0) 
        printf("Writing %d datasets.\n", n_write);

    all_dset_names     = (char**)calloc(n_write, sizeof(char*));
    all_dset_names_1d  = (char*)calloc(n_write*NAMELEN, sizeof(char));

    target_h5_names    = (char**)calloc(n_write, sizeof(char*));
    target_h5_names_1d = (char*)calloc(n_write*NAMELEN, sizeof(char));

    for (i = 0; i < n_write; i++) {
        all_dset_names[i]  = all_dset_names_1d + i*NAMELEN;
        target_h5_names[i] = target_h5_names_1d + i*NAMELEN;
    }

    // Rank 0 read from input file
    if (rank == 0) {
        printf("Reading from %s\n", in_filename);
        FILE *pm_file = fopen(in_filename, "r");
        if (NULL == pm_file) {
            fprintf(stderr, "Error opening file: %s\n", in_filename);
            return (-1);
        }

        i = 0;
        while (fgets(tmp_input, 512, pm_file) != NULL ) {
            if (i >= n_write) 
                break;
            sscanf(tmp_input, "%[^:]:%s", all_dset_names[i], target_h5_names[i]);
            /* for (j = 0; j < strlen(all_dset_names[i]); j++) { */
            /*     if (all_dset_names[i][j] == '/') */ 
            /*         all_dset_names[i][j] = '-'; */
            /* } */
            i++;
        }

        fclose(pm_file);
        n_write = i;

        /* for (i = 0; i < n_write; i++) { */
        /*     printf("[%s]: %d\n", all_dset_names[i], target_h5_names[i]); */
        /* } */
    }

    fflush(stdout);
    my_n_write  = n_write;

#ifdef ENABLE_MPI
    MPI_Bcast( &n_write, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast( all_dset_names_1d, n_write*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast( target_h5_names_1d, n_write*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);
    
    // Distribute work evenly
    // Last rank may have extra work
    my_n_write = n_write / size;
    my_write_start = rank * my_n_write;
    if (rank == size - 1) 
        my_n_write += n_write % size;

#endif 

    /* for (i = 0; i < n_write; i++) { */
    /*     printf("Rank %d - [%s]: %s\n", rank, all_dset_names[i], target_h5_names[i]); */
    /* } */

    // Prepare the write file and create all datasets
    hid_t file_id, out_file_id, grp_id, dset_id, filespace;
    hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);

    if (out_filename != NULL) {
        H5Pset_fapl_mpio(plist_id, MPI_COMM_WORLD, MPI_INFO_NULL);
        out_file_id = H5Fcreate(out_filename, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
        H5Pclose(plist_id);
        if (out_file_id < 0) {
            printf("Error creating a file [%s]\n", out_filename);
            goto done;
        }

        // TODO
        // Now all processes will create all datasets
        /* hsize_t dims[3] = {0}; */
        /* for (i = 0; i < n_write; i++) { */
        /*     /1* dims[0] = all_dset_sizes[i]; *1/ */
        /*     filespace = H5Screate_simple(1, dims, NULL); */ 

        /*     dset_id = H5Dcreate(out_file_id, all_dset_names[i], H5T_NATIVE_CHAR, filespace, */ 
        /*                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */
        /*     H5Sclose(filespace); */
        /*     H5Dclose(dset_id); */
        /* } */
    }

    void *buf = NULL;
    int prev_buf_size;
    herr_t ret;
    char my_grp_name[128] = {0};
    char my_dset_name[128] = {0};
    int idx;

    for (i = 0; i < my_n_write; i++) {
        idx = i + my_write_start;

        file_id = H5Fopen(target_h5_names[idx], H5F_ACC_RDONLY, H5P_DEFAULT);
        if (file_id < 0) {
            printf("Error opening a file [%s]\n", target_h5_names[idx]);
            continue;
        }

        for (j = strlen(all_dset_names[idx]); j >= 0; j--) {
            if (all_dset_names[idx][j] == '/') 
                break;
        }
        strcpy(my_dset_name, &all_dset_names[idx][j+1]);
        strncpy(my_grp_name, &all_dset_names[idx][0], j);
        my_grp_name[j] = 0;

        grp_id = H5Gopen(file_id, my_grp_name, H5P_DEFAULT); 
        if (grp_id < 0) {
            printf("Error opening the group [%s]\n", my_grp_name);
            continue;
        }

        dset_id = H5Dopen(grp_id, my_dset_name, H5P_DEFAULT);
        if (dset_id < 0) {
            printf("Error opening the dataset [%s]\n", my_dset_name);
            continue;
        }

        /* if (buf == NULL) { */
        /*     buf = (void*)calloc(sizeof(void), 10486760); */
        /*     prev_buf_size = 10486760; */
        /* } */

        /* ret = H5Dread(dset_id, H5T_NATIVE_CHAR, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf); */
        /* if (ret < 0) { */
        /*     printf("Error writing to the dataset [%s]\n", all_dset_names[idx]); */
        /*     H5Dclose(dset_id); */
        /*     continue; */
        /* } */
        /* printf("Proc %d: written dset [%s]\n", rank, all_dset_names[idx]); */
        H5Dclose(dset_id);
        H5Gclose(grp_id);
        H5Fclose(file_id);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (out_filename == NULL) {
        H5Fclose(out_file_id);
    }

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
