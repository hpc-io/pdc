#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <hdf5.h>

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#define NDSET 25304802
#define NAMELEN 48

void print_usage() {
    printf("Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets(optional)\n");
}

int main(int argc, char *argv[])
{
    int size, rank;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif

    char *in_filename, *out_filename;
    char **all_dset_names = NULL, *all_dset_names_1d = NULL;
    /* char **my_dset_names = NULL, *my_dset_names_1d = NULL; */
    char tmp_input[256];
    int *all_dset_sizes = NULL;
    /* int *my_dset_sizes = NULL; */
    int i, j, n_write = NDSET, my_n_write = 0, my_write_start;
    
    in_filename  = argv[1];
    out_filename = argv[2];
    if (argc < 3 || in_filename == NULL ) {
        print_usage();
        exit(-1);
    }

    if (argc == 4) {
        n_write = atoi(argv[3]);
        if (rank == 0) 
            printf("Writing %d datasets.\n", n_write);
    }


    all_dset_names    = (char**)calloc(n_write, sizeof(char*));
    all_dset_names_1d = (char*)calloc(n_write*NAMELEN, sizeof(char));
    all_dset_sizes = (int*)calloc(n_write, sizeof(int));
    for (i = 0; i < n_write; i++) 
        all_dset_names[i] = all_dset_names_1d + i*NAMELEN;

    // Rank 0 read from input file
    if (rank == 0) {
        printf("Reading from %s\n", in_filename);
        FILE *pm_file = fopen(in_filename, "r");
        if (NULL == pm_file) {
            fprintf(stderr, "Error opening file: %s\n", in_filename);
            return (-1);
        }

        // /3523/55144/1/coadd, 1D: 4619, 147808
        i = 0;
        char dset_ndim[4];
        int  dset_nelem;
        while (fgets(tmp_input, 255, pm_file) != NULL ) {
            if (i >= n_write) 
                break;
            sscanf(tmp_input+1, "%[^,], %[^:]: %d, %d", 
                               all_dset_names[i], dset_ndim, &dset_nelem, &all_dset_sizes[i]);
            if (strcmp(dset_ndim, "1D") != 0) {
                printf("Dimension is %s!\n", dset_ndim);
                continue;
            }
            for (j = 0; j < strlen(all_dset_names[i]); j++) {
                if (all_dset_names[i][j] == '/') 
                    all_dset_names[i][j] = '-';
                
            }
            i++;
        }

        fclose(pm_file);
        n_write = i;

        /* for (i = 0; i < n_write; i++) { */
        /*     printf("[%s]: %d\n", all_dset_names[i], all_dset_sizes[i]); */
        /* } */
    }

    fflush(stdout);
    my_n_write  = n_write;

#ifdef ENABLE_MPI
    MPI_Bcast( &n_write, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast( all_dset_names_1d, n_write*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast( all_dset_sizes, n_write, MPI_INT, 0, MPI_COMM_WORLD);
    
    // Distribute work evenly
    // Last rank may have extra work
    my_n_write = n_write / size;
    my_write_start = rank * my_n_write;
    if (rank == size - 1) 
        my_n_write += n_write % size;

#endif 

    /* for (i = 0; i < n_write; i++) { */
    /*     printf("Rank %d - [%s]: %d\n", rank, all_dset_names[i], all_dset_sizes[i]); */
    /* } */

    hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);
    hid_t file_id, dset_id, filespace;
    H5Pset_fapl_mpio(plist_id, MPI_COMM_WORLD, MPI_INFO_NULL);
    file_id = H5Fcreate(out_filename, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
    H5Pclose(plist_id);
    if (file_id < 0) {
        printf("Error creating a file [%s]\n", out_filename);
        goto done;
    }
    /* H5Fclose(file_id); */

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    /* file_id = H5Fopen(out_filename, H5F_ACC_RDWR, H5P_DEFAULT); */
    /* if (file_id < 0) { */
    /*     printf("Error opening the output file [%s]\n", out_filename); */
    /*     goto done; */
    /* } */

    // Now all processes will create all datasets
    hsize_t dims[3] = {0};

    clock_t before = clock();

    for (i = 0; i < n_write; i++) {
        dims[0] = all_dset_sizes[i];
        filespace = H5Screate_simple(1, dims, NULL); 

        dset_id = H5Dcreate(file_id, all_dset_names[i], H5T_NATIVE_CHAR, filespace, 
                            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Sclose(filespace);
        H5Dclose(dset_id);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    clock_t difference = clock() - before;
    double msec = difference * 1000 / CLOCKS_PER_SEC;
    if (rank == 0) {
        printf("Dset create: %f\n", msec / 1000.0);
    }


    hsize_t count = 0;
    char *buf = NULL;
    int prev_buf_size;
    herr_t ret;

    before = clock();
    for (i = 0; i < my_n_write; i++) {
        count = all_dset_sizes[i+my_write_start];
        dset_id = H5Dopen(file_id, all_dset_names[my_write_start+i], H5P_DEFAULT);
        if (dset_id < 0) {
            printf("Error opening the dataset [%s]\n", all_dset_names[my_write_start+i]);
            continue;
        }
        if (buf == NULL) {
            buf = (char*)calloc(sizeof(char), all_dset_sizes[i+my_write_start]);
            prev_buf_size = all_dset_sizes[i+my_write_start];
        }
        else {
            if (prev_buf_size < all_dset_sizes[i+my_write_start]) {
                buf = realloc(buf, all_dset_sizes[i+my_write_start]);
            }
        }
        buf[0] = all_dset_names[my_write_start+i][1]; 
        buf[1] = all_dset_names[my_write_start+i][2]; 
        buf[2] = all_dset_names[my_write_start+i][3]; 
        buf[3] = all_dset_names[my_write_start+i][4]; 
        ret = H5Dwrite(dset_id, H5T_NATIVE_CHAR, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf);
        if (ret < 0) {
            printf("Error writing to the dataset [%s]\n", all_dset_names[my_write_start+i]);
            H5Dclose(dset_id);
            continue;
        }
        /* printf("Proc %d: written dset [%s]\n", rank, all_dset_names[my_write_start+i]); */
        /* all_dset_names[my_write_start+i] */
        H5Dclose(dset_id);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    difference = clock() - before;
    msec = difference * 1000 / CLOCKS_PER_SEC;
    if (rank == 0) {
        printf("Dset create: %f\n", msec / 1000.0);
    }


    H5Fclose(file_id);

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
