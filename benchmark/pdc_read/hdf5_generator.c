/* Derived from the SC26 I/O artifact by Hyunju Oh. See LICENSE.artifact. */
#include <errno.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <mpi.h>
#include <hdf5.h>
#include <math.h>

#ifndef NUM_SAMPLES_PER_FILE
#define NUM_SAMPLES_PER_FILE 1
#endif
#define RECORD_LENGTH_BYTES ((size_t)NUM_SAMPLES_PER_FILE * DIM1 * DIM2)
#ifndef DIM1
#define DIM1 10240
#endif
#ifndef DIM2
#define DIM2 10240
#endif

static void
require_hdf5(int success, int rank, const char *operation, const char *path)
{
    if (!success) {
        fprintf(stderr, "Rank %d: %s failed for %s\n", rank, operation, path);
        MPI_Abort(MPI_COMM_WORLD, 3);
    }
}

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc != 2) {
        if (rank == 0) {
            fprintf(stderr, "Usage: %s <total_files>\n", argv[0]);
            fprintf(stderr, "Example: %s 168\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    const char *data_dir = getenv("DATA_DIR");
    if (!data_dir)
        data_dir = "hdf5_data_dir";

    int total_files = atoi(argv[1]);
    if (total_files <= 0) {
        if (rank == 0)
            fprintf(stderr, "Error: total_files must be greater than 0.\n");
        MPI_Finalize();
        return 1;
    }

    if (rank == 0) {
        printf("--- Starting HDF5 Data Generator ---\n");
        printf("Generating %d files in %s...\n", total_files, data_dir);
    }

    require_hdf5(mkdir(data_dir, 0755) == 0 || errno == EEXIST, rank, "Create directory", data_dir);

    // Allocate buffer and fill with RANDOM data
    uint8_t *dummy_data = (uint8_t *)malloc(RECORD_LENGTH_BYTES * sizeof(uint8_t));
    require_hdf5(dummy_data != NULL, rank, "Allocate record buffer", data_dir);
    srand(10 + rank);
    for (size_t i = 0; i < RECORD_LENGTH_BYTES; i++) {
        dummy_data[i] = (uint8_t)(rand() % 255);
    }

    // Setup the record_label array
    int *record_labels = (int *)calloc(NUM_SAMPLES_PER_FILE, sizeof(int));
    require_hdf5(record_labels != NULL, rank, "Allocate labels", data_dir);

    // Define the 3D data space for records
    hsize_t dims[3]      = {NUM_SAMPLES_PER_FILE, DIM1, DIM2};
    hid_t   dataspace_id = H5Screate_simple(3, dims, NULL);
    require_hdf5(dataspace_id >= 0, rank, "Create record dataspace", data_dir);

    // Define the 1D data space for labels
    hsize_t label_dims[1]  = {NUM_SAMPLES_PER_FILE};
    hid_t   label_space_id = H5Screate_simple(1, label_dims, NULL);
    require_hdf5(label_space_id >= 0, rank, "Create label dataspace", data_dir);

    double start_time = MPI_Wtime();

    for (int file_idx = rank; file_idx < total_files; file_idx += size) {
        char filename[1024];
        int  length = snprintf(filename, sizeof(filename), "%s/dlio_data_file_%d.h5", data_dir, file_idx);
        require_hdf5(length >= 0 && (size_t)length < sizeof(filename), rank, "Format file path", data_dir);

        // Create File
        hid_t file_id = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
        require_hdf5(file_id >= 0, rank, "Create file", filename);

        // Create records
        hid_t dset_id = H5Dcreate(file_id, "records", H5T_NATIVE_UINT8, dataspace_id, H5P_DEFAULT,
                                  H5P_DEFAULT, H5P_DEFAULT);
        require_hdf5(dset_id >= 0, rank, "Create records", filename);
        require_hdf5(H5Dwrite(dset_id, H5T_NATIVE_UINT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, dummy_data) >= 0,
                     rank, "Write records", filename);
        require_hdf5(H5Dclose(dset_id) >= 0, rank, "Close records", filename);

        // Create labels
        hid_t label_dset_id = H5Dcreate(file_id, "labels", H5T_NATIVE_INT, label_space_id, H5P_DEFAULT,
                                        H5P_DEFAULT, H5P_DEFAULT);
        require_hdf5(label_dset_id >= 0, rank, "Create labels", filename);
        require_hdf5(H5Dwrite(label_dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, record_labels) >=
                         0,
                     rank, "Write labels", filename);
        require_hdf5(H5Dclose(label_dset_id) >= 0, rank, "Close labels", filename);
        require_hdf5(H5Fclose(file_id) >= 0, rank, "Close file", filename);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double end_time = MPI_Wtime();

    // Cleanup
    require_hdf5(H5Sclose(dataspace_id) >= 0, rank, "Close record dataspace", data_dir);
    require_hdf5(H5Sclose(label_space_id) >= 0, rank, "Close label dataspace", data_dir);
    free(dummy_data);
    free(record_labels);

    if (rank == 0) {
        printf("Data Generation Complete in %.2f seconds.\n", end_time - start_time);
    }

    MPI_Finalize();
    return 0;
}
