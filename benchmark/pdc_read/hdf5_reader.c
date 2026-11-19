/* Derived from the SC26 I/O artifact by Hyunju Oh. See LICENSE.artifact. */
#include <errno.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <mpi.h>
#include <hdf5.h>

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

#ifndef NUM_EPOCHS
#define NUM_EPOCHS 5
#endif
#define BASE_SEED 42

static uint64_t rand_state;

void
set_seed(uint64_t seed)
{
    rand_state = seed;
}

uint32_t
get_rand()
{
    rand_state = rand_state * 6364136223846793005ULL + 1;
    return (uint32_t)(rand_state >> 32);
}

void
shuffle_indices(int *array, int n, uint64_t seed)
{
    set_seed(seed);
    for (int i = n - 1; i > 0; i--) {
        int j    = get_rand() % (i + 1);
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
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
        if (rank == 0)
            fprintf(stderr, "Usage: %s <total_files>\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    const char *data_dir = getenv("DATA_DIR");
    if (!data_dir)
        data_dir = "../data_generator/hdf5_data_dir";

    int total_files = atoi(argv[1]);
    if (total_files <= 0) {
        if (rank == 0)
            fprintf(stderr, "Error: total_files must be greater than 0.\n");
        MPI_Finalize();
        return 1;
    }

    // Dynamic global sample calculation
    int total_global_samples = total_files * NUM_SAMPLES_PER_FILE;

    int *global_indices = (int *)malloc(total_global_samples * sizeof(int));
    for (int i = 0; i < total_global_samples; i++) {
        global_indices[i] = i;
    }

    // Allocate buffers for both datasets
    uint8_t *image_buffer = (uint8_t *)malloc(RECORD_LENGTH_BYTES * sizeof(uint8_t));

    // Define memory spaces matching the generator
    // 3D space for records
    hsize_t mem_dims[3] = {1, DIM1, DIM2};
    hid_t   memspace_id = H5Screate_simple(3, mem_dims, NULL);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        printf("--- Starting HDF5 Data Reader Benchmark ---\n");
        printf("Ranks: %d | Epochs: %d | Total Files: %d\n", size, NUM_EPOCHS, total_files);
        printf("Reading 3D 'records' [%d, %d, %d] \n", 1, DIM1, DIM2);
    }

    double total_benchmark_start = MPI_Wtime();

    // ==========================================
    // EPOCH LOOP
    // ==========================================
    for (int epoch = 0; epoch < NUM_EPOCHS; epoch++) {

        // Match PyTorch seed progression
        shuffle_indices(global_indices, total_global_samples, BASE_SEED + epoch);

        double epoch_start = MPI_Wtime();

        // MPI Stride Loop: Read through the globally shuffled array
        for (int i = rank; i < total_global_samples; i += size) {
            int current_global_idx = global_indices[i];

            int target_file_idx  = current_global_idx / NUM_SAMPLES_PER_FILE;
            int local_sample_idx = current_global_idx % NUM_SAMPLES_PER_FILE;

            // char filename[256];
            // sprintf(filename, "dlio_data_file_%d.h5", target_file_idx);
            char filename[512];
            sprintf(filename, "%s/dlio_data_file_%d.h5", data_dir, target_file_idx);

            double sample_start_time = MPI_Wtime();

            // Open File
            hid_t file_id = H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT);

            // Read records
            hid_t dset_id      = H5Dopen(file_id, "records", H5P_DEFAULT);
            hid_t filespace_id = H5Dget_space(dset_id);

            hsize_t offset[3] = {local_sample_idx, 0, 0};
            hsize_t count[3]  = {1, DIM1, DIM2};

            H5Sselect_hyperslab(filespace_id, H5S_SELECT_SET, offset, NULL, count, NULL);
            H5Dread(dset_id, H5T_NATIVE_UINT8, memspace_id, filespace_id, H5P_DEFAULT, image_buffer);

            // if (rank == 0)
            //     printf("image_buffer[100]: %u\n", image_buffer[100]);

            H5Sclose(filespace_id);
            H5Dclose(dset_id);
            H5Fclose(file_id);

            double sample_end_time = MPI_Wtime();
            double sample_duration = sample_end_time - sample_start_time;

            // Print the timing profile for this sample
            printf("[RANK %d | EPOCH %d] Read Sample %d in %.4f sec\n", rank, epoch + 1, current_global_idx,
                   sample_duration);

            memset(image_buffer, 0, (size_t)RECORD_LENGTH_BYTES);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        double epoch_end = MPI_Wtime();

        if (rank == 0) {
            double epoch_time = epoch_end - epoch_start;
            printf("Epoch %d Complete | Time: %.4f sec\n", epoch + 1, epoch_time);
        }
    }

    double total_benchmark_end = MPI_Wtime();

    // Cleanup
    H5Sclose(memspace_id);
    free(image_buffer);
    free(global_indices);

    if (rank == 0) {
        printf("\nTotal Benchmark Time: %.4f sec\n", total_benchmark_end - total_benchmark_start);
    }

    MPI_Finalize();
    return 0;
}
