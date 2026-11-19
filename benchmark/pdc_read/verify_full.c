/* Validate every record byte outside the timed benchmark, matching generator ranks. */
#include <mpi.h>
#include <hdf5.h>
#include <pdc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef DIM1
#define DIM1 10240
#endif
#ifndef DIM2
#define DIM2 10240
#endif
#define RECORD_BYTES ((size_t)DIM1 * DIM2)
int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (argc != 3 || !getenv("DATA_DIR"))
        MPI_Abort(MPI_COMM_WORLD, 2);
    int      pdc_backend = strcmp(argv[1], "pdc") == 0, total = atoi(argv[2]), count = 0;
    uint8_t *expected = malloc(RECORD_BYTES), *actual = malloc(RECORD_BYTES);
    if (!expected || !actual || total <= 0)
        MPI_Abort(MPI_COMM_WORLD, 3);
    srand(10 + rank);
    for (size_t i = 0; i < RECORD_BYTES; i++)
        expected[i] = (uint8_t)(rand() % 255);
    pdcid_t pdc = 0;
    if (pdc_backend && !(pdc = PDCinit("pdc")))
        MPI_Abort(MPI_COMM_WORLD, 4);
    for (int file = rank; file < total; file += size) {
        char path[1024];
        if (pdc_backend) {
            snprintf(path, sizeof(path), "%s/dataset_%d", getenv("DATA_DIR"), file);
            pdcid_t  obj       = PDCobj_open(path, pdc);
            uint64_t offset[3] = {0, 0, 0}, dims[3] = {1, DIM1, DIM2};
            pdcid_t  local = PDCregion_create(3, offset, dims), remote = PDCregion_create(3, offset, dims);
            pdcid_t  tr = PDCregion_transfer_create(actual, PDC_READ, obj, local, remote);
            if (!obj || !local || !remote || !tr || PDCregion_transfer_start(tr) < 0 ||
                PDCregion_transfer_wait(tr) < 0)
                MPI_Abort(MPI_COMM_WORLD, 5);
            PDCregion_transfer_close(tr);
            PDCregion_close(local);
            PDCregion_close(remote);
            PDCobj_close(obj);
        }
        else {
            snprintf(path, sizeof(path), "%s/dlio_data_file_%d.h5", getenv("DATA_DIR"), file);
            hid_t input = H5Fopen(path, H5F_ACC_RDONLY, H5P_DEFAULT);
            hid_t obj   = H5Dopen2(input, "records", H5P_DEFAULT);
            if (input < 0 || obj < 0 ||
                H5Dread(obj, H5T_NATIVE_UINT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, actual) < 0)
                MPI_Abort(MPI_COMM_WORLD, 6);
            H5Dclose(obj);
            H5Fclose(input);
        }
        if (memcmp(actual, expected, RECORD_BYTES) != 0) {
            fprintf(stderr, "rank %d file %d: record mismatch\n", rank, file);
            MPI_Abort(MPI_COMM_WORLD, 7);
        }
        count++;
    }
    int all_count;
    MPI_Reduce(&count, &all_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    if (!rank)
        printf("VERIFIED backend=%s files=%d bytes_per_file=%zu generator_ranks=%d\n", argv[1], all_count,
               RECORD_BYTES, size);
    if (pdc_backend)
        PDCclose(pdc);
    free(expected);
    free(actual);
    MPI_Finalize();
    return 0;
}
