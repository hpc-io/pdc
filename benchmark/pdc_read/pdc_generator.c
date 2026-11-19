/* Derived from the SC26 I/O artifact by Hyunju Oh. See LICENSE.artifact. */
#include <errno.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <inttypes.h>
#include "pdc.h"

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

int main(int argc, char** argv) {
    int rank, size;
    pdcid_t pdc_id;

    MPI_Comm comm;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);

    // Init pdc
    pdc_id = PDCinit("pdc");

    // Parse arguemnt
    if (argc != 2) {
        if (rank == 0) {
            fprintf(stderr, "Usage: %s <total_files>\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    const char *data_dir = getenv("DATA_DIR");
    if (!data_dir) data_dir = "pdc_data_dir";

    int total_files = atoi(argv[1]);
    if (total_files <= 0) {
        if (rank == 0) fprintf(stderr, "Error: total_files must be greater than 0.\n");
        MPI_Finalize();
        return 1;
    }

    if (mkdir(data_dir, 0755) != 0 && errno != EEXIST) {
        perror("Failed to create directory");
        exit(1);
    }

     if (rank == 0) {
        printf("--- Starting PDC Data Generator ---\n");
        printf("Generating %d files in %s...\n", total_files, data_dir);
    }

    // Record data allocation and initialization
    uint8_t* record_data = (uint8_t *) malloc(RECORD_LENGTH_BYTES * sizeof(uint8_t));
    srand(10 + rank);
    for (size_t i = 0; i < RECORD_LENGTH_BYTES; i++) {
        record_data[i] = (uint8_t)(rand() % 255);
    }

    // Label data allocation and initialization
    int* label_data = (int*) calloc(NUM_SAMPLES_PER_FILE, sizeof(int));

    double start_time = MPI_Wtime();

    for (int file_idx = rank; file_idx < total_files; file_idx += size) {
        // Object name
        char rec_out_path[512];
        char lab_out_path[512];

        snprintf(rec_out_path, sizeof(rec_out_path), "%s/dataset_%d", data_dir, file_idx);
        snprintf(lab_out_path, sizeof(lab_out_path), "%s/dataset_%d-labels", data_dir, file_idx);

        // printf("[RANK %d] Generating PDC Data - file %d/%d\n", rank, file_idx + 1, total_files);

        uint64_t dims[3] = {1, DIM1, DIM2}; // same as offset_length
        uint64_t offset[3] = {0, 0, 0};
        uint64_t offset_length[3] = {1, DIM1, DIM2};

        // Create container
        pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
        if (cont_prop <= 0) {
            printf("Fail to create container property @ line  %d!\n", __LINE__);
            return 1;
        }

        pdcid_t cont_id = PDCcont_create(rec_out_path, cont_prop);
        if (cont_id <= 0) {
            printf("Fail to create container @ line  %d!\n", __LINE__);
            return 1;
        }

        // Create a record object property
        pdcid_t obj_record_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
        PDCprop_set_obj_type(obj_record_prop, PDC_UINT8);
        PDCprop_set_obj_dims(obj_record_prop, 3, dims);
        PDCprop_set_obj_user_id(obj_record_prop, getuid());
        PDCprop_set_obj_time_step(obj_record_prop, 0);
        PDCprop_set_obj_transfer_region_type(obj_record_prop, PDC_OBJ_STATIC);

        // Create a label object property
        pdcid_t obj_label_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
        PDCprop_set_obj_type(obj_label_prop, PDC_INT);
        PDCprop_set_obj_dims(obj_label_prop, 1, dims);
        PDCprop_set_obj_user_id(obj_label_prop, getuid());
        PDCprop_set_obj_time_step(obj_label_prop, 0);
        PDCprop_set_obj_transfer_region_type(obj_label_prop, PDC_OBJ_STATIC);

        // Create object
        pdcid_t rec_obj_id = PDCobj_create(cont_id, rec_out_path, obj_record_prop);
        pdcid_t lab_obj_id = PDCobj_create(cont_id, lab_out_path, obj_label_prop);

        // Create record region
        pdcid_t rec_local_reg = PDCregion_create(3, offset, offset_length);
        pdcid_t rec_remote_reg = PDCregion_create(3, offset, offset_length);

        // Create label region
        offset[0]        = 0;
        offset_length[0] = NUM_SAMPLES_PER_FILE;
        pdcid_t lab_local_reg = PDCregion_create(1, offset, offset_length);
        pdcid_t lab_remote_reg = PDCregion_create(1, offset, offset_length);

        // Transfer request
        pdcid_t transfer_request_record = PDCregion_transfer_create(record_data, PDC_WRITE, rec_obj_id, rec_local_reg, rec_remote_reg);
        pdcid_t transfer_request_label = PDCregion_transfer_create(label_data, PDC_WRITE, lab_obj_id, lab_local_reg, lab_remote_reg);

        PDCregion_transfer_start(transfer_request_record);
        PDCregion_transfer_start(transfer_request_label);

        PDCregion_transfer_wait(transfer_request_record);
        PDCregion_transfer_wait(transfer_request_label);

        PDCregion_transfer_close(transfer_request_record);
        PDCregion_transfer_close(transfer_request_label);

        // Region close
        PDCregion_close(rec_local_reg);
        PDCregion_close(rec_remote_reg);
        PDCregion_close(lab_local_reg);
        PDCregion_close(lab_remote_reg);

        // Object close
        PDCobj_close(rec_obj_id);
        PDCobj_close(lab_obj_id);

        // Container and property close
        PDCcont_close(cont_id);
        PDCprop_close(obj_record_prop);
        PDCprop_close(obj_label_prop);
        PDCprop_close(cont_prop);

        // printf("[RANK %d] Generated %s\n", rank, rec_out_path);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double end_time = MPI_Wtime();

    PDCclose(pdc_id);

    free(record_data);
    free(label_data);

    if (rank == 0) {
        printf("Data Generation Complete in %.2f seconds.\n", end_time - start_time);
    }

    MPI_Finalize();
    return 0;
}
