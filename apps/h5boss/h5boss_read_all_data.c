#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

#define NDSET 2500
#define NAMELEN 128

int main(int argc, char *argv[])
{
    int size, my_rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    char *in_fname = argv[1];
    char *out_path = argv[2];
    char out_fname[128];
    sprintf(out_fname, "%s/server%d.bin", out_path, my_rank);

    int count, i, my_count, my_start;
    char **all_dset_names, *all_dset_names_1d;
    all_dset_names    = (char**)calloc(NDSET, sizeof(char*));
    all_dset_names_1d = (char*)calloc(NDSET*NAMELEN, sizeof(char));
    for (i = 0; i < NDSET; i++) 
        all_dset_names[i] = all_dset_names_1d + i*NAMELEN;


    // Rank 0 read from pm file
    if (my_rank == 0) {

        printf("Reading from %s\nWrite to %s\n", in_fname, out_fname);
        FILE *in_file_ptr = fopen(in_fname, "r");
        if (NULL == in_file_ptr) {
            fprintf(stderr, "Error opening file: %s\n", in_fname);
            return (-1);
        }

        i = 0;
        while (EOF != fscanf(in_file_ptr, "%s", all_dset_names[i])) {
            i++;
        }

        fclose(in_file_ptr);
        count = i;

        /* for (i = 0; i < count; i++) { */
        /*     printf("[%s]\n", all_dset_names[i]); */
        /* } */
    }

    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(all_dset_names_1d, count*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);

    // Distribute work evenly
    my_count = count / size;
    my_start = my_rank * my_count;

    // Last rank may have extra work
    if (my_rank >= size - count % size) {
        my_count++; 
        my_start += my_rank - (size - count % size);
    }

    /* printf("Proc %d: my_count: %d, my_start: %d\n", my_rank, my_count, my_start); */
    /* fflush(stdout); */

    FILE *fp = NULL;
    void **buf = NULL;
    long fsize = 0, fsize_total = 0;
    double start_time, end_time;
    buf = (void**)calloc(sizeof(void*), my_count);

    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();
    for (i = 0; i < my_count; i++) {
        /* printf("Proc %d: opening file [%s]\n", my_rank, all_dset_names[i+my_start]); */
        fp = fopen(all_dset_names[i+my_start], "rb");
        if (NULL == fp) {
            printf("Proc %d: error opening file [%s]\n", my_rank, all_dset_names[i+my_start]);
            continue;
        }
        fseek(fp, 0, SEEK_END);
        fsize = ftell(fp);
        fsize_total += fsize;
        fseek(fp, 0, SEEK_SET);
        buf[i] = malloc(fsize + 1);
        fread(buf[i], fsize, 1, fp);
        printf("Proc %d: read data %.2f MB\n", my_rank, fsize/1048576.0);
        fclose(fp);
    }

    fflush(stdout);
    MPI_Barrier(MPI_COMM_WORLD);
    end_time = MPI_Wtime();

    if (my_rank == 0) {
        printf("Total read time from Lustre: %.4f\n", end_time - start_time);
        fflush(stdout);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();
    fp = fopen(out_fname, "ab");
    for (i = 0; i < my_count; i++) {
        /* printf("Proc %d: opening file [%s]\n", my_rank, all_dset_names[i+my_start]); */
        if (NULL == fp) {
            printf("Proc %d: error opening file [%s]\n", my_rank, out_fname);
            continue;
        }
        fwrite(buf[i], fsize, 1, fp);
        /* printf("Proc %d: write data %.2f MB\n", my_rank, fsize/1048576.0); */
        /* fflush(stdout); */
    }
    fclose(fp);

    fflush(stdout);
    MPI_Barrier(MPI_COMM_WORLD);
    end_time = MPI_Wtime();

    if (my_rank == 0) {
        printf("Total write time to BB: %.4f\n", end_time - start_time);
        fflush(stdout);
    }


    for (i = 0; i < my_count; i++) {
        free(buf[i]);
    }
    free(buf);

    fflush(stdout);
    void *new_buf;
    new_buf = malloc(fsize_total + 1);
    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();

    /* printf("Proc %d: opening file [%s]\n", my_rank, out_fname); */
    fp = fopen(out_fname, "rb");
    if (NULL == fp) {
        printf("Proc %d: error opening file [%s]\n", my_rank, out_fname);
        goto done;
    }
    fseek(fp, 0, SEEK_END);
    fseek(fp, 0, SEEK_SET);
    fread(new_buf, fsize_total, 1, fp);
    fclose(fp);
    fflush(stdout);

    MPI_Barrier(MPI_COMM_WORLD);
    end_time = MPI_Wtime();

    if (my_rank == 0) {
        printf("Proc %d: read data from BB %.2f MB\n", my_rank, fsize_total/1048576.0);
        printf("Total read time from Lustre: %.4f\n", end_time - start_time);
        fflush(stdout);
    }


    free(new_buf);

done:
    MPI_Finalize();
    return 0;
}
