#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

#define FILEPATH "/global/cscratch1/sd/houhun/link_test/subfiles"
#define NFILE 268

void print_usage(const char *name) {
    printf("Usage: srun -n ./%s num_links_to_create [stat|exist]\n", name);
}

int main(int argc, char **argv)
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    int i, j, count, my_count;
    char my_files[1001][256];

    if (argc < 3) {
        print_usage(argv[0]);
        exit(-1);
    }

    count = atoi(argv[1]);
    char *link_dir = argv[2];

    int stat_only = 0, exist_only = 0;
    if (strcmp(argv[3], "stat") == 0) {
        stat_only = 1;
    }
    if (strcmp(argv[3], "exist") == 0) {
        exist_only = 1;
    }

    my_count = count / size;
    // last proc may get extra work
    if (rank == size - 1) {
        my_count += count % size;
    }

    // Read file path to array
    /* printf("Reading from %s\n", pm_filename); */
    char my_subfile[256];
    sprintf(my_subfile, "%s/x%03d", FILEPATH, rank % NFILE);

    FILE *pm_file = fopen(my_subfile, "r");

    if (NULL == pm_file) {
        fprintf(stderr, "Error opening file: %s\n", my_subfile);
        goto done;
    }

    i = 0;
    while (EOF != fscanf(pm_file, "%s", my_files[i])) {
        /* printf("%s\n", my_files[i]); */
        i++;
        if (i >= my_count) {
            break;
        }
    }

    fclose(pm_file);
    my_count = i;

    if (rank == 0) {
        printf("0: Creating %d sym links under %s\n", my_count, link_dir);
    }


    char link_name[256];


    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    // Create Link
    gettimeofday(&ht_total_start, 0);

    for (i = 0; i < my_count; i++) {
        sprintf(link_name, "%s/%d_%d.link", link_dir, rank, i);
        symlink(my_files[i], link_name);
    }

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    if (rank == 0) {
        printf("Time to create %d symlinks with %d ranks: %.6f\n", count, size, ht_total_sec);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    int status;
    // Test file exist
    if (exist_only == 1) {
        gettimeofday(&ht_total_start, 0);

        for (i = 0; i < my_count; i++) {
            status = access( my_files[i], F_OK );
            if (status == -1) {
                printf("%s doesn't exist\n",  my_files[i]);
            }
        }

        gettimeofday(&ht_total_end, 0);
        ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
        ht_total_sec        = ht_total_elapsed / 1000000.0;

        if (rank == 0) {
            printf("Time to test %d file exist with %d ranks: %.6f\n", count, size, ht_total_sec);
            fflush(stdout);
        }

    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif


    // Test file exist
    if (stat_only == 1) {
        struct stat sb;
        gettimeofday(&ht_total_start, 0);

        for (i = 0; i < my_count; i++) {
            status = stat(my_files[i], &sb);
            if (status == -1) {
                printf("%s doesn't exist\n",  my_files[i]);
            }

        }

        gettimeofday(&ht_total_end, 0);
        ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
        ht_total_sec        = ht_total_elapsed / 1000000.0;

        if (rank == 0) {
            printf("Time to stat   %d file with %d ranks    : %.6f\n", count, size, ht_total_sec);
            fflush(stdout);
        }

    }

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
