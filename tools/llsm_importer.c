#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>

// #define ENABLE_MPI 1

#ifdef ENABLE_MPI
// #include "mpi.h"
#endif

#include "pdc.h"
// #include "pdc_client_server_common.h"
// #include "pdc_client_connect.h"

#include "llsm/parallelReadTiff.h"

int
parse_console_args(int argc, char *argv[], char **file_name)
{
    int c;

    while ((c = getopt(argc, argv, "f:")) != -1) {
        printf("c : %c \n", c);
        switch (c) {
            case 'f':
                *file_name = optarg;
                break;
            default:
                fprintf(stderr, "Usage: %s [-f filename]\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
}

int
main(int argc, char *argv[])
{

    char *file_name = NULL;
    void *tiff      = NULL;
    int i = 0;
    char bytes[10];
    char *tiff_str_ptr;
    printf("Program started!\n");
    int parse_code = parse_console_args(argc, argv, &file_name);
    printf("parse_code %d\n", parse_code);
    if (parse_code) {
        return parse_code;
    }

    printf("Filename: %s\n", file_name ? file_name : "(none)");

    parallel_TIFF_load(file_name, &tiff, 1, NULL);

    tiff_str_ptr = (char *)tiff;
    for (i = 0; i < 10; i++) {
        bytes[i] = tiff_str_ptr[i];
    }
    printf("first few bytes : %s\n", bytes);

    return 0;
}