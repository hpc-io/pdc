#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
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
        switch (c) {
            case 'f':
                *file_name = optarg;
                break;
            case '?':
                if (optopt == 'f') {
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                }
                else {
                    fprintf(stderr, "Unknown option: -%c\n", optopt);
                }
                return 1;
            default:
                abort();
        }
    }
}

int
main(int argc, char *argv[])
{

    char *file_name = NULL;
    void *tiff      = NULL;

    int parse_code = parse_console_args(argc, argv, &file_name);

    if (parse_code) {
        return parse_code;
    }

    printf("Filename: %s\n", file_name ? file_name : "(none)");

    parallel_TIFF_load(file_name, &tiff, 1, NULL);

    return 0;
}