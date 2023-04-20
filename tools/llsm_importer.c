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
#include "llsm/imageListReader.h"

int
parse_console_args(int argc, char *argv[], char **file_name)
{
    int c, parse_code = -1;

    while ((c = getopt(argc, argv, "f:")) != -1) {
        switch (c) {
            case 'f':
                *file_name = optarg;
                parse_code = 0;
                break;
            default:
                fprintf(stderr, "Usage: %s [-f filename]\n", argv[0]);
                parse_code = -1;
                exit(EXIT_FAILURE);
        }
    }
    return parse_code;
}

void
print_image_file_info(const image_file_info_t *image_info)
{
    printf("Filepath: %s\n", image_info->filepath);
    printf("Filename: %s\n", image_info->filename);
    printf("Stage X (um): %.2f\n", image_info->stageX_um_);
    printf("Stage Y (um): %.2f\n", image_info->stageY_um_);
    printf("Stage Z (um): %.2f\n", image_info->stageZ_um_);
    printf("Objective X (um): %.2f\n", image_info->objectiveX_um_);
    printf("Objective Y (um): %.2f\n", image_info->objectiveY_um_);
    printf("Objective Z (um): %.2f\n", image_info->objectiveZ_um_);
}

void
on_image(image_file_info_t *imageinfo)
{
    print_image_file_info(imageinfo);
    // calling tiff loading process.
    void *tiff      = NULL;
    int   i         = 0;
    parallel_TIFF_load(imageinfo->filename, &tiff, 1, NULL);

    if (!tiff)
        return 1;

    printf("first few bytes ");
    for (i = 0; i < 10; i++) {
        printf("%d ", ((uint8_t *)tiff)[i]);
    }
    printf("\n");
    free(tiff);
}

int
main(int argc, char *argv[])
{

    char *file_name = NULL;
    
    
    char  bytes[10];
    
    // parse console argument
    int parse_code = parse_console_args(argc, argv, &file_name);
    if (parse_code) {
        return parse_code;
    }

    // print file name for validating purpose
    printf("Filename: %s\n", file_name ? file_name : "(none)");

    scan_image_list(file_name, &on_image);

    return 0;
}