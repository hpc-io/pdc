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
#include <libgen.h>

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
on_image(image_file_info_t *image_info, img_scan_callback_args_t *args)
{
    print_image_file_info(image_info);
    
    char *dirname = (char *)args->input;
    char filepath[256];
    // calling tiff loading process.
    void *tiff      = NULL;
    int   i         = 0;

    // check if the path ends with a forward slash
    if (dirname[strlen(dirname) - 1] != '/') {
        strcat(dirname, "/"); // add a forward slash to the end of the path
    }

    strcpy(filepath, dirname); // copy the directory path to the file path
    strcat(filepath, image_info->filename); // concatenate the file name to the file path


    parallel_TIFF_load(filepath, &tiff, 1, NULL);

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
    img_scan_callback_args_t callback_args;
    // parse console argument
    int parse_code = parse_console_args(argc, argv, &file_name);
    if (parse_code) {
        return parse_code;
    }
    char* directory_path = dirname(strdup(file_name));

    // print file name for validating purpose
    printf("Filename: %s\n", file_name ? file_name : "(none)");
    printf("Directory: %s\n", directory_path ? directory_path : "(none)");

    callback_args.input = (void *)directory_path;
    scan_image_list(file_name, &on_image, &callback_args);

    return 0;
}