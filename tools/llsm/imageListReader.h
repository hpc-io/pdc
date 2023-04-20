#ifndef IMAGELISTREADER_H
#define IMAGELISTREADER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>

typedef struct {
    char *filepath;
    char *filename;
    double stageX_um_;
    double stageY_um_; 
    double stageZ_um_; 
    double objectiveX_um_; 
    double objectiveY_um_; 
    double objectiveZ_um_;
} image_file_info_t;

typedef struct {
    void *input;
    void *output;
} img_scan_callback_args_t;

typedef void (*on_image_ptr_t)(image_file_info_t *, img_scan_callback_args_t *args);

void scan_image_list(char *imageListFileName, on_image_ptr_t image_callback, img_scan_callback_args_t *args);

#endif // IMAGELISTREADER_H