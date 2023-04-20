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

typedef void (*on_image_ptr_t)(image_file_info_t *);

void scan_image_list(char *imageListFileName, on_image_ptr_t image_callback);

#endif // IMAGELISTREADER_H