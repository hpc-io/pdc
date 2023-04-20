#include "imageListReader.h"
#include "parallelReadTiff.h"

void
scan_image_list(char *imageListFileName, on_image_ptr_t image_callback)
{
    FILE* file = fopen(imageListFileName, "r");

    if (file == NULL) {
        printf("Error: could not open file %s\n", imageListFileName);
        return;
    }

    char buffer[1024];
    char* token;
    int line_count = 0;
    image_file_info_t image_info;

    // Read and discard the first line (the header)
    fgets(buffer, sizeof(buffer), file);
    line_count++;

    // Read the remaining lines of the file
    while (fgets(buffer, sizeof(buffer), file)) {
        line_count++;

        // Tokenize the line by comma
        token = strtok(buffer, ",");

        // Extract the filepath
        image_info.filepath = strdup(token);

        // Extract the filename
        token = strtok(NULL, ",");
        image_info.filename = strdup(token);

        // Extract the stageX_um_
        token = strtok(NULL, ",");
        image_info.stageX_um_ = atof(token);

        // Extract the stageY_um_
        token = strtok(NULL, ",");
        image_info.stageY_um_ = atof(token);

        // Extract the stageZ_um_
        token = strtok(NULL, ",");
        image_info.stageZ_um_ = atof(token);

        // Extract the objectiveX_um_
        token = strtok(NULL, ",");
        image_info.objectiveX_um_ = atof(token);

        // Extract the objectiveY_um_
        token = strtok(NULL, ",");
        image_info.objectiveY_um_ = atof(token);

        // Extract the objectiveZ_um_
        token = strtok(NULL, ",");
        image_info.objectiveZ_um_ = atof(token);

        // Do something with the extracted image info...
        if (image_callback != NULL) {
            image_callback(&image_info);
        }
        
    }

    fclose(file);

    printf("Read %d lines from file %s\n", line_count, imageListFileName);
}