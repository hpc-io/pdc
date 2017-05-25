#include <stdio.h>
#include <stdlib.h>

int main(int argc, const char *argv[])
{
    int nfiles, nperfile;
    int i, j, k;
    char filename[128];
    char obj_name[128];
    char loc[128];

    if (argc < 3) {
        printf("Usage: ./%s nfiles nperfile\n", argv[0]);
        goto done;
    }

    nfiles = atoi(argv[1]);
    nperfile = atoi(argv[2]);

    printf("Going to generate %d files, each with %d records\n", nfiles, nperfile);
    FILE *file;

    for (i = 0; i < nfiles; i++) {
        sprintf(filename, "./boss_metadata_%d.csv", i, nfiles*nperfile);
        file = fopen(filename, "w");
        if (NULL == file) {
            fprintf(stderr, "Error opening file: %s\n", filename);
            goto done;
        }

        k = 0;
            fprintf(file, "uid, app_name, obj_name, oid, create_time, modified_time, tags, location, ndim, dims0, dims1\n");
        for (j = 0; j < nperfile; j++) {
            sprintf(obj_name, "%d-%d-%d", i, j, j);
            sprintf(loc, "%s/%d/%d", "/global/projecta/projectdirs/sdss/dr12/boss/spectro/redux/v5_7_0", i, j);
            fprintf(file, "%d, %s, %s, %d, %d, %d, %d, %s, %d, %d, %d\n",
                 1234, "BOSS", obj_name, 1234, 20170509, 20170509, i+j*nfiles, loc, 2, 1000, 8);
        }

        fclose(file);
    }

done:
    return 0;
}
