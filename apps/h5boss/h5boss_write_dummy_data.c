#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#define NDSET 25304803
#define NAMELEN 48

void print_usage() {
    printf("Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt n_dsets(optional)\n");
}

int main(int argc, char *argv[])
{
    int size, rank;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif

    char *in_filename;
    char **all_dset_names = NULL, *all_dset_names_1d = NULL;
    char **my_dset_names = NULL, *my_dset_names_1d = NULL;
    char tmp_input[256];
    int *all_dset_sizes = NULL;
    int *my_dset_sizes = NULL;
    int i, n_write = 0, my_n_write = 0;
    
    in_filename = argv[1];
    if (argc < 2 || in_filename == NULL ) {
        print_usage();
        exit(-1);
    }

    if (argc == 3) {
        n_write = atoi(argv[2]);
        if (rank == 0) 
            printf("Writing %d datasets.\n", n_write);
    }


    // Rank 0 read from input file
    if (rank == 0) {
        all_dset_names    = (char**)calloc(NDSET, sizeof(char*));
        all_dset_names_1d = (char*)calloc(NDSET*NAMELEN, sizeof(char));
        all_dset_sizes = (int*)calloc(NDSET, sizeof(int));
        for (i = 0; i < NDSET; i++) 
            all_dset_names[i] = all_dset_names_1d + i*NAMELEN;

        printf("Reading from %s\n", in_filename);
        FILE *pm_file = fopen(in_filename, "r");
        if (NULL == pm_file) {
            fprintf(stderr, "Error opening file: %s\n", in_filename);
            return (-1);
        }

        // /3523/55144/1/coadd, 1D: 4619, 147808
        i = 0;
        char dset_ndim[4];
        int  dset_nelem;
        while (fgets(tmp_input, 255, pm_file) != NULL ) {
            sscanf(tmp_input, "%[^,], %[^:]: %d, %d", 
                               all_dset_names[i], dset_ndim, &dset_nelem, &all_dset_sizes[i]);
            i++;
            if (strcmp(dset_ndim, "1D") != 0) {
                printf("Dimension is %s!\n", dset_ndim);
                continue;
            }
            if (n_write > 0 && i >= n_write) 
                break;
        }

        fclose(pm_file);
        n_write = i;

        /* for (i = 0; i < n_write; i++) { */
        /*     printf("[%s]: %d\n", all_dset_names[i], all_dset_sizes[i]); */
        /* } */
    }

    fflush(stdout);
    my_n_write  = n_write;
    my_dset_names = all_dset_names;
    

#ifdef ENABLE_MPI
    MPI_Bcast( &n_write, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    // Distribute work evenly
    my_n_write = n_write / size;

    // Last rank may have extra work
    if (rank == size - 1) {
        my_n_write += n_write % size;
    }

    /* printf("%d: my_n_write is %d\n", rank, my_n_write); */
    /* fflush(stdout); */

    my_dset_names    = (char**)calloc(my_n_write, sizeof(char*));
    my_dset_names_1d = (char*)calloc(my_n_write*NAMELEN, sizeof(char));
    my_dset_sizes    = (int*)calloc(my_n_write, sizeof(int));

    int *sendn_write = (int*)calloc(sizeof(int), size);
    int *sendn_write_size = (int*)calloc(sizeof(int), size);
    int *displs = (int*)calloc(sizeof(int), size);
    int *displs_size  = (int*)calloc(sizeof(int), size);
    for (i = 0; i < size; i++) {
        sendn_write[i] = (n_write / size) * NAMELEN;
        sendn_write_size[i] = (n_write / size);
        if (i == size-1) {
            sendn_write[i]      += (n_write % size) * NAMELEN;
            sendn_write_size[i] += (n_write % size);
        }
        displs[i] = i * (n_write / size) * NAMELEN;
        displs_size[i] = i * (n_write / size);
        /* if (rank == 0) */ 
        /*     printf("n_write[%d]=%d, displs[%d]=%d\n", i, sendn_write[i], i, displs[i]); */
    }
    
    /* printf("%d: all_dset_names_1d [%s]\n", rank, all_dset_names_1d+NAMELEN); */
    MPI_Scatterv(all_dset_names_1d, sendn_write, displs, MPI_CHAR, 
                 my_dset_names_1d, my_n_write*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);

    MPI_Scatterv(all_dset_sizes, sendn_write_size, displs_size, MPI_INT, 
                 my_dset_sizes, my_n_write, MPI_INT, 0, MPI_COMM_WORLD);

    for (i = 0; i < my_n_write; i++) 
        my_dset_names[i] = my_dset_names_1d + i*NAMELEN;
    free(displs);
    free(sendn_write);
    free(displs_size);
    free(sendn_write_size);
#else

    my_n_write = n_write;
    my_dset_names = all_dset_names;
    my_dset_sizes = all_dset_sizes;
#endif 

    /* for (i = 0; i < my_n_write; i++) { */
    /*     printf("Rank %d - [%s]: %d\n", rank, my_dset_names[i], my_dset_sizes[i]); */
    /* } */

    pdcid_t pdc_id, cont_id, obj_prop, cont_prop, obj_id;
    uint64_t float_dims[1] = {1000};
    uint64_t offset[1], len[1];
    char cont_name[128]={0}, prev_cont_name[128]={0}, tags[128]={0};
    sprintf(tags, "%s", "tags0=1");
    struct PDC_region_info obj_region;
    char *buf;
    int prev_buf_size;

    pdc_id    = PDC_init("pdc");
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    obj_prop  = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop, 1, float_dims);
    PDCprop_set_obj_type(obj_prop, PDC_FLOAT);
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_user_id(obj_prop, 12345);
    PDCprop_set_obj_app_name(obj_prop, "H5BOSS");
    PDCprop_set_obj_tags(obj_prop, tags);

    strncpy(cont_name, my_dset_names[0], 10);
    cont_name[10] = 0;
    strcpy(prev_cont_name, cont_name);
    cont_id = PDCcont_create(cont_name, cont_prop);
    /* printf("Rank %d - Created container [%s]\n", rank, cont_name); */

    for (i = 0; i < my_n_write; i++) {
        strncpy(cont_name, my_dset_names[0], 10);
        cont_name[10] = 0;
        if (strcmp(cont_name, prev_cont_name) != 0) {
            cont_id = PDCcont_create(cont_name, cont_prop);
            /* printf("Rank %d - Created container [%s]\n", rank, cont_name); */
        }
        strcpy(prev_cont_name, cont_name);

        obj_region.ndim   = 1;
        offset[0] = 0;
        len[0] = my_dset_sizes[i];
        obj_region.offset = offset;
        obj_region.size   = len;

        obj_id = PDCobj_create(cont_id, my_dset_names[i], obj_prop);
        if (obj_id <= 0) {    
            printf("Error getting an object %s from server, exit...\n", my_dset_names[i]);
            continue;
        }
        /* printf("Rank %d - create object [%s]: %d\n", rank, my_dset_names[i], my_dset_sizes[i]); */

        if (buf == NULL) {
            buf = (char*)calloc(sizeof(char), my_dset_sizes[i]);
            prev_buf_size = my_dset_sizes[i];
        }
        else {
            if (prev_buf_size < my_dset_sizes[i]) {
                buf = realloc(buf, my_dset_sizes[i]);
            }
        }
        buf[0] = my_dset_names[i][1]; 
        buf[1] = my_dset_names[i][2]; 
        buf[2] = my_dset_names[i][3]; 
        buf[3] = my_dset_names[i][4]; 

        PDC_Client_write_id(obj_id, &obj_region, buf);
        /* printf("Rank %d - written object [%s]: %d\n", rank, my_dset_names[i], my_dset_sizes[i]); */
        fflush(stdout);
        if (i != 0 && i % 100 == 0) {
            printf("Rank %d - finished written 100 objects, total %d\n", rank, i); 
            fflush(stdout);
        }
    }

    printf("Rank %d - finished written all %d objects\n", rank, i); 
    fflush(stdout);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
