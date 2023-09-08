#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

// #define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "hdf5.h"
#include "pdc.h"
// #include "pdc_client_server_common.h"
// #include "pdc_client_connect.h"

#define MAX_NAME         1024
#define MAX_FILES        2500
#define MAX_FILENAME_LEN 64
#define MAX_TAG_SIZE     8192
#define TAG_LEN_MAX      2048

typedef struct ArrayList {
    int    length;
    int    capacity;
    char **items;
} ArrayList;

ArrayList *
newList(void)
{
    char **    items = malloc(4 * sizeof(char *));
    ArrayList *list  = malloc(sizeof(ArrayList));
    list->length     = 0;
    list->capacity   = 4;
    list->items      = items;
    return list;
}

// Check and expand list if needed
void
check(ArrayList *list)
{
    if (list->length >= list->capacity) {
        list->capacity = list->capacity * 2;
        list->items    = realloc(list->items, list->capacity * sizeof(char *));
        if (list->items == NULL) {
            exit(1);
        }
    }
}

void
add(ArrayList *list, const char *s)
{
    check(list);
    list->items[list->length] = malloc(strlen(s) + 1);
    strcpy(list->items[list->length], s);
    list->length++;
}

pdc_var_type_t do_dtype(hid_t, hid_t, int);
void           do_dset(hid_t did, char *name, char *app_name);
void           do_link(hid_t, char *);
void           scan_group(hid_t, int, char *);
void           do_attr(hid_t, pdcid_t);
void           scan_attrs(hid_t, pdcid_t);
void           do_plist(hid_t);

void
print_usage()
{
    printf("Usage: srun -n 2443 ./h5boss_v2_import h5boss_filenames\n");
}

int     rank = 0, size = 1;
char    tags_g[MAX_TAG_SIZE];
char *  tags_ptr_g;
char    dset_name_g[TAG_LEN_MAX];
hsize_t tag_size_g;
int     ndset_g = 0;
/* FILE *summary_fp_g; */
int               max_tag_size_g = 0;
pdcid_t           pdc_id_g = 0, cont_prop_g = 0, cont_id_g = 0, obj_prop_g = 0;
struct timespec   write_timer_start_g, write_timer_end_g;
struct ArrayList *container_names;
int               overwrite = 0;

int
main(int argc, char **argv)
{
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    hid_t  file;
    hid_t  grp;
    herr_t status;

    int   i, my_count, total_count;
    char *filename;
    char *app_name = "PDC_IMPORT";
    char  all_filenames[MAX_FILES][MAX_FILENAME_LEN];
    char  my_filenames[MAX_FILES][MAX_FILENAME_LEN];
    int   send_counts[MAX_FILES];
    int   displs[MAX_FILES];
    int   total_dset = 0;
    container_names  = newList();
    /* char* summary_fname = "/global/cscratch1/sd/houhun/tag_size_summary.csv"; */

    /* summary_fp_g = fopen(summary_fname, "a+"); */

    // create a pdc
    pdc_id_g = PDCinit("pdc");

    cont_prop_g = PDCprop_create(PDC_CONT_CREATE, pdc_id_g);
    if (cont_prop_g <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    obj_prop_g = PDCprop_create(PDC_OBJ_CREATE, pdc_id_g);

    uint64_t float_dims[1] = {1000};

    PDCprop_set_obj_dims(obj_prop_g, 1, float_dims);
    PDCprop_set_obj_type(obj_prop_g, PDC_FLOAT);
    PDCprop_set_obj_time_step(obj_prop_g, 0);
    PDCprop_set_obj_user_id(obj_prop_g, getuid());
    PDCprop_set_obj_app_name(obj_prop_g, "H5BOSS");

    if (argc < 2) {
        if (rank == 0)
            print_usage();
    }
    else {

        // Rank 0 reads the filename list and distribute data to other ranks
        if (rank == 0) {

            FILE *filenames_fp = fopen(argv[1], "r");
            if (filenames_fp == NULL) {
                printf("Input file open error [%s]\n", argv[1]);
                total_count = 0;
            }
            else {
                i = 0;
                while (fgets(all_filenames[i], MAX_FILENAME_LEN, filenames_fp)) {
                    /* printf("%s\n", all_filenames[i]); */
                    // Remove the trailing '\n'
                    int len                   = strlen(all_filenames[i]);
                    all_filenames[i][len - 1] = 0;
                    i++;
                }
                total_count = i;
                fclose(filenames_fp);

                printf("Running with %d clients, %d files\n", size, total_count);
                fflush(stdout);
            }
        }
        if (argc > 2) {
            int cur_arg_idx = 2;
            while (cur_arg_idx < argc) {
                if (strcmp(argv[cur_arg_idx], "-a") == 0) {
                    if (argc == cur_arg_idx + 1) {
                        printf("No app name given, defaulting to PDC_IMPORT");
                        cur_arg_idx += 1;
                    }
                    else {
                        app_name = argv[cur_arg_idx + 1];
                        cur_arg_idx += 2;
                    }
                }
                else if (strcmp(argv[cur_arg_idx], "-o") == 0) {
                    overwrite = 1;
                    cur_arg_idx += 1;
                }
            }
        }

#ifdef ENABLE_MPI
        MPI_Bcast(&total_count, 1, MPI_INT, 0, MPI_COMM_WORLD);
#endif

        if (total_count < size) {
            printf("More MPI ranks than total number of files, exiting...\n");
            goto done;
        }

        my_count = total_count / size;
        for (i = 0; i < size; i++) {
            send_counts[i] = my_count * MAX_FILENAME_LEN;
            displs[i]      = i * send_counts[i];
        }

        // Last rank takes care of leftovers
        if (rank == size - 1) {
            my_count += total_count % size;
        }
        send_counts[size - 1] += (total_count % size * MAX_FILENAME_LEN);

#ifdef ENABLE_MPI
        // Distribute the data
        MPI_Scatterv(&all_filenames[0][0], send_counts, displs, MPI_CHAR, &my_filenames[0][0],
                     my_count * MAX_FILENAME_LEN, MPI_CHAR, 0, MPI_COMM_WORLD);
#else
        memcpy(&my_filenames[0][0], &all_filenames[0][0], MAX_FILES * MAX_FILENAME_LEN);

#endif

        printf("Importer%2d: I will import %d files\n", rank, my_count);
        for (i = 0; i < my_count; i++)
            printf("Importer%2d: [%s] \n", rank, my_filenames[i]);
        fflush(stdout);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        struct timespec pdc_timer_start, pdc_timer_end;
        clock_gettime(CLOCK_MONOTONIC, &pdc_timer_start);

        for (i = 0; i < my_count; i++) {
            filename = my_filenames[i];
            printf("Importer%2d: processing [%s]\n", rank, my_filenames[i]);
            fflush(stdout);
            file = H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT);
            if (file < 0) {
                /* status = H5Fclose(file); */
                printf("Failed to open file [%s]\n", filename);
                fflush(stdout);
                continue;
            }

            grp = H5Gopen(file, "/", H5P_DEFAULT);
            scan_group(grp, 0, app_name);

            status = H5Fclose(file);

#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
#endif
            // Checkpoint all metadata after import each hdf5 file
            if (rank == 0) {
                // FIXME: this should be replaced by a function in public headers.
                // PDC_Client_all_server_checkpoint();
            }
            /* printf("%s, %d\n", filename, max_tag_size_g); */
            /* printf("\n\n======================\nNumber of datasets: %d\n", ndset_g); */
            /* #ifdef ENABLE_MPI */
            /*             MPI_Barrier(MPI_COMM_WORLD); */
            /* #endif */
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        clock_gettime(CLOCK_MONOTONIC, &pdc_timer_end);
        double write_time =
            (pdc_timer_end.tv_sec - pdc_timer_start.tv_sec) * 1e9 +
            (pdc_timer_end.tv_nsec - pdc_timer_start.tv_nsec); // calculate duration in nanoseconds

#ifdef ENABLE_MPI
        MPI_Reduce(&ndset_g, &total_dset, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
        total_dset = ndset_g;
#endif
        if (rank == 0) {
            printf("Import %d datasets with %d ranks took %.2f seconds.\n", total_dset, size,
                   write_time / 1e9);
        }
    }

    /* fclose(summary_fp_g); */

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}

/*
 * Process a group and all it's members
 *
 *   This can be used as a model to implement different actions and
 *   searches.
 */

void
scan_group(hid_t gid, int level, char *app_name)
{
    int     i;
    ssize_t len;
    hsize_t nobj;
    herr_t  err;
    int     otype;
    hid_t   grpid, typeid, dsid;
    char    group_name[MAX_NAME];
    char    memb_name[MAX_NAME];
    int     create_cont;

    /*
     * Information about the group:
     *  Name and attributes
     *
     *  Other info., not shown here: number of links, object id
     */
    len = H5Iget_name(gid, group_name, MAX_NAME);

    /* printf("Group Name: %s\n",group_name); */

    /*
     *  process the attributes of the group, if any.
     */
    // scan_attrs(gid);

    /*
     *  Get all the members of the groups, one at a time.
     */
    err = H5Gget_num_objs(gid, &nobj);
    for (i = 0; i < nobj; i++) {
        /*
         *  For each object in the group, get the name and
         *   what type of object it is.
         */
        /* printf("  Member: #%d ",i);fflush(stdout); */
        len = H5Gget_objname_by_idx(gid, (hsize_t)i, memb_name, (size_t)MAX_NAME);
        /* printf("   len=%d ",len);fflush(stdout); */
        /* printf("  Member: \"%s\" ",memb_name);fflush(stdout); */
        otype = H5Gget_objtype_by_idx(gid, (size_t)i);

        /*
         * process each object according to its type
         */
        switch (otype) {
            case H5G_LINK:
                /* printf(" SYM_LINK:\n"); */
                // do_link(gid,memb_name);
                break;
            case H5G_GROUP:
                /* printf(" GROUP:\n"); */
                grpid = H5Gopen(gid, memb_name, H5P_DEFAULT);
                scan_group(grpid, level + 1, app_name);
                H5Gclose(grpid);
                break;
            case H5G_DATASET:
                /* printf(" DATASET:\n"); */
                // create container - check if container exists first
                //  create a container
                create_cont = 1;
                for (int ctn = 0; ctn < container_names->length; ctn++) {
                    if (strcmp(container_names->items[ctn], group_name) == 0) {
                        create_cont = 0;
                        break;
                    }
                }
                if (create_cont) {
                    cont_id_g = PDCcont_create(group_name, cont_prop_g);
                    if (cont_id_g <= 0)
                        printf("Fail to create container @ line  %d!\n", __LINE__);
                    printf("Importer%2d: Created container [%s]\n", rank, group_name);
                    add(container_names, group_name);
                }
                dsid = H5Dopen(gid, memb_name, H5P_DEFAULT);
                do_dset(dsid, memb_name, app_name);
                H5Dclose(dsid);
                break;
            case H5G_TYPE:
                /* printf(" DATA TYPE:\n"); */
                typeid = H5Topen(gid, memb_name, H5P_DEFAULT);
                do_dtype(typeid, gid, 0);
                H5Tclose(typeid);
                break;
            default:
                printf(" Unknown Object Type!\n");
                break;
        }
    }
}

/*
 *  Retrieve information about a dataset.
 *
 *  Many other possible actions.
 *
 *  This example does not read the data of the dataset.
 */
void
do_dset(hid_t did, char *name, char *app_name)
{
    hid_t                  tid, pid, sid, dspace;
    hsize_t                dtype_size, dset_size;
    char                   ds_name[MAX_NAME];
    char                   grp_name[MAX_NAME];
    char *                 obj_name;
    int                    name_len, i;
    hsize_t                ndim, dims[10];
    uint64_t               region_offset[10], region_size[10];
    void *                 buf;
    struct pdc_region_info obj_region;

    tag_size_g = 0;
    memset(tags_g, 0, sizeof(char) * TAG_LEN_MAX);
    tags_ptr_g = tags_g;
    /*
     * Information about the group:
     *  Name and attributes
     *
     *  Other info., not shown here: number of links, object id
     */
    H5Iget_name(did, ds_name, TAG_LEN_MAX);
    memset(dset_name_g, 0, TAG_LEN_MAX);
    strcpy(dset_name_g, ds_name);

    // dset_name_g has the full path to the dataset, e.g. /group/subgroup/dset_name
    // substract the actual dset name with following
    name_len = strlen(ds_name);
    for (i = name_len; i >= 0; i--) {
        if (ds_name[i] == '/') {
            obj_name = &ds_name[i + 1];
            break;
        }
    }
    /* printf("[%s] {\n", obj_name); */

    /* fprintf(summary_fp_g, "%s, ", ds_name); */

    /*
     * Get dataset information: dataspace, data type
     */
    sid = H5Dget_space(did); /* the dimensions of the dataset (not shown) */
    tid = H5Dget_type(did);

    pdcid_t cur_obj_prop_g = PDCprop_create(PDC_OBJ_CREATE, pdc_id_g);

    pdc_var_type_t cur_type = do_dtype(tid, did, 0);
    if (cur_type == PDC_UNKNOWN) {
        cur_type = PDC_FLOAT;
    }

    const int cur_ndims = H5Sget_simple_extent_ndims(sid);
    hsize_t   cur_dims[cur_ndims];
    H5Sget_simple_extent_dims(sid, cur_dims, NULL);

    PDCprop_set_obj_dims(obj_prop_g, cur_ndims, (uint64_t *)cur_dims);
    PDCprop_set_obj_type(obj_prop_g, cur_type);
    PDCprop_set_obj_time_step(obj_prop_g, 0);
    PDCprop_set_obj_user_id(obj_prop_g, getuid());
    PDCprop_set_obj_app_name(obj_prop_g, app_name);

    pdcid_t obj_id = PDCobj_open(ds_name, pdc_id_g);
    // FIXME: where is the declaration of 'check'?
    if (check > 0) {
        if (!overwrite) {
            return;
        }
    }
    else {
        obj_id = PDCobj_create(cont_id_g, ds_name, obj_prop_g);
    }
    if (obj_id <= 0) {
        printf("Error getting an object %s from server, exit...\n", dset_name_g);
    }

    do_dtype(tid, did, 0);

    ndset_g++;
    /* if (ndset_g > 10) { */
    /*     return; */
    /* } */
    /*
     *  process the attributes of the dataset, if any.
     */
    // scan_attrs(did);

    /*
     * Retrieve and analyse the dataset properties
     */
    /* pid = H5Dget_create_plist(did); /1* get creation property list *1/ */
    /* do_plist(pid); */
    /* size = H5Dget_storage_size(did); */
    /* printf("Total space currently written in file: %d\n",(int)size); */

    /*
     * The datatype and dataspace can be used to read all or
     * part of the data.  (Not shown in this example.)
     */

    /* ... read data with H5Dread, write with H5Dwrite, etc. */
    dspace = H5Dget_space(did);
    ndim   = H5Sget_simple_extent_ndims(dspace);
    H5Sget_simple_extent_dims(dspace, dims, NULL);
    dtype_size = H5Tget_size(tid);
    dset_size  = dtype_size;
    for (i = 0; i < ndim; i++)
        dset_size *= dims[i];

    buf = malloc(dset_size);
    H5Dread(did, tid, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf);

    /* H5Pclose(pid); */
    H5Tclose(tid);
    H5Sclose(sid);

    // Create a pdc object per dataset with tag
    // Currently there is a bug in Mercury that causes issues when send size is larger than 4KB
    // so we temporarily just cut the tags longer than 2048

    scan_attrs(did, obj_id);

    // pdc_metadata_t *meta = NULL;
    obj_region.ndim = ndim;
    for (i = 0; i < ndim; i++) {
        region_offset[i] = 0;
        region_size[i]   = dims[i];
    }
    region_size[0] *= dtype_size;
    obj_region.offset = region_offset;
    obj_region.size   = region_size;

    if (ndset_g == 1)
        clock_gettime(CLOCK_MONOTONIC, &write_timer_start_g);

    /* PDC_Client_query_metadata_name_timestep(dset_name_g, 0, &meta); */
    /* if (meta == NULL) */
    /*     printf("Error with obtainig metadata, skipping PDC write\n"); */
    /* else */
    /*     PDC_Client_write(meta, &obj_region, buf); */

    pdcid_t local_region     = PDCregion_create(ndim, region_offset, region_size);
    pdcid_t remote_region    = PDCregion_create(ndim, region_offset, region_size);
    pdcid_t transfer_request = PDCregion_transfer_create(buf, PDC_WRITE, obj_id, local_region, remote_region);
    PDCregion_transfer_start(transfer_request);
    PDCregion_transfer_wait(transfer_request);

    // PDC_Client_write_id(obj_id, &obj_region, buf);
    if (ndset_g % 100 == 0) {
        clock_gettime(CLOCK_MONOTONIC, &write_timer_end_g);
        double elapsed_time =
            (write_timer_end_g.tv_sec - write_timer_start_g.tv_sec) * 1e9 +
            (write_timer_end_g.tv_nsec - write_timer_start_g.tv_nsec); // calculate duration in nanoseconds;
        printf("Importer%2d: Finished written 100 objects, took %.2f, my total %d\n", rank,
               elapsed_time / 1e9, ndset_g);
        fflush(stdout);
        clock_gettime(CLOCK_MONOTONIC, &write_timer_start_g);
    }

    free(buf);
    /* printf("} [%s] tag_size %d  \n========================\n%s\n========================\n\n\n", */
    /*         obj_name, tag_size_g, tags_g); */
    /* printf("size %d\n%s\n\n", tag_size_g, tags_g); */
    /* fprintf(summary_fp_g, "%d\n", tag_size_g); */
    if (tag_size_g > max_tag_size_g) {
        max_tag_size_g = tag_size_g;
    }
    tag_size_g = 0;
}

/*
 *  Analyze a data type description
 */
pdc_var_type_t
do_dtype(hid_t tid, hid_t oid, int is_compound)
{

    herr_t      status;
    int         compound_nmember, i;
    hsize_t     dims[8], ndim;
    char *      mem_name;
    char *      attr_string[100], new_string[TAG_LEN_MAX], tmp_str[TAG_LEN_MAX];
    hsize_t     attr_size, attr_len;
    hid_t       mem_type;
    hid_t       atype, aspace, naive_type;
    H5T_class_t t_class, compound_class;
    t_class = H5Tget_class(tid);
    if (t_class < 0) {
        /* puts("   Invalid datatype.\n"); */
    }
    else {
        attr_size = H5Tget_size(tid);
        /* printf("    Datasize %3d, type", attr_size); */
        /*
         * Each class has specific properties that can be
         * retrieved, e.g., attr_size, byte order, exponent, etc.
         */
        if (t_class == H5T_INTEGER) {
            /* puts(" 'H5T_INTEGER'."); */
            return PDC_INT;

            /* display size, signed, endianess, etc. */
        }
        else if (t_class == H5T_FLOAT) {
            /* puts(" 'H5T_FLOAT'."); */
            return PDC_FLOAT;
        }
        else if (t_class == H5T_STRING) {
            /* puts(" 'H5T_STRING'."); */
            return PDC_CHAR;
        }
        else if (t_class == H5T_COMPOUND) {
            // For compound type, the size would be calculated by its sub-types
            printf("PDC does not support compound data type yet.\n");
            return PDC_UNKNOWN;
        }
        else if (t_class == H5T_ARRAY) {
            if (is_compound == 0) {
                tag_size_g += attr_size;
            }
            ndim = H5Tget_array_ndims(tid);
            H5Tget_array_dims2(tid, dims);
            /* printf(" 'H5T_ARRAY', ndim=%d:  ", ndim); */

            /* printf("\n                                                "); */
            pdc_var_type_t type = do_dtype(H5Tget_super(tid), oid, 1);
            return type;
            /* display  dimensions, base type  */
        }
        else if (t_class == H5T_ENUM) {
            /* puts(" 'H5T_ENUM'."); */
            return PDC_INT;
        }
        else {
            printf("PDC does not support this data type yet.\n");
            return PDC_UNKNOWN;
        }
    }
    return PDC_UNKNOWN;
}

/*
 *  Analyze a symbolic link
 *
 * The main thing you can do with a link is find out
 * what it points to.
 */
void
do_link(hid_t gid, char *name)
{
    herr_t status;
    char   target[MAX_NAME];

    status = H5Gget_linkval(gid, name, MAX_NAME, target);
    /* printf("Symlink: %s points to: %s\n", name, target); */
}

/*
 *  Run through all the attributes of a dataset or group.
 *  This is similar to iterating through a group.
 */
void
scan_attrs(hid_t oid, pdcid_t obj_id)
{
    int   na;
    hid_t aid;
    int   i;

    na = H5Aget_num_attrs(oid);

    for (i = 0; i < na; i++) {
        aid = H5Aopen_idx(oid, (unsigned int)i);
        do_attr(aid, obj_id);
        H5Aclose(aid);
    }
}

/*
 *  Process one attribute.
 *  This is similar to the information about a dataset.
 */
void
do_attr(hid_t aid, pdcid_t obj_id)
{
    ssize_t len;
    hid_t   atype;
    hid_t   aspace;
    char    buf[MAX_NAME]         = {0};
    char    read_buf[TAG_LEN_MAX] = {0};
    // pdc_kvtag_t kvtag1;
    char *         tag_name;
    void *         tag_value;
    pdc_var_type_t value_type;
    size_t         tag_size;

    /*
     * Get the name of the attribute.
     */
    len = H5Aget_name(aid, MAX_NAME, buf);
    /* printf("    Attribute Name : %s\n",buf); */

    // Skip the COMMENT attribute
    if (strcmp("COMMENT", buf) == 0 || strcmp("comments", buf) == 0)
        return;

    atype = H5Aget_type(aid);
    H5Aread(aid, atype, read_buf);
    tag_name  = buf;
    tag_value = (void *)read_buf;
    if (atype == H5T_STRING) {
        tag_size = strlen(read_buf) + 1;
    }
    else {
        tag_size = H5Tget_size(atype);
    }
    PDCobj_put_tag(obj_id, tag_name, tag_value, value_type, tag_size);

    /*
     * Get attribute information: dataspace, data type
     */
    aspace = H5Aget_space(aid); /* the dimensions of the attribute data */

    atype = H5Aget_type(aid);
    do_dtype(atype, aid, 0);

    /*
     * The datatype and dataspace can be used to read all or
     * part of the data.  (Not shown in this example.)
     */

    /* ... read data with H5Aread, write with H5Awrite, etc. */

    H5Tclose(atype);
    H5Sclose(aspace);
}

/*
 *   Example of information that can be read from a Dataset Creation
 *   Property List.
 *
 *   There are many other possibilities, and there are other property
 *   lists.
 */
void
do_plist(hid_t pid)
{
    hsize_t          chunk_dims_out[2];
    int              rank_chunk;
    int              nfilters;
    H5Z_filter_t     filtn;
    int              i;
    unsigned int     filt_flags, filt_conf;
    size_t           cd_nelmts;
    unsigned int     cd_values[32];
    char             f_name[MAX_NAME];
    H5D_fill_time_t  ft;
    H5D_alloc_time_t at;
    H5D_fill_value_t fvstatus;
    unsigned int     szip_options_mask;
    unsigned int     szip_pixels_per_block;

    /* zillions of things might be on the plist */
    /*  here are a few... */

    /*
     * get chunking information: rank and dimensions.
     *
     *  For other layouts, would get the relevant information.
     */
    /* if(H5D_CHUNKED == H5Pget_layout(pid)){ */
    /* 	rank_chunk = H5Pget_chunk(pid, 2, chunk_dims_out); */
    /* 	printf("chunk rank %d, dimensions %lu x %lu\n", rank_chunk, */
    /* 	   (unsigned long)(chunk_dims_out[0]), */
    /* 	   (unsigned long)(chunk_dims_out[1])); */
    /* } /1* else if contiguous, etc. *1/ */

    /*
     *  Get optional filters, if any.
     *
     *  This include optional checksum and compression methods.
     */

    nfilters = H5Pget_nfilters(pid);
    for (i = 0; i < nfilters; i++) {
        /* For each filter, get
         *   filter ID
         *   filter specific parameters
         */
        cd_nelmts = 32;
        filtn = H5Pget_filter(pid, (unsigned)i, &filt_flags, &cd_nelmts, cd_values, (size_t)MAX_NAME, f_name,
                              &filt_conf);
        /*
         *  These are the predefined filters
         */
        switch (filtn) {
            case H5Z_FILTER_DEFLATE: /* AKA GZIP compression */
                /* printf("DEFLATE level = %d\n", cd_values[0]); */
                break;
            case H5Z_FILTER_SHUFFLE:
                /* printf("SHUFFLE\n"); /1* no parms *1/ */
                break;
            case H5Z_FILTER_FLETCHER32:
                /* printf("FLETCHER32\n"); /1* Error Detection Code *1/ */
                break;
            case H5Z_FILTER_SZIP:
                szip_options_mask = cd_values[0];
                ;
                szip_pixels_per_block = cd_values[1];

                /* printf("SZIP COMPRESSION: "); */
                /* printf("PIXELS_PER_BLOCK %d\n", */
                /* szip_pixels_per_block); */
                /* print SZIP options mask, etc. */
                break;
            default:
                /* printf("UNKNOWN_FILTER\n" ); */
                break;
        }
    }

    /*
     *  Get the fill value information:
     *    - when to allocate space on disk
     *    - when to fill on disk
     *    - value to fill, if any
     */

    /* printf("ALLOC_TIME "); */
    H5Pget_alloc_time(pid, &at);

    switch (at) {
        case H5D_ALLOC_TIME_EARLY:
            /* printf("EARLY\n"); */
            break;
        case H5D_ALLOC_TIME_INCR:
            /* printf("INCR\n"); */
            break;
        case H5D_ALLOC_TIME_LATE:
            /* printf("LATE\n"); */
            break;
        default:
            /* printf("unknown allocation policy"); */
            break;
    }

    /* printf("FILL_TIME: "); */
    H5Pget_fill_time(pid, &ft);
    switch (ft) {
        case H5D_FILL_TIME_ALLOC:
            /* printf("ALLOC\n"); */
            break;
        case H5D_FILL_TIME_NEVER:
            /* printf("NEVER\n"); */
            break;
        case H5D_FILL_TIME_IFSET:
            /* printf("IFSET\n"); */
            break;
        default:
            printf("?\n");
            break;
    }

    H5Pfill_value_defined(pid, &fvstatus);

    if (fvstatus == H5D_FILL_VALUE_UNDEFINED) {
        /* printf("No fill value defined, will use default\n"); */
    }
    else {
        /* Read  the fill value with H5Pget_fill_value.
         * Fill value is the same data type as the dataset.
         * (details not shown)
         **/
    }

    /* ... and so on for other dataset properties ... */
}
