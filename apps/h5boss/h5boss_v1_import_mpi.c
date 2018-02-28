#include <stdio.h>
#include <stdlib.h>

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "hdf5.h"
#include "pdc.h"
#include "pdc_client_server_common.h"

#define MAX_NAME 1024
#define MAX_FILES 2500
#define MAX_FILENAME_LEN 64


void do_dtype(hid_t, hid_t, int);
void do_dset(hid_t did, char *name);
void do_link(hid_t, char *);
void scan_group(hid_t, int);
void do_attr(hid_t);
void scan_attrs(hid_t);
void do_plist(hid_t);

void print_usage() {
    printf("Usage: srun -n 2443 ./h5boss_v2_import h5boss_filenames\n");
}

char tags_g[TAG_LEN_MAX];
char *tags_ptr_g;
char dset_name_g[TAG_LEN_MAX];
hsize_t tag_size_g;
int  ndset_g = 0;
/* FILE *summary_fp_g; */
int max_tag_size_g = 0;
pdcid_t pdc_id_g = 0, cont_prop_g = 0, cont_id_g = 0, obj_prop_g = 0;

int add_tag(char *str)
{
    int str_len = 0;
    if (NULL == str || NULL == tags_ptr_g) {
        fprintf(stderr, "%s - input str is NULL!", __func__);
        return 0;
    }
    else if (tag_size_g + str_len >= TAG_LEN_MAX) {
        fprintf(stderr, "%s - tags_ptr_g overflow!", __func__);
        return 0;
    }

    // Remove the trailing ','
    if (*str == '}' || *str == ')' || *str == ']' ) {
        if (*(--tags_ptr_g) != ',') {
            tags_ptr_g++;
        }
    }

    str_len = strlen(str);
    strncpy(tags_ptr_g, str, str_len);

    tag_size_g += str_len;
    tags_ptr_g += str_len;

    return str_len;
}

int
main(int argc, char **argv)
{
    int rank = 0, size = 1;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    hid_t    file;
    hid_t    grp;
    herr_t   status;

    int i, my_count, total_count;
    char* filename;
    char all_filenames[MAX_FILES][MAX_FILENAME_LEN];
    char my_filenames[MAX_FILES][MAX_FILENAME_LEN];
    int  send_counts[MAX_FILES];
    int  displs[MAX_FILES];
    int total_dset = 0;
    /* char* summary_fname = "/global/cscratch1/sd/houhun/tag_size_summary.csv"; */

    /* summary_fp_g = fopen(summary_fname, "a+"); */

    // create a pdc
    pdc_id_g = PDC_init("pdc");

    cont_prop_g = PDCprop_create(PDC_CONT_CREATE, pdc_id_g);
    if(cont_prop_g <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    obj_prop_g = PDCprop_create(PDC_OBJ_CREATE, pdc_id_g);

    uint64_t float_dims[1] = {1000};

    PDCprop_set_obj_dims(obj_prop_g, 1, float_dims);
    PDCprop_set_obj_type(obj_prop_g, PDC_FLOAT);
    PDCprop_set_obj_time_step(obj_prop_g, 0);
    PDCprop_set_obj_user_id( obj_prop_g, getuid());
    PDCprop_set_obj_app_name(obj_prop_g, "H5BOSS");

    if (argc != 2) {
        if (rank == 0) 
            print_usage();
    }
    else {

        // Rank 0 reads the filename list and distribute data to other ranks
        if (rank == 0) {

            FILE *filenames_fp = fopen(argv[1], "r");
            i = 0;
            while (fgets(all_filenames[i], MAX_FILENAME_LEN, filenames_fp)) {
                /* printf("%s\n", all_filenames[i]); */
                // Remove the trailing '\n'
                int len = strlen(all_filenames[i]);
                all_filenames[i][len-1] = 0;
                i++;
            }
            total_count = i;
            fclose(filenames_fp);

            printf("Running with %d clients, %d files\n", size, total_count);
            fflush(stdout);
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
        send_counts[size-1] += (total_count % size * MAX_FILENAME_LEN);

#ifdef ENABLE_MPI
        // Distribute the data
        MPI_Scatterv(&all_filenames[0][0], send_counts, displs, MPI_CHAR, 
                     &my_filenames[0][0],  my_count*MAX_FILENAME_LEN, MPI_CHAR, 0, MPI_COMM_WORLD);
#else
        memcpy(&my_filenames[0][0], &all_filenames[0][0], MAX_FILES*MAX_FILENAME_LEN);
        
#endif


        /* for (i = 0; i < my_count; i++) { */
        /*     printf("%d: %d [%s] \n", rank, my_count, my_filenames[i]); */
        /* } */

        struct timeval  pdc_timer_start;
        struct timeval  pdc_timer_end;
        gettimeofday(&pdc_timer_start, 0);

        for (i = 0; i < my_count; i++) {
            filename = my_filenames[i];
            /* printf("%d: processing [%s]\n", rank, my_filenames[i]); */
            /* fflush(stdout); */
            file = H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT);
            if (file < 0) {
                status = H5Fclose(file);
                continue;
            }

            grp = H5Gopen(file,"/", H5P_DEFAULT);
            scan_group(grp, 0);

            status = H5Fclose(file);

            /* printf("%s, %d\n", filename, max_tag_size_g); */
            /* printf("\n\n======================\nNumber of datasets: %d\n", ndset_g); */
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        gettimeofday(&pdc_timer_end, 0);
        double write_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

#ifdef ENABLE_MPI
        MPI_Reduce(&ndset_g, &total_dset, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
        total_dset = ndset_g;
#endif
        if (rank == 0) {
            printf("Import %d datasets with %d ranks took %.2f seconds.\n", total_dset, size, write_time);
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
scan_group(hid_t gid, int level) {
    int i;
    ssize_t len;
    hsize_t nobj;
    herr_t err;
    int otype;
    hid_t grpid, typeid, dsid;
    char group_name[MAX_NAME];
    char memb_name[MAX_NAME];

    /*
     * Information about the group:
     *  Name and attributes
     *
     *  Other info., not shown here: number of links, object id
     */
    len = H5Iget_name (gid, group_name, MAX_NAME);
    if (level == 2) {
        // create a container
        cont_id_g = PDCcont_create(group_name, cont_prop_g);
        if(cont_id_g <= 0)
            printf("Fail to create container @ line  %d!\n", __LINE__);
        printf("Created container [%s], %" PRIu64 "\n", group_name, cont_id_g);
    }


    /* printf("Group Name: %s\n",group_name); */

    /*
     *  process the attributes of the group, if any.
     */
    scan_attrs(gid);

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
        len = H5Gget_objname_by_idx(gid, (hsize_t)i,
                                    memb_name, (size_t)MAX_NAME );
        /* printf("   len=%d ",len);fflush(stdout); */
        /* printf("  Member: \"%s\" ",memb_name);fflush(stdout); */
        otype =  H5Gget_objtype_by_idx(gid, (size_t)i );

        /*
         * process each object according to its type
         */
        switch(otype) {
        case H5G_LINK:
            /* printf(" SYM_LINK:\n"); */
            do_link(gid,memb_name);
            break;
        case H5G_GROUP:
            /* printf(" GROUP:\n"); */
            grpid = H5Gopen(gid,memb_name, H5P_DEFAULT);
            scan_group(grpid, level + 1);
            H5Gclose(grpid);
            break;
        case H5G_DATASET:
            /* printf(" DATASET:\n"); */
            dsid = H5Dopen(gid,memb_name, H5P_DEFAULT);
            do_dset(dsid, memb_name);
            H5Dclose(dsid);
            break;
        case H5G_TYPE:
            /* printf(" DATA TYPE:\n"); */
            typeid = H5Topen(gid,memb_name, H5P_DEFAULT);
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
do_dset(hid_t did, char *name)
{
    hid_t tid;
    hid_t pid;
    hid_t sid;
    hsize_t size;
    char ds_name[MAX_NAME];
    char grp_name[MAX_NAME];
    char *obj_name;
    int name_len, i;

    tag_size_g = 0;
    memset(tags_g, 0, sizeof(char)*TAG_LEN_MAX);
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
    /* add_tag(ds_name); */

    // dset_name_g has the full path to the dataset, e.g. /group/subgroup/dset_name
    // substract the actual dset name with following
    name_len = strlen(ds_name);
    for (i = name_len; i >= 0; i--) {
        if (ds_name[i] == '/') {
            obj_name = &ds_name[i+1];
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
    /* add_tag(",DT:"); */

    do_dtype(tid, did, 0);

    add_tag(",");

    ndset_g ++;
    /*
     *  process the attributes of the dataset, if any.
     */
    scan_attrs(did);

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

    /* H5Pclose(pid); */
    H5Tclose(tid);
    H5Sclose(sid);

    if (strcmp("/3673/55160/420/coadd", dset_name_g) == 0) {
        int t=1;
    }

    // Create a pdc object per dataset with tag
    PDCprop_set_obj_tags(obj_prop_g, tags_g);
    pdcid_t obj_id = PDCobj_create(cont_id_g, dset_name_g, obj_prop_g);
    if (obj_id <= 0) {    
        printf("Error getting an object %s from server, exit...\n", dset_name_g);
    }
    else {
        /* printf("%s\n", dset_name_g); */
        /* printf("created [%s] with tag size %d \n", dset_name_g, tag_size_g); */
        /* printf("created [%s] with tag size %d [%s]\n", dset_name_g, tag_size_g, tags_g); */
    }

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
void
do_dtype(hid_t tid, hid_t oid, int is_compound) {

    herr_t      status;
    int compound_nmember, i;
    hsize_t dims[8], ndim;
    char *mem_name;
    char *attr_string[100], new_string[TAG_LEN_MAX], tmp_str[TAG_LEN_MAX];
    hsize_t size, attr_len;
    hid_t mem_type;
    hid_t atype, aspace, naive_type;
    H5T_class_t t_class, compound_class;
    t_class = H5Tget_class(tid);
    if(t_class < 0) {
        /* puts("   Invalid datatype.\n"); */
    } else {
        size = H5Tget_size (tid);
        /* printf("    Datasize %3d, type", size); */
        /*
         * Each class has specific properties that can be
         * retrieved, e.g., size, byte order, exponent, etc.
         */
        if(t_class == H5T_INTEGER) {
            /* puts(" 'H5T_INTEGER'."); */

            if (1 == is_compound) {
                sprintf(tmp_str, "I%lu,", size);
                add_tag(tmp_str);
            }
            else {
                int attr_int;
                status = H5Aread (oid, tid, &attr_int);
                if (status != HG_SUCCESS) {
                    printf("==Error with H5Aread!\n");
                    printf("==[%s]\n", tags_g);
                }
                /* H5Aread (oid, H5T_NATIVE_INT, &attr_int); */
                sprintf(tmp_str,"%d,", attr_int);
                add_tag(tmp_str);
            }
            /* display size, signed, endianess, etc. */
        } else if(t_class == H5T_FLOAT) {
            /* puts(" 'H5T_FLOAT'."); */
            if (1 == is_compound) {
                sprintf(tmp_str, "F%lu,", size);
                add_tag(tmp_str);
            }
            else {
                double attr_float;
                status = H5Aread (oid, tid, &attr_float);
                if (status != HG_SUCCESS) {
                    printf("==Error with H5Aread!\n");
                    printf("==[%s]\n", tags_g);
                }
                /* H5Aread (oid, H5T_NATIVE_DOUBLE, &attr_float); */
                if (attr_float == 0) {
                    add_tag("0,");
                }
                else {
                    sprintf(tmp_str,"%.2f,", attr_float);
                    // Remove the trailing 0s to save space
                    for (i = strlen(tmp_str) - 2; i > 0; i--) {
                        if (tmp_str[i] == '0') {
                            tmp_str[i] = ',';
                            tmp_str[i+1] = 0;
                        }
                        else if (tmp_str[i] == '.') {
                            tmp_str[i] = ',';
                            tmp_str[i+1] = 0;
                            break;
                        }
                        else
                            break;
                    }

                    if (strlen(tmp_str) > 8) {
                        sprintf(tmp_str,"%.2E,", attr_float);
                    }
                    add_tag(tmp_str);
                }
            }
            /* display size, endianess, exponennt, etc. */
        } else if(t_class == H5T_STRING) {
            /* puts(" 'H5T_STRING'."); */

            // Only include the string in tag if it is an attribute,
            // not any strings in compound datatype
            if (is_compound == 0) {
                hsize_t totsize;
                aspace = H5Aget_space(oid);
                atype  = H5Aget_type(oid);
                ndim = H5Sget_simple_extent_ndims(aspace);
                H5Sget_simple_extent_dims(aspace, dims, NULL);
                // Deal with variable-length string
                memset(attr_string, 0, 100);
                if(H5Tis_variable_str(atype) != 1) {
                    H5Aread(oid, atype, &attr_string);
                }
                else {
                    naive_type = H5Tget_native_type(atype, H5T_DIR_ASCEND);
                    H5Aread(oid, naive_type, &attr_string);
                }

                add_tag(attr_string[0]);
                add_tag(",");

            } // End if is_compound == 0
            else {
                sprintf(tmp_str, "S%lu,", size);
                add_tag(tmp_str);
            }

            /* display size, padding, termination, etc. */
            /* } else if(t_class == H5T_BITFIELD) { */
            /*       puts(" 'H5T_BITFIELD'."); */
            /* 	/1* display size, label, etc. *1/ */
            /* } else if(t_class == H5T_OPAQUE) { */
            /*       puts(" 'H5T_OPAQUE'."); */
            /* 	/1* display size, etc. *1/ */
        } else if(t_class == H5T_COMPOUND) {
            // For compound type, the size would be calculated by its sub-types
            /* puts(" 'H5T_COMPOUND' {"); */
            add_tag("[");
            /* recursively display each member: field name, type  */
            compound_nmember = H5Tget_nmembers(tid);
            for (i = 0; i < compound_nmember; i++) {
                mem_name = H5Tget_member_name(tid, i);
                /* printf("        Compound member [%20s]  ", mem_name); */
                add_tag(mem_name);
                add_tag("=");
                mem_type = H5Tget_member_type(tid, i);
                do_dtype(mem_type, oid, 1);
            }
            /* puts("    } End 'H5T_COMPOUND'.\n"); */

            add_tag("]");

        } else if(t_class == H5T_ARRAY) {
            if (is_compound == 0) {
                tag_size_g += size;
            }
            ndim = H5Tget_array_ndims(tid);
            H5Tget_array_dims2(tid, dims);
            /* printf(" 'H5T_ARRAY', ndim=%d:  ", ndim); */
            sprintf(tmp_str, "A%d", ndim);
            add_tag(tmp_str);
            for (i = 0; i < ndim; i++) {
                /* printf("%d, ", dims[i]); */
                sprintf(tmp_str, "_%d", dims[i]);
                add_tag(tmp_str);
            }
            /* printf("\n                                                "); */
            do_dtype(H5Tget_super(tid), oid, 1);
            /* display  dimensions, base type  */
        } else if(t_class == H5T_ENUM) {
            /* puts(" 'H5T_ENUM'."); */
            sprintf(tmp_str, "E,");
            add_tag(tmp_str);
            /* display elements: name, value   */
        } else  {
            /* puts(" 'Other'."); */
            sprintf(tmp_str, "!OTHER!,");
            add_tag(tmp_str);
            /* eg. Object Reference, ...and so on ... */
        }
    }
}


/*
 *  Analyze a symbolic link
 *
 * The main thing you can do with a link is find out
 * what it points to.
 */
void
do_link(hid_t gid, char *name) {
    herr_t status;
    char target[MAX_NAME];

    status = H5Gget_linkval(gid, name, MAX_NAME, target  ) ;
    /* printf("Symlink: %s points to: %s\n", name, target); */
}

/*
 *  Run through all the attributes of a dataset or group.
 *  This is similar to iterating through a group.
 */
void
scan_attrs(hid_t oid) {
    int na;
    hid_t aid;
    int i;

    na = H5Aget_num_attrs(oid);

    for (i = 0; i < na; i++) {
        aid =	H5Aopen_idx(oid, (unsigned int)i );
        do_attr(aid);
        H5Aclose(aid);
    }
}

/*
 *  Process one attribute.
 *  This is similar to the information about a dataset.
 */
void do_attr(hid_t aid) {
    ssize_t len;
    hid_t atype;
    hid_t aspace;
    char buf[MAX_NAME] = {0};

    /*
     * Get the name of the attribute.
     */
    len = H5Aget_name(aid, MAX_NAME, buf );
    /* printf("    Attribute Name : %s\n",buf); */

    // Skip the COMMENT attribute
    if (strcmp("COMMENT", buf) == 0 || strcmp("comments", buf) == 0)
        return;

    add_tag(buf);
    add_tag("=");
    /*
     * Get attribute information: dataspace, data type
     */
    aspace = H5Aget_space(aid); /* the dimensions of the attribute data */

    atype  = H5Aget_type(aid);
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
do_plist(hid_t pid) {
    hsize_t chunk_dims_out[2];
    int  rank_chunk;
    int nfilters;
    H5Z_filter_t  filtn;
    int i;
    unsigned int   filt_flags, filt_conf;
    size_t cd_nelmts;
    unsigned int cd_values[32] ;
    char f_name[MAX_NAME];
    H5D_fill_time_t ft;
    H5D_alloc_time_t at;
    H5D_fill_value_t fvstatus;
    unsigned int szip_options_mask;
    unsigned int szip_pixels_per_block;

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
    for (i = 0; i < nfilters; i++)
    {
        /* For each filter, get
         *   filter ID
         *   filter specific parameters
         */
        cd_nelmts = 32;
        filtn = H5Pget_filter(pid, (unsigned)i,
                              &filt_flags, &cd_nelmts, cd_values,
                              (size_t)MAX_NAME, f_name, &filt_conf);
        /*
         *  These are the predefined filters
         */
        switch (filtn) {
        case H5Z_FILTER_DEFLATE:  /* AKA GZIP compression */
            /* printf("DEFLATE level = %d\n", cd_values[0]); */
            break;
        case H5Z_FILTER_SHUFFLE:
            /* printf("SHUFFLE\n"); /1* no parms *1/ */
            break;
        case H5Z_FILTER_FLETCHER32:
            /* printf("FLETCHER32\n"); /1* Error Detection Code *1/ */
            break;
        case H5Z_FILTER_SZIP:
            szip_options_mask=cd_values[0];;
            szip_pixels_per_block=cd_values[1];

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

    switch (at)
    {
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
    switch ( ft )
    {
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

    if (fvstatus == H5D_FILL_VALUE_UNDEFINED)
    {
        /* printf("No fill value defined, will use default\n"); */
    } else {
        /* Read  the fill value with H5Pget_fill_value.
         * Fill value is the same data type as the dataset.
         * (details not shown)
         **/
    }

    /* ... and so on for other dataset properties ... */
}
