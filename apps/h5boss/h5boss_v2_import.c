#include <stdio.h>
#include <stdlib.h>

#include "hdf5.h"
#include "pdc.h"

#define MAX_NAME 1024
#define MAX_TAG_LEN 4096

void do_dtype(hid_t, hid_t, int);
void do_dset(hid_t did, char *name);
void do_link(hid_t, char *);
void scan_group(hid_t);
void do_attr(hid_t);
void scan_attrs(hid_t);
void do_plist(hid_t);

void print_usage() {
    printf("Usage: srun -n ./h5boss_v2_import /path/to/h5boss_file\n");
}

char tags_g[MAX_TAG_LEN];
char *tags_ptr_g;
hsize_t tag_size_g;
int  ndset = 0;

int add_tag(char *str)
{
    int str_len = 0;
    if (NULL == str || NULL == tags_ptr_g) {
        fprintf(stderr, "%s - input str is NULL!", __func__);
        return 0;
    }
    else if (tag_size_g + str_len >= MAX_TAG_LEN) {
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

    hid_t    file;
    hid_t    grp;

    herr_t   status;
 

    char* filename;

    if (argc != 2)
        print_usage();
    else {
       filename = argv[1];

       /*
        *  Example: open a file, open the root, scan the whole file.
        */
       file = H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT);

       grp = H5Gopen(file,"/", H5P_DEFAULT);
       scan_group(grp);

       status = H5Fclose(file);

       printf("\n\n======================\nNumber of datasets: %d\n", ndset);
       return 0;
    }
}

/*
 * Process a group and all it's members
 *
 *   This can be used as a model to implement different actions and
 *   searches.
 */

void
scan_group(hid_t gid) {
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
				scan_group(grpid);
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
        char *obj_name;
        int name_len, i;

        tag_size_g = 0;
        memset(tags_g, 0, sizeof(char)*MAX_TAG_LEN);
        tags_ptr_g = tags_g;
        /*
         * Information about the group:
         *  Name and attributes
         *
         *  Other info., not shown here: number of links, object id
         */
	H5Iget_name(did, ds_name, MAX_NAME  );
        add_tag(ds_name);
        add_tag("{");

        name_len = strlen(ds_name);
        for (i = name_len; i >= 0; i--) {
            if (ds_name[i] == '/') {
                obj_name = &ds_name[i+1];
                break;
            }
        }
	printf("[%s] {\n", obj_name);

        if (strcmp(obj_name, "match") == 0 ) {
            i = 0;
        }

        ndset ++;
	/*
	 *  process the attributes of the dataset, if any.
	 */
	scan_attrs(did);
  
	/*    
	 * Get dataset information: dataspace, data type 
	 */
	sid = H5Dget_space(did); /* the dimensions of the dataset (not shown) */
	tid = H5Dget_type(did);
	/* printf(" DATA TYPE:\n"); */
	do_dtype(tid, did, 0);

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

        add_tag("}");

        printf("} [%s] tag_size %d  \n========================\n%s\n========================\n\n\n", 
                obj_name, tag_size_g, tags_g);
        tag_size_g = 0;
}

/*
 *  Analyze a data type description
 */
void
do_dtype(hid_t tid, hid_t oid, int is_compound) {

        int compound_nmember, i;
        hsize_t dims[8], ndim;
        char *mem_name;
        char *attr_string[100], new_string[MAX_TAG_LEN], tmp_str[MAX_TAG_LEN];
        hsize_t size, attr_len;
        hid_t mem_type;
        hid_t atype, aspace, naive_type;
	H5T_class_t t_class, compound_class;
	t_class = H5Tget_class(tid);
	if(t_class < 0){ 
		puts("   Invalid datatype.\n");
	} else {
                size = H5Tget_size (tid);
                printf("    Datasize %3d, type", size);
		/* 
		 * Each class has specific properties that can be 
		 * retrieved, e.g., size, byte order, exponent, etc. 
		 */
		if(t_class == H5T_INTEGER) {
		      puts(" 'H5T_INTEGER'.");
                      sprintf(tmp_str, "I%lu,", size);
                      add_tag(tmp_str);
			/* display size, signed, endianess, etc. */
		} else if(t_class == H5T_FLOAT) {
		      puts(" 'H5T_FLOAT'.");
                      sprintf(tmp_str, "F%lu,", size);
                      add_tag(tmp_str);
			/* display size, endianess, exponennt, etc. */
		} else if(t_class == H5T_STRING) {
		      puts(" 'H5T_STRING'.");

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
                              // TODO: string attribute with variable size is not read correctly
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
		      puts(" 'H5T_COMPOUND' {");
                      add_tag("C[");
			/* recursively display each member: field name, type  */
                      compound_nmember = H5Tget_nmembers(tid);
                      for (i = 0; i < compound_nmember; i++) {
                          mem_name = H5Tget_member_name(tid, i);
                          printf("        Compound member [%20s]  ", mem_name);
                          add_tag(mem_name);
                          add_tag("=");
                          mem_type = H5Tget_member_type(tid, i);
                          do_dtype(mem_type, oid, 1);
                      }
		      puts("    } End 'H5T_COMPOUND'.\n");

                      add_tag("]");

		} else if(t_class == H5T_ARRAY) {
                      if (is_compound == 0) {
                          tag_size_g += size;
                      }
                      ndim = H5Tget_array_ndims(tid);
                      H5Tget_array_dims2(tid, dims);
                      printf(" 'H5T_ARRAY', ndim=%d:  ", ndim);
                      sprintf(tmp_str, "A%d", ndim);
                      add_tag(tmp_str);
                      for (i = 0; i < ndim; i++) { 
                          printf("%d, ", dims[i]);
                          sprintf(tmp_str, "_%d", dims[i]);
                          add_tag(tmp_str);
                      }
                      printf("\n                                                ");
                      do_dtype(H5Tget_super(tid), oid, 0);
			/* display  dimensions, base type  */
		} else if(t_class == H5T_ENUM) {
		      puts(" 'H5T_ENUM'.");
                      sprintf(tmp_str, "E,");
                      add_tag(tmp_str);
			/* display elements: name, value   */
		} else  {
		      puts(" 'Other'.");
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
	printf("Symlink: %s points to: %s\n", name, target);
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
	printf("    Attribute Name : %s\n",buf);

        // Skip the COMMENT attribute
        if (strcmp("COMMENT", buf) == 0) 
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
	if(H5D_CHUNKED == H5Pget_layout(pid)){
		rank_chunk = H5Pget_chunk(pid, 2, chunk_dims_out);
		printf("chunk rank %d, dimensions %lu x %lu\n", rank_chunk, 
		   (unsigned long)(chunk_dims_out[0]),
		   (unsigned long)(chunk_dims_out[1]));
	} /* else if contiguous, etc. */

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
				printf("DEFLATE level = %d\n", cd_values[0]);
				break;
			case H5Z_FILTER_SHUFFLE:
				printf("SHUFFLE\n"); /* no parms */
				break;
		       case H5Z_FILTER_FLETCHER32:
				printf("FLETCHER32\n"); /* Error Detection Code */
				break;
		       case H5Z_FILTER_SZIP:
				szip_options_mask=cd_values[0];;
				szip_pixels_per_block=cd_values[1];

				printf("SZIP COMPRESSION: ");
				printf("PIXELS_PER_BLOCK %d\n", 
					szip_pixels_per_block);
				 /* print SZIP options mask, etc. */
				break;
			default:
				printf("UNKNOWN_FILTER\n" );
				break;
	       }
      }

	/* 
	 *  Get the fill value information:
	 *    - when to allocate space on disk
	 *    - when to fill on disk
	 *    - value to fill, if any
	 */

	printf("ALLOC_TIME ");
	H5Pget_alloc_time(pid, &at);

	switch (at) 
	{
		case H5D_ALLOC_TIME_EARLY: 
			printf("EARLY\n");
			break;
		case H5D_ALLOC_TIME_INCR:
			printf("INCR\n");
			break;
		case H5D_ALLOC_TIME_LATE: 
			printf("LATE\n");
			break;
		default:
			printf("unknown allocation policy");
			break;
	}

	printf("FILL_TIME: ");
	H5Pget_fill_time(pid, &ft);
	switch ( ft ) 
	{
		case H5D_FILL_TIME_ALLOC: 
			printf("ALLOC\n");
			break;
		case H5D_FILL_TIME_NEVER: 
			printf("NEVER\n");
			break;
		case H5D_FILL_TIME_IFSET: 
			printf("IFSET\n");
			break;
		default:
			printf("?\n");
		break;
	}


	H5Pfill_value_defined(pid, &fvstatus);
 
	if (fvstatus == H5D_FILL_VALUE_UNDEFINED) 
	{
		printf("No fill value defined, will use default\n");
	} else {
		/* Read  the fill value with H5Pget_fill_value. 
		 * Fill value is the same data type as the dataset.
		 * (details not shown) 
		 **/
	}

	/* ... and so on for other dataset properties ... */
}
