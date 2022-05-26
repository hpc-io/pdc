#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <regex.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "hdf5.h"

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"
#include "../src/server/include/pdc_server_metadata.h"
#include "cjson/cJSON.h"

const char *avail_args[] = {"-f"};
const int   num_args     = 6;

int     rank = 0, size = 1;
pdcid_t pdc_id_g = 0;

typedef struct pdc_region_metadata_pkg {
    uint64_t                       *reg_offset;
    uint64_t                       *reg_size;
    uint32_t                        data_server_id;
    struct pdc_region_metadata_pkg *next;
} pdc_region_metadata_pkg;

typedef struct pdc_obj_metadata_pkg {
    int                          ndim;
    uint64_t                     obj_id;
    pdc_region_metadata_pkg     *regions;
    pdc_region_metadata_pkg     *regions_end;
    struct pdc_obj_metadata_pkg *next;
} pdc_obj_metadata_pkg;

typedef struct pdc_obj_region_metadata {
    uint64_t  obj_id;
    uint64_t *reg_offset;
    uint64_t *reg_size;
    int       ndim;
} pdc_obj_region_metadata;

typedef struct pdc_metadata_query_buf {
    uint64_t                       id;
    char                          *buf;
    struct pdc_metadata_query_buf *next;
} pdc_metadata_query_buf;

typedef struct RegionNode {
    region_list_t     *region_list;
    struct RegionNode *next;
} RegionNode;

typedef struct MetadataNode {
    pdc_metadata_t       *metadata_ptr;
    struct MetadataNode  *next;
    RegionNode           *region_list_head;
    pdc_obj_metadata_pkg *obj_metadata_pkg;
} MetadataNode;

typedef struct FileNameNode {
    char                *file_name;
    struct FileNameNode *next;
} FileNameNode;

typedef struct ArrayList {
    int    length;
    int    capacity;
    char **items;
} ArrayList;

ArrayList *
newList(void)
{
    char     **items = malloc(4 * sizeof(char *));
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

hid_t
get_h5type(pdc_var_type_t pdc_type)
{
    if (pdc_type == PDC_INT) {
        return H5T_NATIVE_INT;
    }
    else if (pdc_type == PDC_FLOAT) {
        return H5T_NATIVE_FLOAT;
    }
    else if (pdc_type == PDC_CHAR) {
        return H5T_NATIVE_CHAR;
    }
    else {
        return H5T_NATIVE_INT;
    }
}

int
is_arg(char *arg)
{
    for (int i = 0; i < num_args; i++) {
        if (strcmp(arg, avail_args[i]) == 0) {
            return 1;
        }
    }
    return 0;
}

int    pdc_server_rank_g = 0;
int    pdc_server_size_g = 1;
double total_mem_usage_g = 0.0;

static void pdc_ls(FileNameNode *file_name_node, int argc, char *argv[]);

int
main(int argc, char *argv[])
{
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    pdc_id_g = PDCinit("pdc");

    if (argc == 1) {
        printf("Expected directory/checkpoint file.\n");
        return 1;
    }
    else {
        FileNameNode  *head     = NULL;
        FileNameNode  *cur_node = NULL;
        DIR           *d;
        struct dirent *dir;
        d = opendir(argv[1]);
        if (d) {
            while ((dir = readdir(d)) != NULL) {
                if (strstr(dir->d_name, "metadata_checkpoint.")) {
                    char  last = argv[1][strlen(argv[1]) - 1];
                    char *full_path;
                    if (last == '/') {
                        full_path =
                            (char *)malloc(sizeof(char) * (strlen(argv[1]) + strlen(dir->d_name) + 1));
                        strcpy(full_path, argv[1]);
                        strcat(full_path, dir->d_name);
                    }
                    else {
                        full_path =
                            (char *)malloc(sizeof(char) * (strlen(argv[1]) + strlen(dir->d_name) + 2));
                        strcpy(full_path, argv[1]);
                        strcat(full_path, "/");
                        strcat(full_path, dir->d_name);
                    }
                    if (head == NULL) {
                        FileNameNode *new_node = (FileNameNode *)malloc(sizeof(FileNameNode));
                        new_node->file_name    = full_path;
                        new_node->next         = NULL;
                        head                   = new_node;
                        cur_node               = new_node;
                    }
                    else {
                        FileNameNode *new_node = (FileNameNode *)malloc(sizeof(FileNameNode));
                        new_node->file_name    = full_path;
                        new_node->next         = NULL;
                        cur_node->next         = new_node;
                        cur_node               = new_node;
                    }
                }
            }
            closedir(d);
        }
        else {
            FILE *file = fopen(argv[1], "r");
            if (file != NULL) {
                FileNameNode *new_node  = (FileNameNode *)malloc(sizeof(FileNameNode));
                char         *full_path = (char *)malloc(sizeof(char) * (strlen(argv[1]) + 1));
                strcpy(full_path, argv[1]);
                new_node->file_name = full_path;
                new_node->next      = NULL;
                head                = new_node;
                cur_node            = new_node;
                fclose(file);
            }
        }
        if (head == NULL) {
            printf("Unable to open/locate checkpoint file(s).\n");
            return 1;
        }
        else {
            pdc_ls(head, argc, argv);
        }
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
}

int
region_cmp(region_list_t *a, region_list_t *b)
{
    int unit_size = a->ndim * sizeof(uint64_t);
    return memcmp(a->start, b->start, unit_size);
}

char *
get_data_type(int data_type)
{
    if (data_type == -1) {
        return "PDC_UNKNOWN";
    }
    else if (data_type == 0) {
        return "PDC_INT";
    }
    else if (data_type == 1) {
        return "PDC_FLOAT";
    }
    else if (data_type == 2) {
        return "PDC_DOUBLE";
    }
    else if (data_type == 3) {
        return "PDC_CHAR";
    }
    else if (data_type == 4) {
        return "PDC_COMPOUND";
    }
    else if (data_type == 5) {
        return "PDC_ENUM";
    }
    else if (data_type == 6) {
        return "PDC_ARRAY";
    }
    else if (data_type == 7) {
        return "PDC_UINT";
    }
    else if (data_type == 8) {
        return "PDC_INT64";
    }
    else if (data_type == 9) {
        return "PDC_UINT64";
    }
    else if (data_type == 10) {
        return "PDC_INT16";
    }
    else if (data_type == 11) {
        return "PDC_INT16";
    }
    else {
        return "NULL";
    }
}

char *
get_data_loc_type(int data_loc_type)
{
    if (data_loc_type == 0) {
        return "PDC_NONE";
    }
    else if (data_loc_type == 1) {
        return "PDC_LUSTRE";
    }
    else if (data_loc_type == 2) {
        return "PDC_BB";
    }
    else if (data_loc_type == 3) {
        return "PDC_MEM";
    }
    else {
        return "NULL";
    }
}

pdc_obj_metadata_pkg *
do_transfer_request_metadata(int pdc_server_size_input, char *checkpoint)
{
    char *ptr;
    int   n_objs, reg_count;
    int   i, j;

    pdc_obj_metadata_pkg   *metadata_server_objs     = NULL;
    pdc_obj_metadata_pkg   *metadata_server_objs_end = NULL;
    pdc_metadata_query_buf *metadata_query_buf_head  = NULL;
    pdc_metadata_query_buf *metadata_query_buf_end   = NULL;
    int                     pdc_server_size          = pdc_server_size_input;
    uint64_t               *data_server_bytes        = (uint64_t *)calloc(pdc_server_size, sizeof(uint64_t));
    uint64_t                query_id_g               = 100000;
    ptr                                              = checkpoint;

    if (checkpoint) {
        n_objs = *(int *)ptr;
        ptr += sizeof(int);
        for (i = 0; i < n_objs; ++i) {
            if (metadata_server_objs) {
                metadata_server_objs_end->next =
                    (pdc_obj_metadata_pkg *)malloc(sizeof(pdc_obj_region_metadata));
                metadata_server_objs_end = metadata_server_objs_end->next;
            }
            else {
                metadata_server_objs     = (pdc_obj_metadata_pkg *)malloc(sizeof(pdc_obj_region_metadata));
                metadata_server_objs_end = metadata_server_objs;
            }

            metadata_server_objs_end->obj_id = *(uint64_t *)ptr;
            ptr += sizeof(uint64_t);
            metadata_server_objs_end->ndim = *(int *)ptr;
            ptr += sizeof(int);
            reg_count = *(int *)ptr;
            ptr += sizeof(int);

            metadata_server_objs_end->regions =
                (pdc_region_metadata_pkg *)malloc(sizeof(pdc_region_metadata_pkg));
            metadata_server_objs_end->regions_end = metadata_server_objs_end->regions;

            metadata_server_objs_end->regions_end->next = NULL;
            metadata_server_objs_end->regions_end->reg_offset =
                (uint64_t *)malloc(sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
            metadata_server_objs_end->regions_end->reg_size =
                metadata_server_objs_end->regions_end->reg_offset + metadata_server_objs_end->ndim;
            metadata_server_objs_end->regions_end->data_server_id = *(uint32_t *)ptr;
            ptr += sizeof(uint32_t);
            memcpy(metadata_server_objs_end->regions_end->reg_offset, ptr,
                   sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
            ptr += sizeof(uint64_t) * metadata_server_objs_end->ndim * 2;

            for (j = 1; j < reg_count; ++j) {
                metadata_server_objs_end->regions->next =
                    (pdc_region_metadata_pkg *)malloc(sizeof(pdc_region_metadata_pkg));
                metadata_server_objs_end->regions_end = metadata_server_objs_end->regions_end->next;

                metadata_server_objs_end->regions_end->next = NULL;
                metadata_server_objs_end->regions_end->reg_offset =
                    (uint64_t *)malloc(sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
                metadata_server_objs_end->regions_end->reg_size =
                    metadata_server_objs_end->regions_end->reg_offset + metadata_server_objs_end->ndim;
                metadata_server_objs_end->regions_end->data_server_id = *(uint32_t *)ptr;
                ptr += sizeof(uint32_t);
                memcpy(metadata_server_objs_end->regions_end->reg_offset, ptr,
                       sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
                ptr += sizeof(uint64_t) * metadata_server_objs_end->ndim * 2;
            }
        }
    }
    return metadata_server_objs;
}

void
pdc_ls(FileNameNode *file_name_node, int argc, char *argv[])
{
    int arg_index = 2;
    while (arg_index < argc) {
        if (is_arg(argv[arg_index]) == 0) {
            printf("Improperly formatted argument(s).\n");
            return;
        }
        if (strcmp(argv[arg_index], "-f") == 0) {
            arg_index++;
            if (strcmp(argv[arg_index], "hdf5") != 0) {
                printf("The specified file format is not supported.\n");
                return;
            }
        }
        arg_index++;
    }

    char         *filename;
    MetadataNode *metadata_head   = NULL;
    RegionNode   *cur_region_node = NULL;
    FileNameNode *cur_file_node   = file_name_node;

    int all_cont_total     = 0;
    int all_nobj_total     = 0;
    int all_n_region_total = 0;

    struct stat attr;
    regex_t     regex;
    int         reti;

    while (cur_file_node != NULL) {
        filename = cur_file_node->file_name;
        stat(filename, &attr);
        printf("[INFO] File [%s] last modified at: %s\n", filename, ctime(&attr.st_mtime));
        // Start server restart code
        perr_t ret_value = SUCCEED;
        int    n_entry, count, i, j, nobj = 0, all_nobj = 0, all_n_region, n_region, n_objs, total_region = 0,
                                  n_kvtag, key_len;
        int                          n_cont, all_cont;
        pdc_metadata_t              *metadata, *elt;
        region_list_t               *region_list;
        uint32_t                    *hash_key;
        unsigned                     idx;
        pdc_cont_hash_table_entry_t *cont_entry;
        pdc_hash_table_entry_head   *entry;

        FILE *file = fopen(filename, "r");
        if (file == NULL) {
            printf("==PDC_SERVER[%d]: %s -  Checkpoint file open FAILED [%s]!", pdc_server_rank_g, __func__,
                   filename);
            ret_value = FAIL;
            continue;
        }

        if (fread(&n_cont, sizeof(int), 1, file) != 1) {
            printf("Read failed for n_count\n");
        }
        all_cont = n_cont;
        while (n_cont > 0) {
            hash_key = (uint32_t *)malloc(sizeof(uint32_t));
            if (fread(hash_key, sizeof(uint32_t), 1, file) != 1) {
                printf("Read failed for hash_key\n");
            }

            // Reconstruct hash table
            cont_entry = (pdc_cont_hash_table_entry_t *)malloc(sizeof(pdc_cont_hash_table_entry_t));
            if (fread(cont_entry, sizeof(pdc_cont_hash_table_entry_t), 1, file) != 1) {
                printf("Read failed for cont_entry\n");
            }

            n_cont--;
        } // End while
        if (fread(&n_entry, sizeof(int), 1, file) != 1) {
            printf("Read failed for n_entry\n");
        }

        while (n_entry > 0) {
            if (fread(&count, sizeof(int), 1, file) != 1) {
                printf("Read failed for count\n");
            }

            hash_key = (uint32_t *)malloc(sizeof(uint32_t));
            if (fread(hash_key, sizeof(uint32_t), 1, file) != 1) {
                printf("Read failed for hash_key\n");
            }

            // Reconstruct hash table
            entry           = (pdc_hash_table_entry_head *)malloc(sizeof(pdc_hash_table_entry_head));
            entry->n_obj    = 0;
            entry->bloom    = NULL;
            entry->metadata = NULL;

            /* fprintf(stderr, "size of metadata: %lu\n", sizeof(pdc_metadata_t)); */
            metadata = (pdc_metadata_t *)calloc(sizeof(pdc_metadata_t), count);
            for (i = 0; i < count; i++) {
                if (fread(metadata + i, sizeof(pdc_metadata_t), 1, file) != 1) {
                    printf("Read failed for metadata\n");
                }
                MetadataNode *new_node     = (MetadataNode *)malloc(sizeof(MetadataNode));
                new_node->metadata_ptr     = (metadata + i);
                new_node->next             = NULL;
                new_node->region_list_head = NULL;
                if (metadata_head == NULL) {
                    metadata_head = new_node;
                }
                else {
                    MetadataNode *cur_iter_node = metadata_head;
                    MetadataNode *prev_node     = NULL;
                    int           inserted      = 0;
                    while (cur_iter_node != NULL) {
                        if (cur_iter_node->metadata_ptr->obj_id > new_node->metadata_ptr->obj_id) {
                            if (prev_node == NULL) {
                                new_node->next = metadata_head;
                                metadata_head  = new_node;
                                inserted       = 1;
                                break;
                            }
                            else {
                                new_node->next  = cur_iter_node;
                                prev_node->next = new_node;
                                inserted        = 1;
                                break;
                            }
                        }
                        prev_node     = cur_iter_node;
                        cur_iter_node = cur_iter_node->next;
                    }
                    if (inserted == 0) {
                        prev_node->next = new_node;
                    }
                }

                // Read kv tags
                if (fread(&n_kvtag, sizeof(int), 1, file) != 1) {
                    printf("Read failed for n_kvtag\n");
                }
                for (j = 0; j < n_kvtag; j++) {
                    pdc_kvtag_list_t *kvtag_list = (pdc_kvtag_list_t *)calloc(1, sizeof(pdc_kvtag_list_t));
                    kvtag_list->kvtag            = (pdc_kvtag_t *)malloc(sizeof(pdc_kvtag_t));
                    if (fread(&key_len, sizeof(int), 1, file) != 1) {
                        printf("Read failed for key_len\n");
                    }
                    kvtag_list->kvtag->name = malloc(key_len);
                    if (fread((void *)(kvtag_list->kvtag->name), key_len, 1, file) != 1) {
                        printf("Read failed for kvtag_list->kvtag->name\n");
                    }
                    if (fread(&kvtag_list->kvtag->size, sizeof(uint32_t), 1, file) != 1) {
                        printf("Read failed for kvtag_list->kvtag->size\n");
                    }
                    kvtag_list->kvtag->value = malloc(kvtag_list->kvtag->size);
                    if (fread(kvtag_list->kvtag->value, kvtag_list->kvtag->size, 1, file) != 1) {
                        printf("Read failed for kvtag_list->kvtag->value\n");
                    }
                }

                if (fread(&n_region, sizeof(int), 1, file) != 1) {
                    printf("Read failed for n_region\n");
                }
                if (n_region < 0) {
                    printf("==PDC_SERVER[%d]: %s -  Checkpoint file region number ERROR!", pdc_server_rank_g,
                           __func__);
                    ret_value = FAIL;
                    continue;
                }

                total_region += n_region;

                for (j = 0; j < n_region; j++) {
                    region_list = (region_list_t *)malloc(sizeof(region_list_t));
                    if (fread(region_list, sizeof(region_list_t), 1, file) != 1) {
                        printf("Read failed for region_list\n");
                    }

                    int has_hist = 0;
                    if (fread(&has_hist, sizeof(int), 1, file) != 1) {
                        printf("Read failed for has_list\n");
                    }
                    if (has_hist == 1) {
                        region_list->region_hist = (pdc_histogram_t *)malloc(sizeof(pdc_histogram_t));
                        if (fread(&region_list->region_hist->dtype, sizeof(int), 1, file) != 1) {
                            printf("Read failed for region_list->region_hist->dtype\n");
                        }
                        if (fread(&region_list->region_hist->nbin, sizeof(int), 1, file) != 1) {
                            printf("Read failed for region_list->region_hist->nbin\n");
                        }
                        if (region_list->region_hist->nbin == 0) {
                            printf("==PDC_SERVER[%d]: %s -  Checkpoint file histogram size is 0!",
                                   pdc_server_rank_g, __func__);
                        }

                        region_list->region_hist->range =
                            (double *)malloc(sizeof(double) * region_list->region_hist->nbin * 2);
                        region_list->region_hist->bin =
                            (uint64_t *)malloc(sizeof(uint64_t) * region_list->region_hist->nbin);

                        if (fread(region_list->region_hist->range, sizeof(double),
                                  region_list->region_hist->nbin * 2, file) != 1) {
                            printf("Read failed for region_list->region_hist->range\n");
                        }
                        if (fread(region_list->region_hist->bin, sizeof(uint64_t),
                                  region_list->region_hist->nbin, file) != 1) {
                            printf("Read failed for region_list->region_hist->bin\n");
                        }
                        if (fread(&region_list->region_hist->incr, sizeof(double), 1, file) != 1) {
                            printf("Read failed for region_list->region_hist->incr\n");
                        }
                    }

                } // For j
                total_region += n_region;
            } // For i

            nobj += count;
            entry->metadata = NULL;

            n_entry--;
        }

        if (fread(&n_objs, sizeof(int), 1, file) != 1) {
            printf("Read failed for n_objs\n");
        }

        for (i = 0; i < n_objs; ++i) {
            data_server_region_t *new_obj_reg =
                (data_server_region_t *)calloc(1, sizeof(struct data_server_region_t));
            new_obj_reg->fd               = -1;
            new_obj_reg->storage_location = (char *)malloc(sizeof(char) * ADDR_MAX);
            if (fread(&new_obj_reg->obj_id, sizeof(uint64_t), 1, file) != 1) {
                printf("Read failed for obj_id\n");
            }
            if (fread(&n_region, sizeof(int), 1, file) != 1) {
                printf("Read failed for n_region\n");
            }
            MetadataNode *wanted_node = metadata_head;
            while (wanted_node != NULL) {
                if (wanted_node->metadata_ptr->obj_id == new_obj_reg->obj_id) {
                    break;
                }
                wanted_node = wanted_node->next;
            }
            RegionNode *cur_region = wanted_node->region_list_head;
            for (j = 0; j < n_region; j++) {
                region_list_t *new_region_list = (region_list_t *)malloc(sizeof(region_list_t));
                if (fread(new_region_list, sizeof(region_list_t), 1, file) != 1) {
                    printf("Read failed for new_region_list\n");
                }
                RegionNode *new_node  = (RegionNode *)malloc(sizeof(RegionNode));
                new_node->region_list = new_region_list;
                new_node->next        = NULL;
                if (cur_region == NULL) {
                    wanted_node->region_list_head = new_node;
                    cur_region                    = new_node;
                }
                else {
                    cur_region->next = new_node;
                    cur_region       = new_node;
                }
            }
        }

        uint64_t checkpoint_size;
        char    *checkpoint_buf;

        if (fread(&checkpoint_size, sizeof(uint64_t), 1, file) != 1) {
            printf("Read failed for checkpoint size\n");
        }

        checkpoint_buf = (char *)malloc(checkpoint_size);

        if (fread(checkpoint_buf, checkpoint_size, 1, file) != 1) {
            printf("Read failed for checkpoint buf\n");
        }

        pdc_obj_metadata_pkg *metadata_server_objs =
            do_transfer_request_metadata(pdc_server_size_g, checkpoint_buf);

        pdc_obj_metadata_pkg *cur_pkg = metadata_server_objs;
        while (cur_pkg != NULL) {
            uint64_t      wanted_obj_id     = cur_pkg->obj_id;
            MetadataNode *cur_metadata_node = metadata_head;
            while (cur_metadata_node != NULL) {
                if (cur_metadata_node->metadata_ptr->obj_id == wanted_obj_id) {
                    cur_metadata_node->obj_metadata_pkg = cur_pkg;
                    break;
                }
                cur_metadata_node = cur_metadata_node->next;
            }
            cur_pkg = cur_pkg->next;
        }

        fclose(file);
        file = NULL;

        all_nobj     = nobj;
        all_n_region = total_region;

        all_cont_total += all_cont;
        all_nobj_total += all_nobj;
        all_n_region_total += all_n_region;

        // End Server Restart Code
        cur_file_node = cur_file_node->next;
    }

    int             prev_cont_id      = -1;
    MetadataNode   *cur_metadata_node = metadata_head;
    pdc_metadata_t *cur_metadata;
    hid_t           file_id;
    fflush(stdout);
    hid_t group_id;
    hid_t dset_id;
    // iterate through each node
    while (cur_metadata_node != NULL) {
        cur_metadata = cur_metadata_node->metadata_ptr;

        // if new container id, then create new group
        if (prev_cont_id != cur_metadata->cont_id) {
            if (prev_cont_id != -1) {
                H5Gclose(group_id);
            }
            char buf[20];
            sprintf(buf, "%d.hdf5", cur_metadata->cont_id);
            file_id = H5Fcreate(buf, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
            sprintf(buf, "%d", cur_metadata->cont_id);
            group_id = H5Gcreate(file_id, buf, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        }

        // create a dataset for each object

        // create buffer and other needed info for region transfer
        char buf[20];
        sprintf(buf, "%d", cur_metadata->obj_id);
        hid_t sid       = H5Screate_simple(cur_metadata->ndim, (hsize_t *)(cur_metadata->dims), NULL);
        hid_t data_type = get_h5type(cur_metadata->data_type);
        dset_id = H5Dcreate(group_id, cur_metadata->obj_name, data_type, sid, H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT);
        if (dset_id < 0) {
            char *full_path = (char *)malloc(sizeof(char) * strlen(cur_metadata->obj_name) + 1);
            strcpy(full_path, cur_metadata->obj_name);

            char  *last_slash = strrchr(full_path, '/');
            size_t length     = last_slash - full_path + 1;
            char   temp[length + 1];
            memcpy(temp, full_path, length);
            temp[length] = '\0';
            char only_file_name[strlen(cur_metadata->obj_name) + 1];
            strcpy(only_file_name, last_slash + 1);

            char file_name[strlen(cur_metadata->obj_name) + 1];
            char buf[20];
            sprintf(buf, "%d", cur_metadata->cont_id);
            char *cur_path = (char *)malloc(sizeof(char) * strlen(cur_metadata->obj_name) + strlen(buf) + 1);
            strcpy(cur_path, buf);
            const char delim[2] = "/";
            char      *token;
            token              = strtok(temp, delim);
            hid_t cur_group_id = group_id;
            while (token != NULL) {
                printf("%s\n", token);
                strcat(cur_path, "/");
                strcat(cur_path, token);
                hid_t cur_group_id = H5Gopen(file_id, cur_path, H5P_DEFAULT);
                if (cur_group_id < 0) {
                    cur_group_id = H5Gcreate(file_id, cur_path, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
                }
                token = strtok(NULL, delim);
            }
            free(full_path);
            full_path = (char *)malloc(sizeof(char) * strlen(cur_metadata->obj_name) + strlen(buf) + 1);
            strcpy(full_path, buf);
            strcat(full_path, cur_metadata->obj_name);
            dset_id = H5Dcreate(file_id, full_path, data_type, sid, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
            if (dset_id < 0) {
                printf("create dset failed\n");
            }
        }
        int buf_size = 1;
        for (int i = 0; i < cur_metadata->ndim; i++) {
            buf_size *= (cur_metadata->dims)[i];
        }
        hsize_t dtype_size = H5Tget_size(data_type);
        void   *data_buf   = malloc(buf_size * dtype_size); // if data side is large, need to write in batches

        uint64_t offset[10], size[10];
        for (int i = 0; i < cur_metadata->ndim; i++) {
            offset[i] = 0;
            size[i]   = (cur_metadata->dims)[i];
        }
        // size[0] *= dtype_size;

        pdcid_t local_region  = PDCregion_create(cur_metadata->ndim, offset, size);
        pdcid_t remote_region = PDCregion_create(cur_metadata->ndim, offset, size);
        pdcid_t local_obj_id  = PDCobj_open(cur_metadata->obj_name, pdc_id_g);
        pdcid_t transfer_request =
            PDCregion_transfer_create(data_buf, PDC_READ, local_obj_id, local_region, remote_region);
        PDCregion_transfer_start(transfer_request);
        PDCregion_transfer_wait(transfer_request);

        // write data from buffer into dataset
        H5Dwrite(dset_id, data_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_buf);

        // close dataset
        H5Dclose(dset_id);

        prev_cont_id      = cur_metadata->cont_id;
        cur_metadata_node = cur_metadata_node->next;
    }
    H5Gclose(group_id);
    H5Fclose(file_id);
}
