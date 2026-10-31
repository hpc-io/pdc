#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <regex.h>
#include <time.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "pdc.h"
#include "pdc_client_server_common.h"
#include "../src/server/include/pdc_server_metadata.h"
#include "cjson/cJSON.h"

const char *avail_args[] = {"-n", "-i", "-json", "-ln", "-li", "-s", "-tree"};
const int   num_args     = 7;

typedef struct pdc_region_metadata_pkg {
    uint64_t *                      reg_offset;
    uint64_t *                      reg_size;
    uint32_t                        data_server_id;
    struct pdc_region_metadata_pkg *next;
} pdc_region_metadata_pkg;

typedef struct pdc_obj_metadata_pkg {
    int                          ndim;
    uint64_t                     obj_id;
    pdc_region_metadata_pkg *    regions;
    pdc_region_metadata_pkg *    regions_end;
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
    char *                         buf;
    struct pdc_metadata_query_buf *next;
} pdc_metadata_query_buf;

typedef struct RegionNode {
    region_list_t *    region_list;
    struct RegionNode *next;
} RegionNode;

typedef struct MetadataNode {
    pdc_metadata_t *      metadata_ptr;
    struct MetadataNode * next;
    RegionNode *          region_list_head;
    pdc_obj_metadata_pkg *obj_metadata_pkg;
} MetadataNode;

typedef struct FileNameNode {
    char *               file_name;
    struct FileNameNode *next;
} FileNameNode;

// Holds a fully-read container entry (including its obj_ids/entry_types
// membership arrays), so the nested container hierarchy can be reconstructed
// and printed after all checkpoint files have been read.
typedef struct ContNode {
    pdc_cont_hash_table_entry_t *cont_entry;
    struct ContNode *            next;
} ContNode;

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

int
isDir(const char *fileName)
{
    struct stat path;
    stat(fileName, &path);
    return S_ISREG(path.st_mode);
}

static void pdc_ls(FileNameNode *file_name_node, int argc, char *argv[]);

int
main(int argc, char *argv[])
{
    if (argc == 1) {
        LOG_JUST_PRINT(
            "Usage: ./pdc_ls pdc_checkpoint_directory/file [-n obj_name] [-i obj_id] [-json json_fname] "
            "[-ln (list all names)] [-ls (list all ids)] [-s (summary)] "
            "[-tree (print nested container hierarchy)]\n");
        return 0;
    }
    else {
        FileNameNode * head     = NULL;
        FileNameNode * cur_node = NULL;
        DIR *          d;
        struct dirent *dir;
        struct dirent *direc;
        d = opendir(argv[1]);
        char *full_path;

        if (d) {
            while ((dir = readdir(d)) != NULL) {
                // if it's directory
                if (!isDir(dir->d_name)) {
                    if (strstr(dir->d_name, ".")) {
                        // ignore parent and current directories
                        continue;
                    }
                    // appends path together
                    char tmp[1024];
                    sprintf(tmp, "%s/%s", argv[1], dir->d_name);
                    DIR *d1 = opendir(tmp);

                    while ((direc = readdir(d1)) != NULL) { // go into it and go for checkpoint files again
                        if (strstr(direc->d_name, "metadata_checkpoint.")) {
                            char last = argv[1][strlen(argv[1]) - 1];
                            if (last == '/') {
                                full_path = (char *)malloc(sizeof(char) *
                                                           (strlen(argv[1]) + strlen(direc->d_name) + 1));
                                strcpy(full_path, argv[1]);
                                strcat(full_path, direc->d_name);
                                strcat(full_path, "/");
                                strcat(full_path, direc->d_name);
                            }
                            else {
                                full_path = (char *)malloc(sizeof(char) *
                                                           (strlen(argv[1]) + strlen(direc->d_name) + 2));
                                strcpy(full_path, argv[1]);
                                strcat(full_path, "/");
                                strcat(full_path, dir->d_name);
                                strcat(full_path, "/");
                                strcat(full_path, direc->d_name);
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
                    closedir(d1);
                }
                if (strstr(dir->d_name, "metadata_checkpoint.")) {
                    LOG_INFO("%s\n", dir->d_name);
                    char last = argv[1][strlen(argv[1]) - 1];
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
            } // End if dir = readdir
            closedir(d);
        } // Ene if d
        else {
            // Open one checkpoint file
            FILE *file = fopen(argv[1], "r");
            if (file != NULL) {
                FileNameNode *new_node = (FileNameNode *)malloc(sizeof(FileNameNode));
                full_path              = (char *)malloc(sizeof(char) * (strlen(argv[1]) + 1));
                strcpy(full_path, argv[1]);
                new_node->file_name = full_path;
                new_node->next      = NULL;
                head                = new_node;
                cur_node            = new_node;
                fclose(file);
            }
        }

        if (head == NULL) {
            LOG_ERROR("Unable to open/locate checkpoint file(s).\n");
            return -1;
        }
        else {
            pdc_ls(head, argc, argv);
        }
    }
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
    char *result = get_enum_name_by_dtype(data_type);
    if (result == NULL) {
        return "NULL";
    }
    return result;
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

static pdc_metadata_t *
pdc_ls_find_metadata_by_id(MetadataNode *head, uint64_t obj_id)
{
    while (head != NULL) {
        if (head->metadata_ptr->obj_id == obj_id)
            return head->metadata_ptr;
        head = head->next;
    }
    return NULL;
}

static ContNode *
find_cont_node_by_id(ContNode *head, uint64_t cont_id)
{
    while (head != NULL) {
        if (head->cont_entry->cont_id == cont_id)
            return head;
        head = head->next;
    }
    return NULL;
}

// A container is "top-level" (a tree root to start printing from) unless
// some other container lists it as a PDC_ENTITY_CONT entry.
static int
is_nested_under_another(ContNode *all_conts, uint64_t cont_id)
{
    ContNode *c = all_conts;
    while (c != NULL) {
        pdc_cont_hash_table_entry_t *e = c->cont_entry;
        int                          i;
        for (i = 0; i < e->n_obj; i++) {
            if (e->obj_ids[i] == cont_id && e->entry_types[i] == PDC_ENTITY_CONT)
                return 1;
        }
        c = c->next;
    }
    return 0;
}

static void
print_container_tree(ContNode *all_conts, pdc_cont_hash_table_entry_t *cont, MetadataNode *meta_head,
                     int depth, FILE *fp)
{
    int i, d;

    for (d = 0; d < depth; d++)
        fprintf(fp, "  ");
    fprintf(fp, "%s (cont_id=%" PRIu64 ")\n", cont->cont_name, cont->cont_id);

    for (i = 0; i < cont->n_obj; i++) {
        if (cont->obj_ids[i] == 0) // deleted entry
            continue;

        if (cont->entry_types[i] == PDC_ENTITY_CONT) {
            // The recursive call prints its own indented heading line, so
            // don't print an indent for this entry ourselves.
            ContNode *child = find_cont_node_by_id(all_conts, cont->obj_ids[i]);
            if (child != NULL) {
                print_container_tree(all_conts, child->cont_entry, meta_head, depth + 1, fp);
                continue;
            }
            for (d = 0; d < depth + 1; d++)
                fprintf(fp, "  ");
            fprintf(fp, "[container, unresolved] cont_id=%" PRIu64 "\n", cont->obj_ids[i]);
        }
        else {
            pdc_metadata_t *m = pdc_ls_find_metadata_by_id(meta_head, cont->obj_ids[i]);
            for (d = 0; d < depth + 1; d++)
                fprintf(fp, "  ");
            if (m != NULL)
                fprintf(fp, "[object] %s (obj_id=%" PRIu64 ")\n", m->obj_name, cont->obj_ids[i]);
            else
                fprintf(fp, "[object, unresolved] obj_id=%" PRIu64 "\n", cont->obj_ids[i]);
        }
    }
}

static void
print_all_container_trees(ContNode *all_conts, MetadataNode *meta_head, FILE *fp)
{
    ContNode *c = all_conts;

    if (all_conts == NULL) {
        fprintf(fp, "No containers found.\n");
        return;
    }

    fprintf(fp, "Container hierarchy:\n");
    while (c != NULL) {
        if (!is_nested_under_another(all_conts, c->cont_entry->cont_id))
            print_container_tree(all_conts, c->cont_entry, meta_head, 0, fp);
        c = c->next;
    }
}

pdc_obj_metadata_pkg *
do_transfer_request_metadata(int pdc_server_size_input, char *checkpoint)
{
    char *ptr;
    int   n_objs, reg_count;
    int   i, j;

    pdc_obj_metadata_pkg *  metadata_server_objs     = NULL;
    pdc_obj_metadata_pkg *  metadata_server_objs_end = NULL;
    pdc_metadata_query_buf *metadata_query_buf_head  = NULL;
    pdc_metadata_query_buf *metadata_query_buf_end   = NULL;
    int                     pdc_server_size          = pdc_server_size_input;
    uint64_t *              data_server_bytes        = (uint64_t *)calloc(pdc_server_size, sizeof(uint64_t));
    uint64_t                query_id_g               = 100000;
    ptr                                              = checkpoint;

    if (checkpoint) {
        n_objs = *(int *)ptr;
        ptr += sizeof(int);
        for (i = 0; i < n_objs; ++i) {
            if (metadata_server_objs) {
                metadata_server_objs_end->next = (pdc_obj_metadata_pkg *)malloc(sizeof(pdc_obj_metadata_pkg));
                metadata_server_objs_end       = metadata_server_objs_end->next;
            }
            else {
                metadata_server_objs     = (pdc_obj_metadata_pkg *)malloc(sizeof(pdc_obj_metadata_pkg));
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
    char *wanted_name      = NULL;
    int   wanted_id        = 0;
    char *output_file_name = NULL;
    int   list_names       = 0;
    int   list_ids         = 0;
    int   summary          = 0;
    int   print_tree       = 0;
    int   print_all        = 1;

    int arg_index = 2;
    while (arg_index < argc) {
        if (is_arg(argv[arg_index]) == 0) {
            LOG_ERROR("Improperly formatted argument(s).\n");
            return;
        }
        if (strcmp(argv[arg_index], "-n") == 0) {
            arg_index++;
            wanted_name = argv[arg_index];
        }
        else if (strcmp(argv[arg_index], "-i") == 0) {
            arg_index++;
            wanted_id = atoi(argv[arg_index]);
        }
        else if (strcmp(argv[arg_index], "-json") == 0) {
            arg_index++;
            output_file_name = argv[arg_index];
        }
        else if (strcmp(argv[arg_index], "-ln") == 0) {
            list_names = 1;
            print_all  = 0;
        }
        else if (strcmp(argv[arg_index], "-li") == 0) {
            list_ids  = 1;
            print_all = 0;
        }
        else if (strcmp(argv[arg_index], "-s") == 0) {
            summary   = 1;
            print_all = 0;
        }
        else if (strcmp(argv[arg_index], "-tree") == 0) {
            print_tree = 1;
            print_all  = 0;
        }
        arg_index++;
    }
    ArrayList *obj_names, *obj_ids;
    if (list_names) {
        obj_names = newList();
    }
    if (list_ids) {
        obj_ids = newList();
    }

    char *        filename;
    MetadataNode *metadata_head   = NULL;
    RegionNode *  cur_region_node = NULL;
    FileNameNode *cur_file_node   = file_name_node;
    ContNode *    cont_head       = NULL;
    ContNode *    cont_tail       = NULL;

    int all_cont_total     = 0;
    int all_nobj_total     = 0;
    int all_n_region_total = 0;

    struct stat attr;
    regex_t     regex;
    int         reti;

    while (cur_file_node != NULL) {
        filename = cur_file_node->file_name;
        stat(filename, &attr);
        LOG_INFO("[INFO] File [%s] last modified at: %s", filename, ctime(&attr.st_mtime));
        // Start server restart code
        perr_t ret_value = SUCCEED;
        int    n_entry, count, i, j, nobj = 0, all_nobj = 0, all_n_region, n_region, n_objs, total_region = 0,
                                  n_kvtag, key_len;
        int                          n_cont, all_cont;
        pdc_metadata_t *             metadata, *elt;
        region_list_t *              region_list;
        uint32_t *                   hash_key;
        unsigned                     idx;
        pdc_cont_hash_table_entry_t *cont_entry;
        pdc_hash_table_entry_head *  entry;

        FILE *file = fopen(filename, "r");
        if (file == NULL) {
            LOG_ERROR("Checkpoint file open FAILED [%s]", filename);
            ret_value = FAIL;
            continue;
        }

        if (fread(&n_cont, sizeof(int), 1, file) != 1) {
            LOG_ERROR("Read failed for cont count\n");
        }
        all_cont = n_cont;
        while (n_cont > 0) {
            hash_key = (uint32_t *)malloc(sizeof(uint32_t));
            if (fread(hash_key, sizeof(uint32_t), 1, file) != 1) {
                LOG_ERROR("Read failed for cont hash_key\n");
                return;
            }

            // Reconstruct the container entry. Its fields are written
            // individually (not as a raw struct dump), since obj_ids/
            // entry_types/kvtag_list_head are heap pointers private to the
            // server process that wrote the checkpoint.
            cont_entry = (pdc_cont_hash_table_entry_t *)calloc(1, sizeof(pdc_cont_hash_table_entry_t));
            if (fread(&cont_entry->cont_id, sizeof(uint64_t), 1, file) != 1) {
                LOG_ERROR("Read failed for cont_entry cont_id\n");
                return;
            }
            if (fread(cont_entry->cont_name, sizeof(cont_entry->cont_name), 1, file) != 1) {
                LOG_ERROR("Read failed for cont_entry cont_name\n");
                return;
            }
            if (fread(&cont_entry->n_obj, sizeof(int), 1, file) != 1) {
                LOG_ERROR("Read failed for cont_entry n_obj\n");
                return;
            }
            if (fread(&cont_entry->n_deleted, sizeof(int), 1, file) != 1) {
                LOG_ERROR("Read failed for cont_entry n_deleted\n");
                return;
            }
            if (fread(cont_entry->tags, sizeof(cont_entry->tags), 1, file) != 1) {
                LOG_ERROR("Read failed for cont_entry tags\n");
                return;
            }
            if (cont_entry->n_obj > 0) {
                cont_entry->obj_ids     = (uint64_t *)malloc(sizeof(uint64_t) * cont_entry->n_obj);
                cont_entry->entry_types = (uint8_t *)malloc(sizeof(uint8_t) * cont_entry->n_obj);
                if (fread(cont_entry->obj_ids, sizeof(uint64_t) * cont_entry->n_obj, 1, file) != 1) {
                    LOG_ERROR("Read failed for cont_entry obj_ids\n");
                    return;
                }
                if (fread(cont_entry->entry_types, sizeof(uint8_t) * cont_entry->n_obj, 1, file) != 1) {
                    LOG_ERROR("Read failed for cont_entry entry_types\n");
                    return;
                }
            }

            {
                ContNode *cont_node   = (ContNode *)malloc(sizeof(ContNode));
                cont_node->cont_entry = cont_entry;
                cont_node->next       = NULL;
                if (cont_head == NULL) {
                    cont_head = cont_node;
                    cont_tail = cont_node;
                }
                else {
                    cont_tail->next = cont_node;
                    cont_tail       = cont_node;
                }
            }

            n_cont--;
        } // End while
        if (fread(&n_entry, sizeof(int), 1, file) != 1) {
            LOG_ERROR("Read failed for n_entry\n");
        }

        while (n_entry > 0) {
            if (fread(&count, sizeof(int), 1, file) != 1) {
                LOG_ERROR("Read failed for obj count\n");
                return;
            }

            hash_key = (uint32_t *)malloc(sizeof(uint32_t));
            if (fread(hash_key, sizeof(uint32_t), 1, file) != 1) {
                LOG_ERROR("Read failed for obj hash_key\n");
                return;
            }

            // Reconstruct hash table
            entry           = (pdc_hash_table_entry_head *)malloc(sizeof(pdc_hash_table_entry_head));
            entry->n_obj    = 0;
            entry->bloom    = NULL;
            entry->metadata = NULL;

            metadata = (pdc_metadata_t *)calloc(sizeof(pdc_metadata_t), count);
            for (i = 0; i < count; i++) {
                if (fread(metadata + i, sizeof(pdc_metadata_t), 1, file) != 1) {
                    LOG_ERROR("Read failed for metadata\n");
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
                    LOG_ERROR("Read failed for n_kvtag\n");
                }
                for (j = 0; j < n_kvtag; j++) {
                    pdc_kvtag_list_t *kvtag_list = (pdc_kvtag_list_t *)calloc(1, sizeof(pdc_kvtag_list_t));
                    kvtag_list->kvtag            = (pdc_kvtag_t *)malloc(sizeof(pdc_kvtag_t));
                    if (fread(&key_len, sizeof(int), 1, file) != 1) {
                        LOG_ERROR("Read failed for key_len\n");
                    }
                    kvtag_list->kvtag->name = malloc(key_len);
                    if (fread((void *)(kvtag_list->kvtag->name), key_len, 1, file) != 1) {
                        LOG_ERROR("Read failed for kvtag_list->kvtag->name\n");
                    }
                    if (fread(&kvtag_list->kvtag->size, sizeof(uint32_t), 1, file) != 1) {
                        LOG_ERROR("Read failed for kvtag_list->kvtag->size\n");
                    }
                    if (fread(&kvtag_list->kvtag->type, sizeof(int8_t), 1, file) != 1) {
                        LOG_ERROR("Read failed for kvtag_list->kvtag->size\n");
                    }
                    kvtag_list->kvtag->value = malloc(kvtag_list->kvtag->size);
                    if (fread(kvtag_list->kvtag->value, kvtag_list->kvtag->size, 1, file) != 1) {
                        LOG_ERROR("Read failed for kvtag_list->kvtag->value\n");
                    }
                }

                if (fread(&n_region, sizeof(int), 1, file) != 1) {
                    LOG_ERROR("Read failed for n_region\n");
                }
                if (n_region < 0) {
                    LOG_ERROR("Checkpoint file region number error\n");
                    ret_value = FAIL;
                    continue;
                }

                total_region += n_region;

                for (j = 0; j < n_region; j++) {
                    region_list = (region_list_t *)malloc(sizeof(region_list_t));
                    if (fread(region_list, sizeof(region_list_t), 1, file) != 1) {
                        LOG_ERROR("Read failed for region_list\n");
                    }

                    int has_hist = 0;
                    if (fread(&has_hist, sizeof(int), 1, file) != 1) {
                        LOG_ERROR("Read failed for has_list\n");
                    }
                    if (has_hist == 1) {
                        region_list->region_hist = (pdc_histogram_t *)malloc(sizeof(pdc_histogram_t));
                        if (fread(&region_list->region_hist->dtype, sizeof(int), 1, file) != 1) {
                            LOG_ERROR("Read failed for region_list->region_hist->dtype\n");
                        }
                        if (fread(&region_list->region_hist->nbin, sizeof(int), 1, file) != 1) {
                            LOG_ERROR("Read failed for region_list->region_hist->nbin\n");
                        }
                        if (region_list->region_hist->nbin == 0) {
                            LOG_ERROR("Checkpoint file histogram size is 0\n");
                        }

                        region_list->region_hist->range =
                            (double *)malloc(sizeof(double) * region_list->region_hist->nbin * 2);
                        region_list->region_hist->bin =
                            (uint64_t *)malloc(sizeof(uint64_t) * region_list->region_hist->nbin);

                        if (fread(region_list->region_hist->range, sizeof(double),
                                  region_list->region_hist->nbin * 2, file) != 1) {
                            LOG_ERROR("Read failed for region_list->region_hist->range\n");
                        }
                        if (fread(region_list->region_hist->bin, sizeof(uint64_t),
                                  region_list->region_hist->nbin, file) != 1) {
                            LOG_ERROR("Read failed for region_list->region_hist->bin\n");
                        }
                        if (fread(&region_list->region_hist->incr, sizeof(double), 1, file) != 1) {
                            LOG_ERROR("Read failed for region_list->region_hist->incr\n");
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
            LOG_ERROR("Read failed for n_objs\n");
        }

        for (i = 0; i < n_objs; ++i) {
            data_server_region_t *new_obj_reg =
                (data_server_region_t *)calloc(1, sizeof(struct data_server_region_t));
            new_obj_reg->fd               = -1;
            new_obj_reg->storage_location = (char *)malloc(sizeof(char) * ADDR_MAX);
            if (fread(&new_obj_reg->obj_id, sizeof(uint64_t), 1, file) != 1) {
                LOG_ERROR("Read failed for obj_id\n");
            }
            if (fread(&n_region, sizeof(int), 1, file) != 1) {
                LOG_ERROR("Read failed for n_region\n");
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
                    LOG_ERROR("Read failed for new_region_list\n");
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
        char *   checkpoint_buf;

        if (fread(&checkpoint_size, sizeof(uint64_t), 1, file) != 1) {
            LOG_ERROR("Read failed for checkpoint size\n");
        }

        checkpoint_buf = (char *)malloc(checkpoint_size);

        if (fread(checkpoint_buf, checkpoint_size, 1, file) != 1) {
            LOG_ERROR("Read failed for checkpoint buf\n");
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

    // Create JSON
    MetadataNode *  cur_m_node = metadata_head;
    RegionNode *    cur_r_node;
    pdc_metadata_t *cur_metadata;
    region_list_t * cur_region;
    char *          data_type;
    int             add_obj;
    cJSON *         cont_id_json     = NULL;
    cJSON *         cur_obj_json     = NULL;
    cJSON *         dim_arr_json     = NULL;
    cJSON *         dim_ent_json     = NULL;
    cJSON *         region_arr_json  = NULL;
    cJSON *         region_info_json = NULL;
    cJSON *         count_arr_json   = NULL;
    cJSON *         start_arr_json   = NULL;
    cJSON *         output           = cJSON_CreateObject();
    int             prev_cont_id     = -1;
    char            buf[1024];
    while (cur_m_node != NULL) {
        cur_metadata = cur_m_node->metadata_ptr;
        if (prev_cont_id != cur_metadata->cont_id) {
            cont_id_json = cJSON_CreateArray();
            sprintf(buf, "cont_id: %d", cur_metadata->cont_id);
            cJSON_AddItemToObject(output, buf, cont_id_json);
        }
        add_obj = 1;
        if (wanted_name && wanted_id) {
            int matched_name = 0;
            int matched_id   = 0;
            reti             = regcomp(&regex, wanted_name, 0);
            if (reti) {
                if (strcmp(wanted_name, cur_metadata->obj_name) == 0) {
                    matched_name = 1;
                }
            }
            else {
                reti = regexec(&regex, cur_metadata->obj_name, 0, NULL, 0);
                if (!reti) {
                    matched_name = 1;
                }
            }

            sprintf(buf, "%d", wanted_id);
            reti = regcomp(&regex, buf, 0);
            if (reti) {
                if (wanted_id == cur_metadata->obj_id) {
                    matched_id = 1;
                }
            }
            else {
                sprintf(buf, "%d", cur_metadata->obj_id);
                reti = regexec(&regex, buf, 0, NULL, 0);
                if (!reti) {
                    matched_id = 1;
                }
            }
            if (matched_name == 0 || matched_id == 0) {
                add_obj = 0;
            }
        }
        else if (wanted_name) {
            int matched_name = 0;
            reti             = regcomp(&regex, wanted_name, 0);
            if (reti) {
                if (strcmp(wanted_name, cur_metadata->obj_name) == 0) {
                    matched_name = 1;
                }
            }
            else {
                reti = regexec(&regex, cur_metadata->obj_name, 0, NULL, 0);
                if (!reti) {
                    matched_name = 1;
                }
            }
            add_obj = matched_name;
        }
        else if (wanted_id) {
            int matched_id = 0;
            sprintf(buf, "%d", wanted_id);
            reti = regcomp(&regex, buf, 0);
            if (reti) {
                if (wanted_id == cur_metadata->obj_id) {
                    matched_id = 1;
                }
            }
            else {
                sprintf(buf, "%d", cur_metadata->obj_id);
                reti = regexec(&regex, buf, 0, NULL, 0);
                if (!reti) {
                    matched_id = 1;
                }
            }
            add_obj = matched_id;
        }
        if (add_obj) {
            if (list_names) {
                add(obj_names, cur_metadata->obj_name);
            }
            if (list_ids) {
                sprintf(buf, "%d", cur_metadata->obj_id);
                add(obj_ids, buf);
            }
            cur_obj_json = cJSON_CreateObject();
            cJSON_AddNumberToObject(cur_obj_json, "obj_id", cur_metadata->obj_id);
            cJSON_AddStringToObject(cur_obj_json, "app_name", cur_metadata->app_name);
            cJSON_AddStringToObject(cur_obj_json, "obj_name", cur_metadata->obj_name);
            cJSON_AddNumberToObject(cur_obj_json, "user_id", cur_metadata->user_id);
            cJSON_AddStringToObject(cur_obj_json, "tags", cur_metadata->tags);
            data_type = get_data_type(cur_metadata->data_type);
            cJSON_AddStringToObject(cur_obj_json, "data_type", data_type);
            cJSON_AddNumberToObject(cur_obj_json, "num_dims", cur_metadata->ndim);
            int dims[cur_metadata->ndim];
            for (int i = 0; i < (cur_metadata->ndim); i++) {
                dims[i] = (cur_metadata->dims)[i];
            }
            dim_arr_json = cJSON_CreateIntArray(dims, cur_metadata->ndim);
            cJSON_AddItemToObject(cur_obj_json, "dims", dim_arr_json);
            cJSON_AddNumberToObject(cur_obj_json, "time_step", cur_metadata->time_step);

            region_arr_json = cJSON_CreateArray();
            cur_r_node      = cur_m_node->region_list_head;
            while (cur_r_node != NULL) {
                cur_region       = cur_r_node->region_list;
                region_info_json = cJSON_CreateObject();
                cJSON_AddStringToObject(region_info_json, "storage_loc", cur_region->storage_location);
                cJSON_AddNumberToObject(region_info_json, "offset", cur_region->offset);
                cJSON_AddNumberToObject(region_info_json, "num_dims", cur_region->ndim);
                for (int i = 0; i < (cur_metadata->ndim); i++) {
                    dims[i] = (cur_region->start)[i];
                }
                start_arr_json = cJSON_CreateIntArray(dims, cur_region->ndim);
                cJSON_AddItemToObject(region_info_json, "start", start_arr_json);
                for (int i = 0; i < (cur_metadata->ndim); i++) {
                    dims[i] = (cur_region->count)[i];
                }
                count_arr_json = cJSON_CreateIntArray(dims, cur_region->ndim);
                cJSON_AddItemToObject(region_info_json, "count", count_arr_json);
                cJSON_AddNumberToObject(region_info_json, "unit_size", cur_region->unit_size);
                data_type = get_data_loc_type(cur_region->data_loc_type);
                cJSON_AddStringToObject(region_info_json, "data_loc_type", data_type);
                cJSON_AddItemToArray(region_arr_json, region_info_json);
                cur_r_node = cur_r_node->next;
            }
            cJSON_AddItemToObject(cur_obj_json, "region_list_info", region_arr_json);

            cJSON_AddItemToArray(cont_id_json, cur_obj_json);
        }

        prev_cont_id = cur_metadata->cont_id;
        cur_m_node   = cur_m_node->next;
    }

    FILE *fp;
    if (output_file_name) {
        fp = fopen(output_file_name, "w");
        LOG_INFO("Output to [%s]\n", output_file_name);
    }
    else {
        fp = stdout;
    }
    if (list_names) {
        cJSON *all_names_json = cJSON_CreateStringArray((const char **)obj_names->items, obj_names->length);
        cJSON_AddItemToObject(output, "all_obj_names", all_names_json);
    }
    if (list_ids) {
        int id_arr[obj_ids->length];
        for (int i = 0; i < obj_ids->length; i++) {
            id_arr[i] = atoi(obj_ids->items[i]);
        }
        cJSON *all_ids_json = cJSON_CreateIntArray(id_arr, obj_ids->length);
        cJSON_AddItemToObject(output, "all_obj_ids", all_ids_json);
    }
    if (summary) {
        sprintf(buf, "pdc_ls found: %d containers, %d objects, %d regions", all_cont_total, all_nobj_total,
                all_n_region_total);
        cJSON_AddStringToObject(output, "summary", buf);
    }

    // Always include each container's raw entry list (objects and/or nested
    // containers) in the JSON, regardless of -tree.
    {
        cJSON *   containers_json = cJSON_CreateArray();
        ContNode *cn              = cont_head;
        int       ci;
        while (cn != NULL) {
            pdc_cont_hash_table_entry_t *ce        = cn->cont_entry;
            cJSON *                      cont_json = cJSON_CreateObject();
            cJSON_AddNumberToObject(cont_json, "cont_id", (double)ce->cont_id);
            cJSON_AddStringToObject(cont_json, "cont_name", ce->cont_name);
            cJSON *entries_json = cJSON_CreateArray();
            for (ci = 0; ci < ce->n_obj; ci++) {
                if (ce->obj_ids[ci] == 0) // deleted entry
                    continue;
                cJSON *entry_json = cJSON_CreateObject();
                cJSON_AddNumberToObject(entry_json, "id", (double)ce->obj_ids[ci]);
                cJSON_AddStringToObject(entry_json, "type",
                                        ce->entry_types[ci] == PDC_ENTITY_CONT ? "container" : "object");
                cJSON_AddItemToArray(entries_json, entry_json);
            }
            cJSON_AddItemToObject(cont_json, "entries", entries_json);
            cJSON_AddItemToArray(containers_json, cont_json);
            cn = cn->next;
        }
        cJSON_AddItemToObject(output, "containers", containers_json);
    }

    if (print_tree) {
        print_all_container_trees(cont_head, metadata_head, fp);
    }

    if (output_file_name) {
        fclose(fp);
    }
    char *json_string = cJSON_Print(output);
    fprintf(fp, json_string);
    fprintf(fp, "\n");
    if (output_file_name)
        fclose(fp);
}
