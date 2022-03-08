#include <stdio.h>
#include <string.h>

#include "../src/api/pdc.h"
#include "../src/server/pdc_utlist.h"
#include "../src/server/pdc_hash-table.h"
#include "../src/api/pdc_analysis_pkg.h"
#include "../src/api/pdc_client_server_common.h"
#include "../src/api/pdc_transforms_common.h"
#include "../src/server/pdc_server.h"
#include "../src/server/pdc_server_metadata.h"
#include "../src/server/pdc_server_data.h"
#include "../src/api/pdc_timing.h"

// Global hash table for storing metadata
// HashTable *metadata_hash_table_g  = NULL;
// HashTable *container_hash_table_g = NULL;

int               pdc_server_rank_g            = 0;

double total_mem_usage_g = 0.0;

static void pdc_ls(char *filename);

int main(int argc, char *argv[] )  {
    if (argc == 1) {
        printf("No arguments provided.\n");
        return 0;
    } else {
        int arg_index = 1;
        while (arg_index < argc) {
            if (strcmp(argv[arg_index], "pdc_ls") == 0) {
                arg_index++;
                if (arg_index >= argc) {
                    printf("Expected filename.\n");
                }
                pdc_ls(argv[arg_index]);
            }
            arg_index++;
        }
    }
}

int
region_cmp(region_list_t *a, region_list_t *b)
{
    int unit_size = a->ndim * sizeof(uint64_t);
    return memcmp(a->start, b->start, unit_size);
}

void pdc_ls(char *filename) {
    perr_t ret_value = SUCCEED;
    int    n_entry, count, i, j, nobj = 0, all_nobj = 0, all_n_region, n_region, total_region = 0, n_kvtag,
                              key_len;
    int                          n_cont, all_cont;
    pdc_metadata_t *             metadata, *elt;
    region_list_t *              region_list;
    pdc_hash_table_entry_head *  entry;
    pdc_cont_hash_table_entry_t *cont_entry;
    uint32_t *                   hash_key;
    unsigned                     idx;

    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("==PDC_SERVER[%d]: %s -  Checkpoint file open FAILED [%s]!", pdc_server_rank_g, __func__,
               filename);
        ret_value = FAIL;
        goto done;
    }

    char *slurm_jobid = getenv("SLURM_JOB_ID");
    if (slurm_jobid == NULL) {
        printf("Error getting slurm job id from SLURM_JOB_ID!\n");
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
        total_mem_usage_g += sizeof(uint32_t);

        // Reconstruct hash table
        cont_entry = (pdc_cont_hash_table_entry_t *)malloc(sizeof(pdc_cont_hash_table_entry_t));
        total_mem_usage_g += sizeof(pdc_cont_hash_table_entry_t);
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
        total_mem_usage_g += sizeof(uint32_t);

        // Reconstruct hash table
        entry           = (pdc_hash_table_entry_head *)malloc(sizeof(pdc_hash_table_entry_head));
        entry->n_obj    = 0;
        entry->bloom    = NULL;
        entry->metadata = NULL;

        metadata = (pdc_metadata_t *)calloc(sizeof(pdc_metadata_t), count);
        for (i = 0; i < count; i++) {
            if (fread(metadata + i, sizeof(pdc_metadata_t), 1, file) != 1) {
                printf("Read failed for metadata\n");
            }

            (metadata + i)->storage_region_list_head       = NULL;
            (metadata + i)->region_lock_head               = NULL;
            (metadata + i)->region_map_head                = NULL;
            (metadata + i)->region_buf_map_head            = NULL;
            (metadata + i)->bloom                          = NULL;
            (metadata + i)->prev                           = NULL;
            (metadata + i)->next                           = NULL;
            (metadata + i)->kvtag_list_head                = NULL;
            (metadata + i)->all_storage_region_distributed = 0;

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
                goto done;
            }

            /* if (n_region == 0) */
            /*     continue; */

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
                    if (fread(region_list->region_hist->bin, sizeof(uint64_t), region_list->region_hist->nbin,
                              file) != 1) {
                        printf("Read failed for region_list->region_hist->bin\n");
                    }
                    if (fread(&region_list->region_hist->incr, sizeof(double), 1, file) != 1) {
                        printf("Read failed for region_list->region_hist->incr\n");
                    }
                }

                region_list->buf       = NULL;
                region_list->data_size = 1;
                for (idx = 0; idx < region_list->ndim; idx++)
                    region_list->data_size *= region_list->count[idx];
                region_list->is_data_ready            = 0;
                region_list->shm_fd                   = 0;
                region_list->meta                     = (metadata + i);
                region_list->prev                     = NULL;
                region_list->next                     = NULL;
                region_list->overlap_storage_regions  = NULL;
                region_list->n_overlap_storage_region = 0;
                hg_atomic_init32(&(region_list->buf_map_refcount), 0);
                region_list->reg_dirty_from_buf = 0;
                region_list->access_type        = PDC_NA;
                region_list->bulk_handle        = NULL;
                region_list->lock_handle        = NULL;
                region_list->addr               = NULL;
                region_list->obj_id             = (metadata + i)->obj_id;
                region_list->reg_id             = 0;
                region_list->from_obj_id        = 0;
                region_list->client_id          = 0;
                region_list->is_io_done         = 0;
                region_list->is_shm_closed      = 0;
                region_list->seq_id             = 0;
                region_list->sent_to_server     = 0;
                region_list->io_cache_region    = NULL;

                memset(region_list->shm_addr, 0, ADDR_MAX);
                memset(region_list->client_ids, 0, PDC_SERVER_MAX_PROC_PER_NODE * sizeof(uint32_t));

                if (strstr(region_list->storage_location, "/global/cscratch") != NULL) {
                    region_list->data_loc_type = PDC_LUSTRE;
                }

            } // For j

            // read storage region info
            if (fread(&n_region, sizeof(int), 1, file) != 1) {
                printf("Read failed for n_region\n");
            }
            data_server_region_t *new_obj_reg =
                (data_server_region_t *)calloc(1, sizeof(struct data_server_region_t));
            new_obj_reg->fd               = -1;
            new_obj_reg->storage_location = (char *)malloc(sizeof(char) * ADDR_MAX);
            new_obj_reg->obj_id = (metadata + i)->obj_id;
            for (j = 0; j < n_region; j++) {
                region_list_t *new_region_list = (region_list_t *)malloc(sizeof(region_list_t));
                if (fread(new_region_list, sizeof(region_list_t), 1, file) != 1) {
                    printf("Read failed for new_region_list\n");
                }
            }

            total_region += n_region;

            DL_SORT((metadata + i)->storage_region_list_head, region_cmp);
        } // For i

        nobj += count;
        total_mem_usage_g += sizeof(pdc_hash_table_entry_head);
        total_mem_usage_g += (sizeof(pdc_metadata_t) * count);

        entry->metadata = NULL;

        n_entry--;
    }

    fclose(file);
    file = NULL;

#ifdef ENABLE_MPI
    MPI_Reduce(&nobj, &all_nobj, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&total_region, &all_n_region, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_nobj          = nobj;
    all_n_region      = total_region;
#endif

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER[0]: Server restarted from saved session, "
               "successfully loaded %d containers, %d objects, %d regions...\n",
               all_cont, all_nobj, all_n_region);
    }

done:
    fflush(stdout);
}
