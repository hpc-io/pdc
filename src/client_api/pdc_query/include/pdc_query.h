#ifndef PDC_QUERY_H
#define PDC_QUERY_H

#include "pdc_public.h"
#include "pdc_obj.h"

enum pdc_prop_name_t { PDC_OBJ_NAME, PDC_CONT_NAME, PDC_APP_NAME, PDC_USER_ID };

/*************************************/
/* Public Type and Struct Definition */
/*************************************/
typedef enum { PDC_OP_NONE = 0, PDC_GT = 1, PDC_LT = 2, PDC_GTE = 3, PDC_LTE = 4, PDC_EQ = 5 } pdc_query_op_t;

typedef enum { PDC_QUERY_NONE = 0, PDC_QUERY_AND = 1, PDC_QUERY_OR = 2 } pdc_query_combine_op_t;

typedef enum {
    PDC_QUERY_GET_NONE  = 0,
    PDC_QUERY_GET_NHITS = 1,
    PDC_QUERY_GET_SEL   = 2,
    PDC_QUERY_GET_DATA  = 3
} pdc_query_get_op_t;

typedef struct pdcquery_selection_t {
    pdcid_t   query_id;
    size_t    ndim;
    uint64_t  nhits;
    uint64_t *coords;
    uint64_t  coords_alloc;
} pdc_selection_t;

typedef struct pdc_query_constraint_t {
    pdcid_t          obj_id;
    pdc_query_op_t   op;
    pdc_var_type_t   type;
    double           value; // Use it as a generic 64bit value
    pdc_histogram_t *hist;

    int            is_range;
    pdc_query_op_t op2;
    double         value2;

    void *  storage_region_list_head;
    pdcid_t origin_server;
    int     n_sent;
    int     n_recv;
} pdc_query_constraint_t;

typedef struct pdc_query_t {
    pdc_query_constraint_t *constraint;
    struct pdc_query_t *    left;
    struct pdc_query_t *    right;
    pdc_query_combine_op_t  combine_op;
    struct pdc_region_info *region;            // used only on client
    void *                  region_constraint; // used only on server
    pdc_selection_t *       sel;
} pdc_query_t;

// Request structure for async read/write
struct pdc_request {
    int                     seq_id;
    int                     server_id;
    int                     n_client;
    int                     n_update;
    pdc_access_t            access_type;
    void *                  metadata;
    struct pdc_region_info *region;
    void *                  buf;

    char *shm_base;
    char  shm_addr[128];
    int   shm_fd;
    int   shm_size;

    int       n_buf_arr;
    void ***  buf_arr;
    int *     buf_arr_idx;
    char **   shm_base_arr;
    char **   shm_addr_arr;
    int *     shm_fd_arr;
    uint64_t *shm_size_arr;

    void *storage_meta;

    struct pdc_request *prev;
    struct pdc_request *next;
};

/*********************/
/* Public Prototypes */
/*********************/
/**
 * Create a PDC query
 *
 * \param obj_id [IN]            *********
 * \param op [IN]                *********
 * \param type [OUT]             *********
 * \param value [OUT]            *********
 *
 * \return *******
 */
pdc_query_t *PDCquery_create(pdcid_t obj_id, pdc_query_op_t op, pdc_var_type_t type, void *value);

/**
 * *********
 *
 * \param query1 [IN]            *********
 * \param query2 [IN]            *********
 *
 * \return ******
 */
pdc_query_t *PDCquery_and(pdc_query_t *query1, pdc_query_t *query2);

/**
 * *********
 *
 * \param query1 [IN]            *********
 * \param query2 [IN]            *********
 *
 * \return ******
 */
pdc_query_t *PDCquery_or(pdc_query_t *query1, pdc_query_t *query2);

/**
 * Query an object based on a specific metadata (attribute) name and value
 *
 * \param cont_id [IN]          Container ID, 0 for all containers
 * \param prop_name [IN]        Metadta field name
 * \param prop_value [IN]       Metadta field value
 * \param out_ids[OUT]          Result object ids
 * \param n_out[OUT]            Number of results
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_prop_query(pdcid_t cont_id, enum pdc_prop_name_t prop_name, void *prop_value, pdcid_t **out_ids,
                         size_t *n_out);

/**
 * *********
 *
 * \param query [IN]             *********
 * \param obj_region [IN]        Object region information
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCquery_sel_region(pdc_query_t *query, struct pdc_region_info *obj_region);

/**
 * *********
 *
 * \param query [IN]             *********
 * \param sel [IN]               *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCquery_get_selection(pdc_query_t *query, pdc_selection_t *sel);

/**
 * *********
 *
 * \param query [IN]             *********
 * \param n [IN]                 *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCquery_get_nhits(pdc_query_t *query, uint64_t *n);

/**
 * *********
 *
 * \param obj_id [IN]            Object ID
 * \param sel [IN]               *********
 * \param obj_data [IN]          ********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCquery_get_data(pdcid_t obj_id, pdc_selection_t *sel, void *obj_data);

/**
 * *********
 *
 * \param obj_id [IN]            Object ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCquery_get_histogram(pdcid_t obj_id);

/**
 * *********
 *
 * \param query [IN]             *********
 * \param sel [IN]               *********
 * \param data [IN]              *********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCquery_get_sel_data(pdc_query_t *query, pdc_selection_t *sel, void *data);

/**
 * ********
 *
 * \param sel [IN]              *********
 */
void PDCselection_free(pdc_selection_t *sel);

/**
 * ********
 *
 * \param query [IN]            *********
 */
void PDCquery_free(pdc_query_t *query);

/**
 * ********
 *
 * \param query [IN]            *********
 */
void PDCquery_free_all(pdc_query_t *query);

/**
 * ********
 *
 * \param query [IN]            *********
 */
void PDCquery_print(pdc_query_t *query);

/**
 * ********
 *
 * \param sel [IN]              *********
 */
void PDCselection_print(pdc_selection_t *sel);

/**
 * ********
 *
 * \param sel [IN]              *********
 */
void PDCselection_print_all(pdc_selection_t *sel);

#endif /* PDC_QUERY_H */
