#ifndef _pdc_H
#define _pdc_H

#include <stdint.h>
#include <stdbool.h>
#include "pdc_public.h"
#include "pdc_error.h"
#include "pdc_pkg.h"
#include "pdc_prop.h"
#include "pdc_prop_pkg.h"
#include "pdc_cont.h"
#include "pdc_cont_pkg.h"
#include "pdc_obj.h"
#include "pdc_obj_pkg.h"
#include "pdc_dt_conv.h"

typedef struct {
    pdcid_t pdc_id;
} PDC_STRUCT;

typedef enum {
    PDC_QUERY_OP_AND = 0,
    PDC_QUERY_OP_OR  = 1
} PDC_com_op_mode_t;


/* Functions in PDC.c */

///////////////////
// PDC functions //
///////////////////

/* Initialize the PDC layer
 * \param PDC_property [IN] A PDC_property struct
 * \return PDC id 
 */
pdcid_t PDC_init(PDC_prop_t property);

/* Close the PDC layer
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_close();

/* Create a type of PDC
 * \param PDC_STRUCT [IN] A PDC_STRUCT struct
 * \return PDC type id
 */
pdcid_t PDCtype_create(PDC_STRUCT pdc_struct);

/* Insert fields in PDC_STRUCT 
 * \param type_id [IN] Type of PDC, returned by PDCtype_create(struct PDC_STRUCT)
 * \param name [IN] Variable name to insert to PDC_STRUCT
 * \param offset [IN] Offset of the variable in PDC_STRUCT
 * \param var_type [IN] Variable type (enum type), e.g. PDC_var_type_t, 
 * i.e. PDC_int_t, PDC_float_t
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtype_struct_field_insert(pdcid_t type_id, const char *name, uint64_t offset, PDC_var_type_t var_type);

/* Get number of loci for a PDC
 * \param pdc_id [IN] Id of the PDC
 * \param nloci [OUT] Number of loci of the PDC residing at
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCget_loci_count(pdcid_t pdc_id, pdcid_t *nloci);

/* Get PDC info in the locus
 * \param pdc_id [IN] Id of the PDC
 * \param n [IN] Location of memory hierarchy of the PDC
 * \param info [OUT] A PDC_loci_info_t struct
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCget_loci_info(pdcid_t pdc_id, pdcid_t n, PDC_loci_info_t *info);

//////////////////////
// object functions//
//////////////////////

/* Use result from  PDCquery_obj function
 * \param query1_id [IN] Query id, returned by PDC_query_obj function
 * \param PDC_com_op_mode_t [IN] Query Combination type (enum type), PDC_QUERY_OP_AND or PDC_QUERY_OP_OR
 * \param query2_id [IN] Query id, returned by PDC_query_obj function
 * \return Query id
 */
pdcid_t PDC_query_combine(pdcid_t query1_id, PDC_com_op_mode_t combine_op, pdcid_t query2_id);


#endif /* end of _pdc_H */ 
