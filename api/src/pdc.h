/*
 * Copyright Notice for 
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***
 
 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.
 
 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.
 
 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

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
#include "pdc_client_connect.h"

typedef struct {
    pdcid_t pdc_id;
} PDC_STRUCT;

typedef enum {
    PDC_QUERY_OP_AND = 0,
    PDC_QUERY_OP_OR  = 1
} PDC_com_mode_t;


///////////////////
// PDC functions //
///////////////////

/**
 * Initialize the PDC layer
 *
 * \param pdc_name [IN]         Name of the PDC
 *
 * \return PDC id on success/Negative on failure
 */
pdcid_t PDC_init(const char *pdc_name);

/**
 * Close the PDC layer
 *
 * \param pdc_id [IN]          ID of the PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_close(pdcid_t pdcid);

/**
 * Create a type of PDC
 *
 * \param PDC_STRUCT [IN]       A PDC_STRUCT struct
 *
 * \return PDC type id on success/Negative on failure
 */
pdcid_t PDCtype_create(PDC_STRUCT pdc_struct);

/**
 * Insert fields in PDC_STRUCT 
 *
 * \param type_id [IN]           Type of PDC, returned by
 *                               PDCtype_create(struct PDC_STRUCT)
 * \param name [IN]              Variable name to insert to PDC_STRUCT
 * \param offset [IN]            Offset of the variable in PDC_STRUCT
 * \param var_type [IN]          Variable type (enum type), e.g. 
 *                               PDC_int_t, PDC_float_t
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtype_struct_field_insert(pdcid_t type_id, const char *name, uint64_t offset, PDC_var_type_t var_type);

/**
 * Get number of loci for a PDC
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param nloci [OUT]           Number of loci of the PDC residing at
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCget_loci_count(pdcid_t pdc_id, pdcid_t *nloci);

/** 
 * Get PDC info in the locus
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param n [IN]                Location of memory hierarchy of the PDC
 * \param info [OUT]            A PDC_loci_info_t struct

 * \return Non-negative on success/Negative on failure
 */
perr_t PDCget_loci_info(pdcid_t pdc_id, pdcid_t n, PDC_loci_info_t *info);

/**
 *  Use result from  PDCquery_obj function
 *
 * \param query1_id [IN]        Query id, returned by PDC_query_obj function
 * \param PDC_com_mode_t [IN]   Query Combination type (enum type), PDC_QUERY_OP_AND or PDC_QUERY_OP_OR
 * \param query2_id [IN]        Query id, returned by PDC_query_obj function
 *
 * \return Query id
 */
pdcid_t PDC_query_combine(pdcid_t query1_id, PDC_com_mode_t combine_op, pdcid_t query2_id);


#endif /* end of _pdc_H */ 
