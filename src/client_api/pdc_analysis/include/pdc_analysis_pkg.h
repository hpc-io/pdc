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
#ifndef PDC_ANALYSIS_COMMON_H
#define PDC_ANALYSIS_COMMON_H

#include "pdc_transforms_pkg.h"
#include "mercury.h"
#include "mercury_proc_string.h"
#include "mercury_atomic.h"

/*****************************/
/* Library-private Variables */
/*****************************/
hg_id_t           analysis_ftn_register_id_g;
hg_id_t           transform_ftn_register_id_g;
hg_id_t           object_data_iterator_register_id_g;
hg_atomic_int32_t registered_analysis_ftn_count_g;

extern struct _pdc_region_analysis_ftn_info **pdc_region_analysis_registry;

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * *****
 *
 * \param registry [IN]         *******
 *
 * \return ******
 */
int PDC_get_transforms(struct _pdc_region_transform_ftn_info ***registry);

/**
 * *****
 *
 * \param op_type [IN]          The type of transformation
 * \param dest_region [IN]      The struct pointing to destination region
 *
 * \return ********
 */
int PDC_check_transform(pdc_obj_transform_t op_type, struct pdc_region_info *dest_region);

/**
 * ********
 *
 * \param op_type [IN]          The type of transformation
 * \param dest_region [IN]      The struct pointing to destination region
 *
 * \return ********
 */
int PDC_check_analysis(pdc_obj_transform_t op_type, struct pdc_region_info *dest_region);

/**
 * ********
 *
 * \param op_type [IN]          ***********
 *
 * \return ********
 */
int PDC_add_transform_ptr_to_registry_(struct _pdc_region_transform_ftn_info *ftnPtr);

/**
 * ********
 *
 * \param client_index [IN]     The index of the client
 * \param meta_index [IN]       *************
 *
 * \return
 */
int PDC_update_transform_server_meta_index(int client_index, int meta_index);

/**
 * *******
 *
 * \param registry [IN]         ************
 *
 * \return
 */
int PDC_get_analysis_registry(struct _pdc_region_analysis_ftn_info ***registry);

/**
 * *******
 *
 * \param locus_identifier [IN] **********
 */
void PDC_set_execution_locus(_pdc_loci_t locus_identifier);

/**
 * *****
 *
 * \param ftn_infoPtr [IN]      *********
 *
 * \return *****
 */
int PDC_add_analysis_ptr_to_registry_(struct _pdc_region_analysis_ftn_info *ftn_infoPtr);

/**
 * *****
 *
 * \param ftn [IN]              *******
 * \param loadpath [IN]         *******
 * \param ftnPtr [IN]           *******
 *
 * \return *****
 */
int PDC_get_ftnPtr_(const char *ftn, const char *loadpath, void **ftnPtr);

/**
 * *****
 *
 * \return
 */
_pdc_loci_t PDC_get_execution_locus();

/**
 * ********
 *
 * \param workingDir [IN]       Path of working directory
 * \param application [IN]      Name of the application
 *
 * \return ****
 */
char *PDC_find_in_path(char *workingDir, char *application);

/**
 * ********
 *
 * \return ****
 */
char *PDC_get_argv0_();

/**
 * ********
 *
 * \param fname [IN]            Path of working directory
 * \param app_path [IN]         Name of the application
 *
 * \return ****
 */
char *PDC_get_realpath(char *fname, char *app_path);

/**
 * ********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_iterator_end();

/**
 * ********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_analysis_end();

/**
 * ********
 */
void PDC_free_analysis_registry();

/**
 * ********
 */
void PDC_free_transform_registry();

/**
 * ********
 */
void PDC_free_iterator_cache();

#endif /* PDC_ANALYSIS_COMMON_H */
