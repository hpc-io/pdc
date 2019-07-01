/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 PDC VOL connector. The full copyright       *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the COPYING file, which can be found at the root of the   *
 * source code distribution tree.                                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	The public header file for the PDC VOL plugin.
 */

#ifndef H5VLpdc_public_H
#define H5VLpdc_public_H

#include "H5VLpdc_config.h"

#include <mpi.h>

/* Public headers needed by this file */
#include <hdf5.h>

#ifdef __cplusplus
extern "C" {
#endif

/* PDC-specific file access properties */
typedef struct H5VL_pdc_info_t {
    MPI_Comm            comm;           /*communicator                  */
    MPI_Info            info;           /*file information              */
} H5VL_pdc_info_t;

/**
 * Initialize the PDC VOL connector.
 *
 * @returns 0 on success, negative error code on failure
 */
H5VL_PDC_PUBLIC herr_t H5VLpdc_init(void);

/**
 * Finalize the PDC VOL connector.
 *
 * @returns 0 on success, negative error code on failure
 */
H5VL_PDC_PUBLIC herr_t H5VLpdc_term(void);

/**
 * Set the file access property list to use the given MPI communicator/info.
 *
 * @param fapl_id   [IN]    file access property list ID
 * @param comm      [IN]    MPI communicator
 * @param info      [IN]    MPI info
 *
 * @returns 0 on success, negative error code on failure
 */
H5VL_PDC_PUBLIC herr_t H5Pset_fapl_pdc(hid_t fapl_id, MPI_Comm comm, MPI_Info info);

#ifdef __cplusplus
}
#endif

#endif /* H5VLpdc_public_H */
