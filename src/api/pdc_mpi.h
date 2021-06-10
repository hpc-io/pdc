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

#ifndef PDC_MPI_H
#define PDC_MPI_H

#include "mpi.h"

/*********************/
/* Public Prototypes */
/*********************/
/**
 * Create an object
 *
 * \param cont_id [IN]          ID of the container
 * \param obj_name [IN]         Name of the object
 * \param obj_create_prop [IN]  ID of object property,
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param rank_id [IN]          MPI process rank
 *
 * \return Object ID on success/Negative on failure
 */
pdcid_t PDCobj_create_mpi(pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop, int rank_id,
                          MPI_Comm comm);

#endif /* PDC_MPI_H */
