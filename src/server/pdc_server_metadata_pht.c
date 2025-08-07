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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctype.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>

#include "pdc_config.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc_utlist.h"
#include "pdc_hash_table.h"
#include "pdc_dablooms.h"
#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server_metadata_pht.h"
#include "pdc_server.h"
#include "mercury_hash_table.h"
#include "pdc_malloc.h"
#include "string_utils.h"
#include "pdc_logger.h"
#include "pdc_pht.h"


PrefixTable *metadata_pht_g = NULL;

/*
 * Check if two hash keys are equal
 *
 * \param vlocation1 [IN]       Hash table key
 * \param vlocation2 [IN]       Hash table key
 *
 * \return 1 if two keys are equal, 0 otherwise
 */
static int
PDC_Server_metadata_pht_int_compare(void *vlocation1, void *vlocation2)
{
    int32_t k1 = *(int32_t *) key1;
    int32_t k2 = *(int32_t *) key2;

    return (k1 > k2) - (k1 < k2);
}

/*
 * Get hash key's location in hash table
 *
 * \param vlocation [IN]        Hash table key
 *
 * \return the location of hash key in the table
 */
static unsigned int
PDC_Server_metadata_pht_int_hash(void *vlocation)
{
    int *location;
    location = (int *) vlocation;
    return (unsigned int) *location;
}

/*
 * Free the hash key
 *
 * \param  key [IN]        Hash table key
 *
 * \return void
 */
static void
PDC_Server_metadata_pht_int_hash_key_free(void *key)
{
    free(key);
}

/*
 * Free metadata hash value
 *
 * \param  value [IN]        Hash table value
 *
 * \return void
 */
static void
PDC_Server_metadata_pht_value_free(void *value)
{
    pdc_metadata_t    *elt, *tmp;
    PrefixTableBucket *head;

    FUNC_ENTER(NULL);

    head = (PrefixTableBucket *)value;

    // Free metadata list
    if (is_restart_g == 0) {
        DL_FOREACH_SAFE(head.store, elt, tmp)
        {
            free(elt);
        }
    }
}

/*
 * Get the metadata with obj ID from the metadata list
 *
 * \param  mlist[IN]         Metadata list head
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t *
find_metadata_pht_by_id_from_list(pdc_metadata_t *mlist, uint64_t obj_id)
{
    pdc_metadata_t *ret_value, *elt;

    FUNC_ENTER(NULL);

    ret_value = NULL;
    if (mlist == NULL) {
        ret_value = NULL;
        goto done;
    }

    DL_FOREACH(mlist, elt)
    {
        if (elt->obj_id == obj_id) {
            ret_value = elt;
            goto done;
        }
    }

done:
    FUNC_LEAVE(ret_value);
}