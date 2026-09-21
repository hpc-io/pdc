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
 * reproduce, distribute copies to the public, prepare derivative works, and to
 * perform publicly and display publicly, and to permit other to do so.
 */

#ifndef PDC_SERVER_RESCALE_H
#define PDC_SERVER_RESCALE_H

#include "pdc_public.h"

/**
 * Restart with a different server count than the checkpoint (elastic).
 *
 * Eagerly repartitions metadata from N_old checkpoint shards onto N_new ranks.
 * Unpublishes stale server.cfg for the duration of migration, migrates and
 * remaps transfer-query planner state, ensures per-rank tmp dirs exist, and
 * barriers before returning so the caller can publish server.cfg only after
 * metadata is ready.
 *
 * When built with PDC_ENABLE_IDIOMS, elastic restart is refused (index shards
 * are not repartitioned with metadata); same-N restart still recovers IDIOMS.
 *
 * \param n_old [IN] Server count at checkpoint time
 * \param n_new [IN] Current server MPI size
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_restart_elastic(int n_old, int n_new);

/**
 * Look up post-elastic home rank for an object/container ID.
 *
 * Only populated after a successful elastic restart on this process.
 * Returns 1 and sets *home_out on hit; returns 0 if the map is inactive
 * or the ID is not present (e.g. newly created after migrate — use legacy).
 *
 * \param obj_id [IN]   Object or container ID
 * \param home_out [OUT] Home rank under N_new when found
 *
 * \return 1 if found, 0 otherwise
 */
int PDC_Server_lookup_obj_id_home(uint64_t obj_id, uint32_t *home_out);

/**
 * Write obj_id_home_map.bin next to server.cfg from the in-memory elastic map.
 * Rank 0 only performs I/O; call after the map is built and before server.cfg publish.
 */
perr_t PDC_Server_publish_obj_id_home_map(int n_new);

/**
 * Remove obj_id_home_map.bin if present (same-N / fresh start / pre-elastic cleanup).
 */
perr_t PDC_Server_unpublish_obj_id_home_map(void);

#endif /* PDC_SERVER_RESCALE_H */
