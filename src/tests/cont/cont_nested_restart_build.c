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

/*
 * First half of the checkpoint/restart pair for nested containers: builds a
 * multi-level hierarchy against a fresh server and exits. The server is then
 * closed (which checkpoints it) and restarted by the test driver script
 * before cont_nested_restart_verify runs against the SAME names to confirm
 * the parent/child relationships survived the round trip.
 *
 * Names are hardcoded and shared with cont_nested_restart_verify.c on purpose:
 * the two run in separate processes with no other way to hand off state.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"
#include "test_helper.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, obj_prop;
    pdcid_t root, branch_a, branch_b, branch_a_sub;
    pdcid_t obj_root1, obj_a1, obj_sub1, obj_b1;
    int     ret_value = TSUCCEED;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
#endif

    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");

    TASSERT((root = PDCcont_create("restart_root", cont_prop)) != 0, "Created restart_root",
            "Failed to create restart_root");
    TASSERT((branch_a = PDCcont_create_at(root, "restart_branch_a", cont_prop)) != 0,
            "Created+nested restart_branch_a", "Failed to create+nest restart_branch_a");
    TASSERT((branch_b = PDCcont_create_at(root, "restart_branch_b", cont_prop)) != 0,
            "Created+nested restart_branch_b", "Failed to create+nest restart_branch_b");
    TASSERT((branch_a_sub = PDCcont_create_at(branch_a, "restart_branch_a_sub", cont_prop)) != 0,
            "Created+nested restart_branch_a_sub", "Failed to create+nest restart_branch_a_sub");

    TASSERT((obj_root1 = PDCobj_create(root, "restart_obj_root1", obj_prop)) != 0,
            "Created restart_obj_root1", "Failed to create restart_obj_root1");
    TASSERT((obj_a1 = PDCobj_create(branch_a, "restart_obj_a1", obj_prop)) != 0, "Created restart_obj_a1",
            "Failed to create restart_obj_a1");
    TASSERT((obj_sub1 = PDCobj_create(branch_a_sub, "restart_obj_sub1", obj_prop)) != 0,
            "Created restart_obj_sub1", "Failed to create restart_obj_sub1");
    TASSERT((obj_b1 = PDCobj_create(branch_b, "restart_obj_b1", obj_prop)) != 0, "Created restart_obj_b1",
            "Failed to create restart_obj_b1");

    TASSERT(PDCcont_put_objids(root, 1, &obj_root1) == SUCCEED, "Linked restart_obj_root1 under root",
            "Failed to link restart_obj_root1 under root");
    TASSERT(PDCcont_put_objids(branch_a, 1, &obj_a1) == SUCCEED, "Linked restart_obj_a1 under branch_a",
            "Failed to link restart_obj_a1 under branch_a");
    TASSERT(PDCcont_put_objids(branch_a_sub, 1, &obj_sub1) == SUCCEED,
            "Linked restart_obj_sub1 under branch_a_sub",
            "Failed to link restart_obj_sub1 under branch_a_sub");
    TASSERT(PDCcont_put_objids(branch_b, 1, &obj_b1) == SUCCEED, "Linked restart_obj_b1 under branch_b",
            "Failed to link restart_obj_b1 under branch_b");

    // Intentionally NOT closing containers/pdc here beyond what's required:
    // the test driver closes the server (which checkpoints it) right after
    // this process exits.
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
