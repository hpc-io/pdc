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
 * Negative-path coverage for the nested-container entry API: operations that
 * must fail (and must be reported as failures, not silently ignored), plus a
 * couple of operations that must NOT fail even though they look suspicious.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"
#include "test_helper.h"

int
main(int argc, char **argv)
{
    pdcid_t           pdc, cont_prop, obj_prop;
    pdcid_t           cont_a, cont_b, obj1;
    int               ret_value = TSUCCEED;
    int               n_entries;
    pdc_cont_entry_t *entries = NULL;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
#endif

    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");

    TASSERT((cont_a = PDCcont_create("fail_cont_a", cont_prop)) != 0, "Created cont_a",
            "Failed to create cont_a");
    TASSERT((cont_b = PDCcont_create("fail_cont_b", cont_prop)) != 0, "Created cont_b",
            "Failed to create cont_b");
    TASSERT((obj1 = PDCobj_create(cont_a, "fail_obj1", obj_prop)) != 0, "Created obj1",
            "Failed to create obj1");

    // --- Failures that MUST be rejected ---

    // A container cannot contain itself: a direct self-reference would make
    // any recursive walk over its entries loop forever.
    {
        pdc_entity_type_t type_cont = PDC_ENTITY_CONT;
        TASSERT(PDCcont_put_entries(cont_a, 1, &cont_a, &type_cont) != SUCCEED,
                "Self-containment was correctly rejected", "BUG: a container was allowed to contain itself");
    }

    // Verify the rejected self-containment entry was NOT partially applied
    TASSERT(PDCcont_get_entries(cont_a, &n_entries, &entries) == SUCCEED,
            "Got cont_a entries after rejected self-containment",
            "Failed to get cont_a entries after rejected self-containment");
    TASSERT(n_entries == 0, "cont_a has no entries after the rejected self-containment attempt",
            "BUG: cont_a has entries despite the self-containment attempt being rejected");
    free(entries);
    entries = NULL;

    // Operating on a container that no longer exists server-side must fail,
    // not silently succeed. Delete cont_b's server-side metadata while
    // keeping its local handle open, then try to use it.
    TASSERT(PDCcont_del(cont_b) == SUCCEED, "Deleted cont_b server-side metadata",
            "Failed to delete cont_b server-side metadata");
    {
        pdc_entity_type_t type_obj = PDC_ENTITY_OBJ;
        TASSERT(PDCcont_put_entries(cont_b, 1, &obj1, &type_obj) != SUCCEED,
                "Adding entries to a deleted container was correctly rejected",
                "BUG: adding entries to a deleted container succeeded");
    }

    // A local id that was never registered at all must fail client-side,
    // cleanly, rather than crash.
    {
        pdcid_t           bogus_id = (pdcid_t)999999999;
        pdc_entity_type_t type_obj = PDC_ENTITY_OBJ;
        TASSERT(PDCcont_put_entries(bogus_id, 1, &obj1, &type_obj) != SUCCEED,
                "Using a never-registered container id was correctly rejected",
                "BUG: using a never-registered container id succeeded");
        TASSERT(PDCcont_get_entries(bogus_id, &n_entries, &entries) != SUCCEED,
                "Getting entries of a never-registered container id was correctly rejected",
                "BUG: getting entries of a never-registered container id succeeded");
    }

    // Deleting an entry that was never added must be rejected, not treated
    // as a silent no-op -- a caller relying on the return value to know
    // whether something was actually removed needs this to fail.
    TASSERT(PDCcont_del_objids(cont_a, 1, &obj1) != SUCCEED,
            "Deleting a never-added entry was correctly rejected",
            "BUG: deleting a never-added entry reported success");
    TASSERT(PDCcont_get_entries(cont_a, &n_entries, &entries) == SUCCEED,
            "Got cont_a entries after rejected delete", "Failed to get cont_a entries after rejected delete");
    TASSERT(n_entries == 0, "cont_a is still empty after the rejected delete",
            "BUG: the rejected delete changed cont_a's entries");
    free(entries);
    entries = NULL;

    // Adding an entry id that doesn't resolve to any real object or
    // container must also be rejected, not linked in as a dead entry that
    // can never be opened or named.
    {
        pdcid_t           bogus_entry_id = (pdcid_t)888888888;
        pdc_entity_type_t type_obj       = PDC_ENTITY_OBJ;
        TASSERT(PDCcont_put_entries(cont_a, 1, &bogus_entry_id, &type_obj) != SUCCEED,
                "Adding a nonexistent entry id was correctly rejected",
                "BUG: adding a nonexistent entry id succeeded");
    }
    TASSERT(PDCcont_get_entries(cont_a, &n_entries, &entries) == SUCCEED,
            "Got cont_a entries after rejected bogus add",
            "Failed to get cont_a entries after rejected bogus add");
    TASSERT(n_entries == 0, "cont_a is still empty after the rejected bogus add",
            "BUG: the rejected bogus add left a dead entry in cont_a");
    free(entries);
    entries = NULL;

    // --- Things that look suspicious but must NOT fail ---

    // An empty container reports zero entries successfully, not an error.
    TASSERT((entries == NULL), "sanity: entries pointer reset before reuse", "unreachable");
    TASSERT(PDCcont_get_entries(cont_a, &n_entries, &entries) == SUCCEED, "Got entries of empty container",
            "Failed to get entries of empty container");
    TASSERT(n_entries == 0, "Empty container reports 0 entries", "Empty container reported entries");
    free(entries);
    entries = NULL;

    TASSERT(PDCcont_close(cont_a) >= 0, "Closed cont_a", "Failed to close cont_a");
    TASSERT(PDCprop_close(cont_prop) >= 0, "Closed cont_prop", "Failed to close cont_prop");
    TASSERT(PDCprop_close(obj_prop) >= 0, "Closed obj_prop", "Failed to close obj_prop");
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
