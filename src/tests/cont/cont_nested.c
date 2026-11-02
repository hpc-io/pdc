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
 * Builds a multi-level container hierarchy (containers nested inside
 * containers, mixed with plain objects) in a single process and checks that
 * PDCcont_get_entries/get_objids report exactly the expected structure:
 *
 *   root
 *     +-- obj_root1                        (object)
 *     +-- branch_a                         (container)
 *     |     +-- obj_a1, obj_a2             (objects)
 *     |     +-- branch_a_sub               (container)
 *     |           +-- obj_sub1             (object)
 *     +-- branch_b                         (container)
 *           +-- obj_b1                     (object)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "pdc.h"
#include "test_helper.h"

// Checks that `entries` (n_entries of them) is exactly the set
// {expected_ids[i], expected_types[i]}, ignoring order.
static int
check_entries(pdc_cont_entry_t *entries, int n_entries, uint64_t *expected_ids,
              pdc_entity_type_t *expected_types, int n_expected, const char *label)
{
    int  i, j;
    int *matched;
    int  ok = 1;

    if (n_entries != n_expected) {
        LOG_ERROR("%s: expected %d entries, got %d\n", label, n_expected, n_entries);
        return 0;
    }

    matched = (int *)calloc(n_expected, sizeof(int));
    for (i = 0; i < n_entries; i++) {
        int found = 0;
        for (j = 0; j < n_expected; j++) {
            if (matched[j])
                continue;
            if (entries[i].meta_id == expected_ids[j] && entries[i].type == expected_types[j]) {
                matched[j] = 1;
                found      = 1;
                break;
            }
        }
        if (!found) {
            LOG_ERROR("%s: unexpected entry (meta_id=%" PRIu64 ", type=%d)\n", label, entries[i].meta_id,
                      entries[i].type);
            ok = 0;
        }
    }
    free(matched);
    return ok;
}

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, obj_prop;
    pdcid_t root, branch_a, branch_b, branch_a_sub;
    pdcid_t obj_root1, obj_a1, obj_a2, obj_sub1, obj_b1;
    int     ret_value = TSUCCEED;
    int     rank      = 0;

    // Both container and object names must be unique server-wide (a
    // duplicate container name resolves to the SAME underlying container
    // rather than a fresh independent one, same as a duplicate object name
    // does), so under MPI each rank runs this independently with its own
    // rank-suffixed names to avoid colliding with every other rank's
    // otherwise-identical create calls.
    char root_name[64], branch_a_name[64], branch_b_name[64], branch_a_sub_name[64];
    char obj_root1_name[64], obj_a1_name[64], obj_a2_name[64], obj_sub1_name[64], obj_b1_name[64];

    uint64_t          root_expected_ids[3];
    pdc_entity_type_t root_expected_types[3];
    uint64_t          branch_a_expected_ids[3];
    pdc_entity_type_t branch_a_expected_types[3];

    pdc_cont_entry_t *entries   = NULL;
    int               n_entries = 0;
    pdcid_t *         obj_ids   = NULL;
    int               n_obj     = 0;

    struct pdc_cont_info *root_info, *branch_a_info, *branch_b_info, *branch_a_sub_info;
    struct pdc_obj_info * obj_root1_info, *obj_a1_info, *obj_a2_info, *obj_sub1_info, *obj_b1_info;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif

    snprintf(root_name, sizeof(root_name), "nested_root_r%d", rank);
    snprintf(branch_a_name, sizeof(branch_a_name), "nested_branch_a_r%d", rank);
    snprintf(branch_b_name, sizeof(branch_b_name), "nested_branch_b_r%d", rank);
    snprintf(branch_a_sub_name, sizeof(branch_a_sub_name), "nested_branch_a_sub_r%d", rank);
    snprintf(obj_root1_name, sizeof(obj_root1_name), "nested_obj_root1_r%d", rank);
    snprintf(obj_a1_name, sizeof(obj_a1_name), "nested_obj_a1_r%d", rank);
    snprintf(obj_a2_name, sizeof(obj_a2_name), "nested_obj_a2_r%d", rank);
    snprintf(obj_sub1_name, sizeof(obj_sub1_name), "nested_obj_sub1_r%d", rank);
    snprintf(obj_b1_name, sizeof(obj_b1_name), "nested_obj_b1_r%d", rank);

    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");

    // Build the hierarchy
    TASSERT((root = PDCcont_create(root_name, cont_prop)) != 0, "Created root container",
            "Failed to create root container");
    TASSERT((branch_a = PDCcont_create_at(root, branch_a_name, cont_prop)) != 0,
            "Created+nested branch_a under root", "Failed to create+nest branch_a");
    TASSERT((branch_b = PDCcont_create_at(root, branch_b_name, cont_prop)) != 0,
            "Created+nested branch_b under root", "Failed to create+nest branch_b");
    TASSERT((branch_a_sub = PDCcont_create_at(branch_a, branch_a_sub_name, cont_prop)) != 0,
            "Created+nested branch_a_sub under branch_a", "Failed to create+nest branch_a_sub");

    TASSERT((obj_root1 = PDCobj_create(root, obj_root1_name, obj_prop)) != 0, "Created obj_root1",
            "Failed to create obj_root1");
    TASSERT((obj_a1 = PDCobj_create(branch_a, obj_a1_name, obj_prop)) != 0, "Created obj_a1",
            "Failed to create obj_a1");
    TASSERT((obj_a2 = PDCobj_create(branch_a, obj_a2_name, obj_prop)) != 0, "Created obj_a2",
            "Failed to create obj_a2");
    TASSERT((obj_sub1 = PDCobj_create(branch_a_sub, obj_sub1_name, obj_prop)) != 0, "Created obj_sub1",
            "Failed to create obj_sub1");
    TASSERT((obj_b1 = PDCobj_create(branch_b, obj_b1_name, obj_prop)) != 0, "Created obj_b1",
            "Failed to create obj_b1");

    // Link the plain objects into their parent containers
    // (branch_a/branch_b/branch_a_sub were already linked by PDCcont_create_at)
    TASSERT(PDCcont_put_objids(root, 1, &obj_root1) == SUCCEED, "Linked obj_root1 under root",
            "Failed to link obj_root1 under root");
    {
        pdcid_t a_objs[2] = {obj_a1, obj_a2};
        TASSERT(PDCcont_put_objids(branch_a, 2, a_objs) == SUCCEED, "Linked obj_a1/obj_a2 under branch_a",
                "Failed to link obj_a1/obj_a2 under branch_a");
    }
    TASSERT(PDCcont_put_objids(branch_a_sub, 1, &obj_sub1) == SUCCEED, "Linked obj_sub1 under branch_a_sub",
            "Failed to link obj_sub1 under branch_a_sub");
    TASSERT(PDCcont_put_objids(branch_b, 1, &obj_b1) == SUCCEED, "Linked obj_b1 under branch_b",
            "Failed to link obj_b1 under branch_b");

    // Resolve the global meta_ids we expect to see reflected back
    TASSERT((root_info = PDCcont_get_info(root_name)) != NULL, "Got root info", "Failed to get root info");
    TASSERT((branch_a_info = PDCcont_get_info(branch_a_name)) != NULL, "Got branch_a info",
            "Failed to get branch_a info");
    TASSERT((branch_b_info = PDCcont_get_info(branch_b_name)) != NULL, "Got branch_b info",
            "Failed to get branch_b info");
    TASSERT((branch_a_sub_info = PDCcont_get_info(branch_a_sub_name)) != NULL, "Got branch_a_sub info",
            "Failed to get branch_a_sub info");
    TASSERT((obj_root1_info = PDCobj_get_info(obj_root1)) != NULL, "Got obj_root1 info",
            "Failed to get obj_root1 info");
    TASSERT((obj_a1_info = PDCobj_get_info(obj_a1)) != NULL, "Got obj_a1 info", "Failed to get obj_a1 info");
    TASSERT((obj_a2_info = PDCobj_get_info(obj_a2)) != NULL, "Got obj_a2 info", "Failed to get obj_a2 info");
    TASSERT((obj_sub1_info = PDCobj_get_info(obj_sub1)) != NULL, "Got obj_sub1 info",
            "Failed to get obj_sub1 info");
    TASSERT((obj_b1_info = PDCobj_get_info(obj_b1)) != NULL, "Got obj_b1 info", "Failed to get obj_b1 info");

    // root should contain {obj_root1(OBJ), branch_a(CONT), branch_b(CONT)}
    root_expected_ids[0]   = obj_root1_info->meta_id;
    root_expected_types[0] = PDC_ENTITY_OBJ;
    root_expected_ids[1]   = branch_a_info->meta_id;
    root_expected_types[1] = PDC_ENTITY_CONT;
    root_expected_ids[2]   = branch_b_info->meta_id;
    root_expected_types[2] = PDC_ENTITY_CONT;

    TASSERT(PDCcont_get_entries(root, &n_entries, &entries) == SUCCEED, "Got root entries",
            "Failed to get root entries");
    TASSERT(check_entries(entries, n_entries, root_expected_ids, root_expected_types, 3, "root"),
            "root entries match expected structure", "root entries do NOT match expected structure");
    free(entries);
    entries = NULL;

    // branch_a should contain {obj_a1(OBJ), obj_a2(OBJ), branch_a_sub(CONT)}
    branch_a_expected_ids[0]   = obj_a1_info->meta_id;
    branch_a_expected_types[0] = PDC_ENTITY_OBJ;
    branch_a_expected_ids[1]   = obj_a2_info->meta_id;
    branch_a_expected_types[1] = PDC_ENTITY_OBJ;
    branch_a_expected_ids[2]   = branch_a_sub_info->meta_id;
    branch_a_expected_types[2] = PDC_ENTITY_CONT;

    TASSERT(PDCcont_get_entries(branch_a, &n_entries, &entries) == SUCCEED, "Got branch_a entries",
            "Failed to get branch_a entries");
    TASSERT(check_entries(entries, n_entries, branch_a_expected_ids, branch_a_expected_types, 3, "branch_a"),
            "branch_a entries match expected structure", "branch_a entries do NOT match expected structure");
    free(entries);
    entries = NULL;

    // The legacy PDCcont_get_objids() wrapper should filter out branch_a_sub
    // and only return the two plain objects under branch_a.
    TASSERT(PDCcont_get_objids(branch_a, &n_obj, &obj_ids) == SUCCEED, "Got branch_a objids",
            "Failed to get branch_a objids");
    TASSERT(n_obj == 2, "branch_a has exactly 2 plain-object entries", "branch_a objid count mismatch");
    free(obj_ids);
    obj_ids = NULL;

    // branch_a_sub should contain exactly {obj_sub1(OBJ)}
    TASSERT(PDCcont_get_entries(branch_a_sub, &n_entries, &entries) == SUCCEED, "Got branch_a_sub entries",
            "Failed to get branch_a_sub entries");
    TASSERT(n_entries == 1 && entries[0].meta_id == obj_sub1_info->meta_id &&
                entries[0].type == PDC_ENTITY_OBJ,
            "branch_a_sub contains exactly obj_sub1", "branch_a_sub does not match expected structure");
    free(entries);
    entries = NULL;

    // branch_b should contain exactly {obj_b1(OBJ)}
    TASSERT(PDCcont_get_entries(branch_b, &n_entries, &entries) == SUCCEED, "Got branch_b entries",
            "Failed to get branch_b entries");
    TASSERT(n_entries == 1 && entries[0].meta_id == obj_b1_info->meta_id && entries[0].type == PDC_ENTITY_OBJ,
            "branch_b contains exactly obj_b1", "branch_b does not match expected structure");
    free(entries);
    entries = NULL;

    // Deleting an entry removes it from subsequent listings
    TASSERT(PDCcont_del_objids(branch_b, 1, &obj_b1) == SUCCEED, "Deleted obj_b1 from branch_b",
            "Failed to delete obj_b1 from branch_b");
    TASSERT(PDCcont_get_entries(branch_b, &n_entries, &entries) == SUCCEED,
            "Got branch_b entries after delete", "Failed to get branch_b entries after delete");
    TASSERT(n_entries == 0, "branch_b is empty after deleting its only entry",
            "branch_b still reports entries after delete");
    free(entries);
    entries = NULL;

    // A nested child's name can be resolved purely from the id reported by
    // PDCcont_get_entries(), without ever having opened it by name.
    {
        char *resolved_name = NULL;

        TASSERT(PDCcont_get((pdcid_t)branch_a_info->meta_id, &resolved_name) == SUCCEED,
                "Resolved branch_a's name by id", "Failed to resolve branch_a's name by id");
        TASSERT(resolved_name != NULL && strcmp(resolved_name, branch_a_name) == 0,
                "branch_a's name-by-id matches", "branch_a's name-by-id does NOT match");
        free(resolved_name);
        resolved_name = NULL;

        TASSERT(PDCcont_get((pdcid_t)branch_a_sub_info->meta_id, &resolved_name) == SUCCEED,
                "Resolved branch_a_sub's name by id", "Failed to resolve branch_a_sub's name by id");
        TASSERT(resolved_name != NULL && strcmp(resolved_name, branch_a_sub_name) == 0,
                "branch_a_sub's name-by-id matches", "branch_a_sub's name-by-id does NOT match");
        free(resolved_name);
    }

    // Collective (_col) variants should behave identically to their
    // independent counterparts (exercised here at comm size 1, but the same
    // rank-0-queries-then-broadcasts code path runs regardless of size).
    {
        pdcid_t               branch_c;
        pdcid_t               obj_c1;
        pdc_entity_type_t     obj_type      = PDC_ENTITY_OBJ;
        char *                resolved_name = NULL;
        pdc_cont_entry_t *    col_entries   = NULL;
        int                   n_col_entries = 0;
        struct pdc_cont_info *branch_c_info;

        TASSERT((branch_c = PDCcont_create_at_coll(root, "nested_branch_c", cont_prop, MPI_COMM_WORLD)) != 0,
                "Created+nested branch_c under root via _coll",
                "Failed to create+nest branch_c via _coll");
        TASSERT((branch_c_info = PDCcont_get_info("nested_branch_c")) != NULL, "Got branch_c info",
                "Failed to get branch_c info");

        // Calling the collective create again with the same name must be
        // idempotent: it should return the SAME child rather than minting a
        // duplicate (previously this left two same-named children both
        // linked under root).
        {
            pdcid_t branch_c_again =
                PDCcont_create_at_coll(root, "nested_branch_c", cont_prop, MPI_COMM_WORLD);
            TASSERT(branch_c_again != 0, "Re-created+nested branch_c under root via _coll",
                    "Failed to re-create+nest branch_c via _coll");
            struct pdc_cont_info *branch_c_again_info = PDCcont_get_info("nested_branch_c");
            TASSERT(branch_c_again_info != NULL && branch_c_again_info->meta_id == branch_c_info->meta_id,
                    "Re-creating branch_c by name is idempotent (same meta_id)",
                    "Re-creating branch_c by name minted a duplicate");
            PDCcont_close(branch_c_again);
        }

        // No PDCobj_create_col exists yet, and PDCcont_put_entries_coll below
        // only sends rank 0's entry_ids anyway, so only rank 0 needs to
        // actually create obj_c1 -- every other rank creating it too would
        // just collide on the same object name server-side.
        if (rank == 0) {
            TASSERT((obj_c1 = PDCobj_create(branch_c, "nested_obj_c1", obj_prop)) != 0, "Created obj_c1",
                    "Failed to create obj_c1");
        }
        TASSERT(PDCcont_put_entries_coll(branch_c, 1, &obj_c1, &obj_type, MPI_COMM_WORLD) == SUCCEED,
                "Linked obj_c1 under branch_c via _coll", "Failed to link obj_c1 under branch_c via _coll");

        TASSERT(PDCcont_get_entries_coll(branch_c, &n_col_entries, &col_entries, MPI_COMM_WORLD) == SUCCEED,
                "Got branch_c entries via _coll", "Failed to get branch_c entries via _coll");
        TASSERT(n_col_entries == 1 && col_entries[0].type == PDC_ENTITY_OBJ,
                "branch_c contains exactly obj_c1 (via _coll)",
                "branch_c does not match expected structure (via _coll)");

        // Object entries must also be resolvable by id, the same way
        // container entries already are -- PDCcont_get() falls back to the
        // object metadata table when the id isn't a container.
        uint64_t discovered_obj_c1_meta_id = col_entries[0].meta_id;
        {
            char *obj_c1_name = NULL;
            TASSERT(PDCcont_get_coll((pdcid_t)discovered_obj_c1_meta_id, &obj_c1_name, MPI_COMM_WORLD) ==
                        SUCCEED,
                    "Resolved obj_c1's name by id via _coll", "Failed to resolve obj_c1's name by id");
            TASSERT(obj_c1_name != NULL && strcmp(obj_c1_name, "nested_obj_c1") == 0,
                    "obj_c1's name-by-id matches", "obj_c1's name-by-id does NOT match");
            free(obj_c1_name);
        }
        free(col_entries);
        col_entries = NULL;

        TASSERT(PDCcont_get_coll((pdcid_t)branch_c_info->meta_id, &resolved_name, MPI_COMM_WORLD) ==
                    SUCCEED,
                "Resolved branch_c's name by id via _coll",
                "Failed to resolve branch_c's name by id via _coll");
        TASSERT(resolved_name != NULL && strcmp(resolved_name, "nested_branch_c") == 0,
                "branch_c's name-by-id matches (via _coll)",
                "branch_c's name-by-id does NOT match (via _coll)");
        free(resolved_name);

        // Delete by the raw meta id discovered via get_entries_coll above
        // (NOT the local handle obj_c1 from PDCobj_create) -- this is
        // exactly the pattern a VOL connector would need for an entry it
        // only knows about from listing a container's contents, never
        // having locally opened it.
        {
            pdcid_t discovered_obj_c1 = (pdcid_t)discovered_obj_c1_meta_id;
            TASSERT(PDCcont_del_entries_coll(branch_c, 1, &discovered_obj_c1, &obj_type, MPI_COMM_WORLD) ==
                        SUCCEED,
                    "Unlinked obj_c1 (by discovered meta id) from branch_c via _coll",
                    "Failed to unlink obj_c1 (by discovered meta id) from branch_c via _coll");
        }
        TASSERT(PDCcont_get_entries_coll(branch_c, &n_col_entries, &col_entries, MPI_COMM_WORLD) == SUCCEED,
                "Got branch_c entries after _coll delete",
                "Failed to get branch_c entries after _coll delete");
        TASSERT(n_col_entries == 0, "branch_c is empty after _coll delete",
                "branch_c still reports entries after _coll delete");
        free(col_entries);
        col_entries = NULL;

        TASSERT(PDCcont_close(branch_c) >= 0, "Closed branch_c", "Failed to close branch_c");
    }

    TASSERT(PDCcont_close(root) >= 0, "Closed root", "Failed to close root");
    TASSERT(PDCcont_close(branch_a) >= 0, "Closed branch_a", "Failed to close branch_a");
    TASSERT(PDCcont_close(branch_b) >= 0, "Closed branch_b", "Failed to close branch_b");
    TASSERT(PDCcont_close(branch_a_sub) >= 0, "Closed branch_a_sub", "Failed to close branch_a_sub");
    TASSERT(PDCprop_close(cont_prop) >= 0, "Closed cont_prop", "Failed to close cont_prop");
    TASSERT(PDCprop_close(obj_prop) >= 0, "Closed obj_prop", "Failed to close obj_prop");
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
