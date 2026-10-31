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
#endif

    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");

    // Build the hierarchy
    TASSERT((root = PDCcont_create("nested_root", cont_prop)) != 0, "Created root container",
            "Failed to create root container");
    TASSERT((branch_a = PDCcont_create_at(root, "nested_branch_a", cont_prop)) != 0,
            "Created+nested branch_a under root", "Failed to create+nest branch_a");
    TASSERT((branch_b = PDCcont_create_at(root, "nested_branch_b", cont_prop)) != 0,
            "Created+nested branch_b under root", "Failed to create+nest branch_b");
    TASSERT((branch_a_sub = PDCcont_create_at(branch_a, "nested_branch_a_sub", cont_prop)) != 0,
            "Created+nested branch_a_sub under branch_a", "Failed to create+nest branch_a_sub");

    TASSERT((obj_root1 = PDCobj_create(root, "nested_obj_root1", obj_prop)) != 0, "Created obj_root1",
            "Failed to create obj_root1");
    TASSERT((obj_a1 = PDCobj_create(branch_a, "nested_obj_a1", obj_prop)) != 0, "Created obj_a1",
            "Failed to create obj_a1");
    TASSERT((obj_a2 = PDCobj_create(branch_a, "nested_obj_a2", obj_prop)) != 0, "Created obj_a2",
            "Failed to create obj_a2");
    TASSERT((obj_sub1 = PDCobj_create(branch_a_sub, "nested_obj_sub1", obj_prop)) != 0, "Created obj_sub1",
            "Failed to create obj_sub1");
    TASSERT((obj_b1 = PDCobj_create(branch_b, "nested_obj_b1", obj_prop)) != 0, "Created obj_b1",
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
    TASSERT((root_info = PDCcont_get_info("nested_root")) != NULL, "Got root info",
            "Failed to get root info");
    TASSERT((branch_a_info = PDCcont_get_info("nested_branch_a")) != NULL, "Got branch_a info",
            "Failed to get branch_a info");
    TASSERT((branch_b_info = PDCcont_get_info("nested_branch_b")) != NULL, "Got branch_b info",
            "Failed to get branch_b info");
    TASSERT((branch_a_sub_info = PDCcont_get_info("nested_branch_a_sub")) != NULL, "Got branch_a_sub info",
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
