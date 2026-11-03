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
 * Second half of the checkpoint/restart pair: runs against a server that was
 * restarted from the checkpoint written after cont_nested_restart_build.c
 * exited, and confirms the parent/child container relationships it built
 * (both object and nested-container entries) are exactly as they were
 * before the restart. See cont_nested_restart_build.c for the hierarchy and
 * for why the names below are hardcoded rather than passed between the two.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "pdc.h"
#include "test_helper.h"

static int
has_entry(pdc_cont_entry_t *entries, int n, uint64_t meta_id, pdc_entity_type_t type)
{
    int i;
    for (i = 0; i < n; i++)
        if (entries[i].meta_id == meta_id && entries[i].type == type)
            return 1;
    return 0;
}

int
main(int argc, char **argv)
{
    pdcid_t           pdc;
    pdcid_t           root, branch_a, branch_b, branch_a_sub;
    pdcid_t           obj_root1, obj_a1, obj_sub1, obj_b1;
    pdcid_t           dup_parent_a, dup_parent_b;
    int               ret_value = TSUCCEED;
    int               n_entries;
    pdc_cont_entry_t *entries = NULL;

    struct pdc_cont_info *root_info, *branch_a_info, *branch_b_info, *branch_a_sub_info;
    struct pdc_obj_info * obj_root1_info, *obj_a1_info, *obj_sub1_info, *obj_b1_info;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
#endif

    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");

    // Reopen every node by the name cont_nested_restart_build.c gave it.
    TASSERT((root = PDCcont_open("restart_root", pdc)) > 0, "Reopened restart_root after restart",
            "Failed to reopen restart_root after restart");
    TASSERT((branch_a = PDCcont_open("restart_branch_a", pdc)) > 0, "Reopened restart_branch_a after restart",
            "Failed to reopen restart_branch_a after restart");
    TASSERT((branch_b = PDCcont_open("restart_branch_b", pdc)) > 0, "Reopened restart_branch_b after restart",
            "Failed to reopen restart_branch_b after restart");
    TASSERT((branch_a_sub = PDCcont_open("restart_branch_a_sub", pdc)) > 0,
            "Reopened restart_branch_a_sub after restart",
            "Failed to reopen restart_branch_a_sub after restart");
    TASSERT((obj_root1 = PDCobj_open("restart_obj_root1", pdc)) > 0,
            "Reopened restart_obj_root1 after restart", "Failed to reopen restart_obj_root1 after restart");
    TASSERT((obj_a1 = PDCobj_open("restart_obj_a1", pdc)) > 0, "Reopened restart_obj_a1 after restart",
            "Failed to reopen restart_obj_a1 after restart");
    TASSERT((obj_sub1 = PDCobj_open("restart_obj_sub1", pdc)) > 0, "Reopened restart_obj_sub1 after restart",
            "Failed to reopen restart_obj_sub1 after restart");
    TASSERT((obj_b1 = PDCobj_open("restart_obj_b1", pdc)) > 0, "Reopened restart_obj_b1 after restart",
            "Failed to reopen restart_obj_b1 after restart");

    TASSERT((root_info = PDCcont_get_info("restart_root")) != NULL, "Got restart_root info",
            "Failed to get restart_root info");
    TASSERT((branch_a_info = PDCcont_get_info("restart_branch_a")) != NULL, "Got restart_branch_a info",
            "Failed to get restart_branch_a info");
    TASSERT((branch_b_info = PDCcont_get_info("restart_branch_b")) != NULL, "Got restart_branch_b info",
            "Failed to get restart_branch_b info");
    TASSERT((branch_a_sub_info = PDCcont_get_info("restart_branch_a_sub")) != NULL,
            "Got restart_branch_a_sub info", "Failed to get restart_branch_a_sub info");
    TASSERT((obj_root1_info = PDCobj_get_info(obj_root1)) != NULL, "Got restart_obj_root1 info",
            "Failed to get restart_obj_root1 info");
    TASSERT((obj_a1_info = PDCobj_get_info(obj_a1)) != NULL, "Got restart_obj_a1 info",
            "Failed to get restart_obj_a1 info");
    TASSERT((obj_sub1_info = PDCobj_get_info(obj_sub1)) != NULL, "Got restart_obj_sub1 info",
            "Failed to get restart_obj_sub1 info");
    TASSERT((obj_b1_info = PDCobj_get_info(obj_b1)) != NULL, "Got restart_obj_b1 info",
            "Failed to get restart_obj_b1 info");

    // root must still contain {obj_root1(OBJ), branch_a(CONT), branch_b(CONT)}
    TASSERT(PDCcont_get_entries(root, &n_entries, &entries) == SUCCEED, "Got root entries after restart",
            "Failed to get root entries after restart");
    TASSERT(n_entries == 3, "root has 3 entries after restart", "root entry count changed across restart");
    TASSERT(has_entry(entries, n_entries, obj_root1_info->meta_id, PDC_ENTITY_OBJ),
            "root still contains obj_root1 after restart", "root lost obj_root1 across restart");
    TASSERT(has_entry(entries, n_entries, branch_a_info->meta_id, PDC_ENTITY_CONT),
            "root still contains nested branch_a after restart", "root lost nested branch_a across restart");
    TASSERT(has_entry(entries, n_entries, branch_b_info->meta_id, PDC_ENTITY_CONT),
            "root still contains nested branch_b after restart", "root lost nested branch_b across restart");
    free(entries);
    entries = NULL;

    // branch_a must still contain {obj_a1(OBJ), branch_a_sub(CONT)}
    TASSERT(PDCcont_get_entries(branch_a, &n_entries, &entries) == SUCCEED,
            "Got branch_a entries after restart", "Failed to get branch_a entries after restart");
    TASSERT(n_entries == 2, "branch_a has 2 entries after restart",
            "branch_a entry count changed across restart");
    TASSERT(has_entry(entries, n_entries, obj_a1_info->meta_id, PDC_ENTITY_OBJ),
            "branch_a still contains obj_a1 after restart", "branch_a lost obj_a1 across restart");
    TASSERT(has_entry(entries, n_entries, branch_a_sub_info->meta_id, PDC_ENTITY_CONT),
            "branch_a still contains nested branch_a_sub after restart",
            "branch_a lost nested branch_a_sub across restart");
    free(entries);
    entries = NULL;

    // branch_a_sub must still contain exactly {obj_sub1(OBJ)}
    TASSERT(PDCcont_get_entries(branch_a_sub, &n_entries, &entries) == SUCCEED,
            "Got branch_a_sub entries after restart", "Failed to get branch_a_sub entries after restart");
    TASSERT(n_entries == 1 && entries[0].meta_id == obj_sub1_info->meta_id &&
                entries[0].type == PDC_ENTITY_OBJ,
            "branch_a_sub still contains exactly obj_sub1 after restart",
            "branch_a_sub does not match expected structure after restart");
    free(entries);
    entries = NULL;

    // branch_b must still contain exactly {obj_b1(OBJ)}
    TASSERT(PDCcont_get_entries(branch_b, &n_entries, &entries) == SUCCEED,
            "Got branch_b entries after restart", "Failed to get branch_b entries after restart");
    TASSERT(n_entries == 1 && entries[0].meta_id == obj_b1_info->meta_id && entries[0].type == PDC_ENTITY_OBJ,
            "branch_b still contains exactly obj_b1 after restart",
            "branch_b does not match expected structure after restart");
    free(entries);
    entries = NULL;

    // Container kv tags must survive the restart (previously dropped
    // entirely by PDC_Server_checkpoint/_restart -- see
    // cont_nested_restart_build.c for where these were set).
    {
        char *         tag_value_ret = NULL;
        pdc_var_type_t value_type;
        psize_t        value_size;

        TASSERT(PDCcont_get_tag(root, "restart_tag", (void **)&tag_value_ret, &value_type, &value_size) ==
                    SUCCEED,
                "Got restart_root tag after restart", "Failed to get restart_root tag after restart");
        TASSERT(tag_value_ret != NULL && strcmp(tag_value_ret, "restart_tag_value") == 0,
                "restart_root tag value survived restart", "restart_root tag value did NOT survive restart");

        TASSERT(PDCcont_get_tag(branch_a, "restart_tag", (void **)&tag_value_ret, &value_type, &value_size) ==
                    SUCCEED,
                "Got restart_branch_a tag after restart", "Failed to get restart_branch_a tag after restart");
        TASSERT(tag_value_ret != NULL && strcmp(tag_value_ret, "restart_tag_value_branch_a") == 0,
                "restart_branch_a tag value survived restart",
                "restart_branch_a tag value did NOT survive restart");
    }

    // A nested child's name must still be resolvable by id after restart,
    // the same way it would be for an entry discovered via
    // PDCcont_get_entries() that was never directly opened by name.
    {
        char *resolved_name = NULL;

        TASSERT(PDCcont_get((pdcid_t)branch_a_info->meta_id, &resolved_name) == SUCCEED,
                "Resolved restart_branch_a's name by id after restart",
                "Failed to resolve restart_branch_a's name by id after restart");
        TASSERT(resolved_name != NULL && strcmp(resolved_name, "restart_branch_a") == 0,
                "restart_branch_a's name-by-id matches after restart",
                "restart_branch_a's name-by-id does NOT match after restart");
        free(resolved_name);
    }

    // Two parents each nesting a child container named "restart_dup_child":
    // confirm both survived the restart as distinct containers rather than
    // one silently clobbering the other via a hash collision (see
    // cont_nested_restart_build.c for how/why this was built).
    {
        uint64_t child_a_meta_id, child_b_meta_id;
        char *   resolved_name_a = NULL;
        char *   resolved_name_b = NULL;

        TASSERT((dup_parent_a = PDCcont_open("restart_dup_parent_a", pdc)) > 0,
                "Reopened restart_dup_parent_a after restart",
                "Failed to reopen restart_dup_parent_a after restart");
        TASSERT((dup_parent_b = PDCcont_open("restart_dup_parent_b", pdc)) > 0,
                "Reopened restart_dup_parent_b after restart",
                "Failed to reopen restart_dup_parent_b after restart");

        TASSERT(PDCcont_get_entries(dup_parent_a, &n_entries, &entries) == SUCCEED,
                "Got restart_dup_parent_a entries after restart",
                "Failed to get restart_dup_parent_a entries after restart");
        TASSERT(n_entries == 1 && entries[0].type == PDC_ENTITY_CONT,
                "restart_dup_parent_a has exactly its one nested child after restart",
                "restart_dup_parent_a's nested child did not survive the restart");
        child_a_meta_id = entries[0].meta_id;
        free(entries);
        entries = NULL;

        TASSERT(PDCcont_get_entries(dup_parent_b, &n_entries, &entries) == SUCCEED,
                "Got restart_dup_parent_b entries after restart",
                "Failed to get restart_dup_parent_b entries after restart");
        TASSERT(n_entries == 1 && entries[0].type == PDC_ENTITY_CONT,
                "restart_dup_parent_b has exactly its one nested child after restart",
                "restart_dup_parent_b's nested child did not survive the restart");
        child_b_meta_id = entries[0].meta_id;
        free(entries);
        entries = NULL;

        TASSERT(child_a_meta_id != child_b_meta_id,
                "Same-named children under different parents kept distinct ids after restart",
                "Same-named children under different parents collided onto one id after restart");

        // Both ids must still resolve to a live container -- not one that was
        // silently freed when the other's checkpoint entry collided with it.
        TASSERT(PDCcont_get((pdcid_t)child_a_meta_id, &resolved_name_a) == SUCCEED,
                "Resolved parent_a's child by id after restart",
                "parent_a's child did not survive the restart (not found as container)");
        TASSERT(resolved_name_a != NULL && strcmp(resolved_name_a, "restart_dup_child") == 0,
                "parent_a's child resolves to the right name after restart",
                "parent_a's child resolved to the wrong name after restart");
        free(resolved_name_a);

        TASSERT(PDCcont_get((pdcid_t)child_b_meta_id, &resolved_name_b) == SUCCEED,
                "Resolved parent_b's child by id after restart",
                "parent_b's child did not survive the restart (not found as container)");
        TASSERT(resolved_name_b != NULL && strcmp(resolved_name_b, "restart_dup_child") == 0,
                "parent_b's child resolves to the right name after restart",
                "parent_b's child resolved to the wrong name after restart");
        free(resolved_name_b);
    }

    TASSERT(PDCcont_close(root) >= 0, "Closed root", "Failed to close root");
    TASSERT(PDCcont_close(branch_a) >= 0, "Closed branch_a", "Failed to close branch_a");
    TASSERT(PDCcont_close(branch_b) >= 0, "Closed branch_b", "Failed to close branch_b");
    TASSERT(PDCcont_close(branch_a_sub) >= 0, "Closed branch_a_sub", "Failed to close branch_a_sub");
    TASSERT(PDCcont_close(dup_parent_a) >= 0, "Closed dup_parent_a", "Failed to close dup_parent_a");
    TASSERT(PDCcont_close(dup_parent_b) >= 0, "Closed dup_parent_b", "Failed to close dup_parent_b");
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
