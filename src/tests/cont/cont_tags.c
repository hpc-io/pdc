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
#include <string.h>
#include "pdc.h"
#include "test_helper.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, cont2;
    int     ret_value = TSUCCEED;

    int rank = 0, size = 1;

    char           tag_value[128], tag_value2[128], *tag_value_ret;
    pdc_var_type_t value_type;
    psize_t        value_size;
    strcpy(tag_value, "some tag value");
    strcpy(tag_value2, "some tag value 2 is longer than tag 1");

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");

    // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
    TASSERT((cont = PDCcont_create("c1", cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");
    TASSERT((cont2 = PDCcont_create("c2", cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");

#ifdef ENABLE_MPI
    // All ranks use the same containers and keys.
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    TASSERT(PDCcont_put_tag(cont, "some tag", tag_value, PDC_STRING, strlen(tag_value) + 1) >= 0,
            "Call to PDCcont_put_tag succeeded for container 1",
            "Call to PDCcont_put_tag failed for container 1");
    TASSERT(PDCcont_put_tag(cont, "some tag 2", tag_value2, PDC_STRING, strlen(tag_value2) + 1) >= 0,
            "Call to PDCcont_put_tag succeeded for container 2",
            "Call to PDCcont_put_tag failed for container 1");

    TASSERT(PDCcont_put_tag(cont2, "some tag", tag_value, PDC_STRING, strlen(tag_value) + 1) >= 0,
            "Call to PDCcont_put_tag succeeded for container 1",
            "Call to PDCcont_put_tag failed for container 2");
    TASSERT(PDCcont_put_tag(cont2, "some tag 2", tag_value2, PDC_STRING, strlen(tag_value2) + 1) >= 0,
            "Call to PDCcont_put_tag succeeded for container 2",
            "Call to PDCcont_put_tag failed for container 2");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    TASSERT(PDCcont_get_tag(cont, "some tag", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCcont_get_tag succeeded for container 1",
            "Call to PDCcont_get_tag failed for container 1");

    if (tag_value_ret == NULL || value_type != PDC_STRING || value_size != strlen(tag_value) + 1 ||
        memcmp(tag_value, tag_value_ret, value_size) != 0)
        TGOTO_ERROR(TFAIL, "Wrong tag value at container 1, expected [%s]", tag_value);
    free(tag_value_ret);

    TASSERT(PDCcont_get_tag(cont, "some tag 2", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCcont_get_tag succeeded for container 1",
            "Call to PDCcont_get_tag failed for container 1");

    if (tag_value_ret == NULL || value_type != PDC_STRING || value_size != strlen(tag_value2) + 1 ||
        memcmp(tag_value2, tag_value_ret, value_size) != 0)
        TGOTO_ERROR(TFAIL, "Wrong tag value at container 1, expected [%s]", tag_value2);
    free(tag_value_ret);

    TASSERT(PDCcont_get_tag(cont2, "some tag", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCcont_get_tag succeeded for container 2",
            "Call to PDCcont_get_tag failed for container 2");

    if (tag_value_ret == NULL || value_type != PDC_STRING || value_size != strlen(tag_value) + 1 ||
        memcmp(tag_value, tag_value_ret, value_size) != 0)
        TGOTO_ERROR(TFAIL, "Wrong tag value at container 2, expected [%s]", tag_value);
    free(tag_value_ret);

    TASSERT(PDCcont_get_tag(cont2, "some tag 2", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCcont_get_tag succeeded for container 2",
            "Call to PDCcont_get_tag failed for container 2");

    if (tag_value_ret == NULL || value_type != PDC_STRING || value_size != strlen(tag_value2) + 1 ||
        memcmp(tag_value2, tag_value_ret, value_size) != 0)
        TGOTO_ERROR(TFAIL, "Wrong tag value at container 2, expected [%s]", tag_value2);
    free(tag_value_ret);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    TASSERT(PDCcont_del_tag(cont2, "some tag 2") >= 0, "Call to PDCcont_del_tag succeeded for container 2",
            "Call to PDCcont_del_tag failed for container 2");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    TASSERT(PDCcont_get_tag(cont2, "some tag 2", (void **)&tag_value_ret, &value_type, &value_size) >= 0,
            "Call to PDCcont_get_tag succeeded for container 2",
            "Call to PDCcont_get_tag failed for container 2");

    if (tag_value_ret != NULL || value_size != 0)
        TGOTO_ERROR(TFAIL, "Error: got non-empty tag after deletion");
    else
        LOG_INFO("Verified the tag has been deleted successfully\n");

    // close a container
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    // close a container
    TASSERT(PDCcont_close(cont2) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");

    // close a container property
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    if (ret_value != TSUCCEED)
        MPI_Abort(MPI_COMM_WORLD, ret_value);
    MPI_Finalize();
#endif
    return ret_value;
}
