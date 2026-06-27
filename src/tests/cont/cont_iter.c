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
#include <inttypes.h>
#include "pdc.h"
#include "test_helper.h"

int
main(int argc, char **argv)
{
    pdcid_t      pdc, create_prop, cont1, cont2, cont3;
    cont_handle *ch;
    int          rank = 0, size = 1;
    int          ret_value = TSUCCEED;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");

    // create a container property
    TASSERT((create_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
    TASSERT((cont1 = PDCcont_create("c1", create_prop)) != 0, "Call to PDCcont_create succeeded for c1",
            "Call to PDCcont_create failed for c1");
    // create second container
    TASSERT((cont2 = PDCcont_create("c2", create_prop)) != 0, "Call to PDCcont_create succeeded for c2",
            "Call to PDCcont_create failed for c2");
    // create third container
    TASSERT((cont3 = PDCcont_create("c3", create_prop)) != 0, "Call to PDCcont_create succeeded for c3",
            "Call to PDCcont_create failed for c3");

    // start container iteration
    ch = PDCcont_iter_start();
    while (!PDCcont_iter_null(ch)) {
        PDCcont_iter_get_info(ch);
        LOG_INFO("Container property id is %p\n", (void *)ch);
        ch = PDCcont_iter_next(ch);
    }

    // close cont1
    TASSERT(PDCcont_close(cont1) >= 0, "Call to PDCcont_close succeeded for c1",
            "Call to PDCcont_close failed for c1");
    // close cont2
    TASSERT(PDCcont_close(cont2) >= 0, "Call to PDCcont_close succeeded for c2",
            "Call to PDCcont_close failed for c2");
    // close cont3
    TASSERT(PDCcont_close(cont3) >= 0, "Call to PDCcont_close succeeded for c3",
            "Call to PDCcont_close failed for c3");
    // close a container property
    TASSERT(PDCprop_close(create_prop) >= 0, "Call to PDCprop_close succeeded",
            "Call to PDCprop_close failed");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return ret_value;
}
