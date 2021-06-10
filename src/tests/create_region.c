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
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/time.h>

#include "pdc.h"

int
main(int argc, char **argv)
{
    pdcid_t  pdc;
    int      ret_value = 0;
    uint64_t offset[3], offset_length[3];
    int      rank = 0, size = 1;
    offset[0]        = 0;
    offset[1]        = 2;
    offset[2]        = 5;
    offset_length[0] = 2;
    offset_length[1] = 3;
    offset_length[2] = 5;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc

    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    PDCregion_create(3, offset, offset_length);

    PDCregion_create(2, offset, offset_length);

    PDCregion_create(1, offset, offset_length);

    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("fail to close PDC\n");
        ret_value = 1;
    }

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
