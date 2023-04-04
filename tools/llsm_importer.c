#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"

