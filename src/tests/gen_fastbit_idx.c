#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/time.h>
#include <inttypes.h>
#include <unistd.h>

#include "pdc.h"
#include "pdc_client_connect.h"

int
main(int argc, char **argv)
{
    uint64_t        nhits;
    char *          var_name;
    pdc_query_t *   qpreload_x;
    pdc_metadata_t *meta;
    pdcid_t         pdc, id;
    float           preload_value = -2000000000.0;

    if (argc < 2) {
        printf("Please enter var name as input!\n");
        fflush(stdout);
    }
    var_name = argv[1];

    pdc = PDCinit("pdc");

    // Query the created object
    PDC_Client_query_metadata_name_timestep(var_name, 0, &meta);
    if (meta == NULL || meta->obj_id == 0) {
        printf("Error with [%s] metadata!\n", var_name);
        goto done;
    }
    id = meta->obj_id;

    qpreload_x = PDCquery_create(id, PDC_GT, PDC_FLOAT, &preload_value);

    PDCquery_get_nhits(qpreload_x, &nhits);
    printf("Generated Fastbit index for [%s], total %" PRIu64 " elements\n", var_name, nhits);
    PDCquery_free_all(qpreload_x);

done:
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

    return 0;
}
