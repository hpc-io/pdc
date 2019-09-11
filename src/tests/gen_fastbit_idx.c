#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/time.h>
#include <inttypes.h>
#include <unistd.h>

#include "pdc.h"
#include "pdc_client_connect.h"

int main(int argc, char **argv)
{
    int rank = 0, size = 1;
    pdcid_t obj_id;
    struct PDC_region_info region;
    uint64_t i, j, nhits;
    int ndim = 1;
    char* var_name = argv[1];
    pdcquery_t *qpreload_x;
    pdc_metadata_t *meta;
    pdcid_t pdc, id;
    double query_time = 0.0;
    float preload_value = -2000000000.0;

    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    struct timeval  pdc_timer_start_1;
    struct timeval  pdc_timer_end_1;

    pdc = PDC_init("pdc");

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
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

     return 0;
}
