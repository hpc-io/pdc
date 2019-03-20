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
    /* struct PDC_region_info region; */
    uint64_t i;
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    pdc_metadata_t *x_meta, *y_meta, *z_meta, *id1_meta, *id2_meta;
    pdcid_t pdc, x_id, y_id, z_id, id1_id, id2_id;

    pdc = PDC_init("pdc");

    // Query the created object
    PDC_Client_query_metadata_name_timestep("x", 0, &x_meta);
    if (x_meta == NULL || x_meta->obj_id == 0) {
        printf("Error with x metadata!\n");
        goto done;
    }
    x_id = x_meta->obj_id;
    
    PDC_Client_query_metadata_name_timestep("y", 0, &y_meta);
    if (y_meta == NULL || y_meta->obj_id == 0) {
        printf("Error with y metadata!\n");
        goto done;
    }
    y_id = y_meta->obj_id;

    PDC_Client_query_metadata_name_timestep("z", 0, &z_meta);
    if (z_meta == NULL || z_meta->obj_id == 0) {
        printf("Error with z metadata!\n");
        goto done;
    }
    z_id = z_meta->obj_id;

    PDC_Client_query_metadata_name_timestep("id1", 0, &id1_meta);
    if (id1_meta == NULL || id1_meta->obj_id == 0) {
        printf("Error with id1 metadata!\n");
        goto done;
    }
    id1_id = id1_meta->obj_id;

    PDC_Client_query_metadata_name_timestep("id2", 0, &id2_meta);
    if (id2_meta == NULL || id2_meta->obj_id == 0) {
        printf("Error with id2 metadata!\n");
        goto done;
    }
    id2_id = id2_meta->obj_id;
 

    /* region.ndim = ndim; */
    /* region.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim); */
    /* region.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim); */
    /* region.offset[0] = rank * my_data_count; */
    /* region.size[0] = my_data_count; */

    // Construct query constraints
    float x_lo0 = 0.05, x_hi0 = 200.05;
    pdcquery_t *q0l, *q0h, *q1l, *q1h, *q1, *q2l, *q2h, *q2, *q, *q12;

    q0l = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &x_lo0);
    q0h = PDCquery_create(x_id, PDC_LT, PDC_FLOAT, &x_hi0);
    q   = PDCquery_and(q0l, q0h);

    /* PDCquery_sel_region(q0, &region); */

    /* q1l = PDCquery_create(obj_id, PDC_GTE, PDC_INT, &lo1); */
    /* q1h = PDCquery_create(obj_id, PDC_LT, PDC_INT, &hi1); */
    /* q1  = PDCquery_and(q1l, q1h); */

    /* q2l = PDCquery_create(obj_id, PDC_GTE, PDC_INT, &lo2); */
    /* q2h = PDCquery_create(obj_id, PDC_LT, PDC_INT, &hi2); */
    /* q2  = PDCquery_and(q2l, q2h); */

    /* q12 = PDCquery_or(q1, q2); */
    /* q   = PDCquery_or(q0, q12); */

   
    uint64_t nhits;
    pdcselection_t sel;
    /* PDCquery_get_nhits(q, &nhits); */
    /* printf("Query result: %" PRIu64 " hits\n", nhits); */

    PDCquery_get_selection(q, &sel);
    PDCselection_print(&sel);

    PDCquery_free_all(q);
    /* PDCregion_free(&region); */
    PDCselection_free(&sel);


done:
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

     return 0;
}
