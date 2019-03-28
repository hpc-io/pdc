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
    uint64_t i;
    int ndim = 1;

    pdc_metadata_t *x_meta, *y_meta, *z_meta, *energy_meta;
    pdcid_t pdc, x_id, y_id, z_id, energy_id;

    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    struct timeval  pdc_timer_start_1;
    struct timeval  pdc_timer_end_1;

    double query_time = 0.0;


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

    PDC_Client_query_metadata_name_timestep("Energy", 0, &energy_meta);
    if (energy_meta == NULL || energy_meta->obj_id == 0) {
        printf("Error with energy metadata!\n");
        goto done;
    }
    energy_id = energy_meta->obj_id;



    // Construct query constraints
    float x_lo0 = 0.05, x_hi0 = 200.05;
    float energy_lo0 = 1.5555, energy_hi0 = 1.5556;
    pdcquery_t *q0l, *q0h, *q0, *q1l, *q1h, *q1, *q2l, *q2h, *q2, *q, *q12;

    q0l = PDCquery_create(energy_id, PDC_GTE, PDC_FLOAT, &energy_lo0);
    q0h = PDCquery_create(energy_id, PDC_LTE, PDC_FLOAT, &energy_hi0);
    /* q0  = PDCquery_and(q0l, q0h); */

    q  = PDCquery_and(q0l, q0h);
    /* int id2_lo = 0, id2_hi = 32; */
    /* q1l = PDCquery_create(id2_id, PDC_GTE, PDC_INT, &id2_lo); */
    /* q1h = PDCquery_create(id2_id, PDC_LTE, PDC_INT, &id2_hi); */
    /* q1  = PDCquery_and(q1l, q1h); */

    /* q   = PDCquery_and(q0, q1); */

    /* q2l = PDCquery_create(obj_id, PDC_GTE, PDC_INT, &lo2); */
    /* q2h = PDCquery_create(obj_id, PDC_LT, PDC_INT, &hi2); */
    /* q2  = PDCquery_and(q2l, q2h); */

    /* q12 = PDCquery_or(q1, q2); */
    /* q   = PDCquery_or(q0, q12); */

    /* region.ndim = ndim; */
    /* region.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim); */
    /* region.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim); */
    /* region.offset[0] = 100; */
    /* region.size[0]   = 100; */
    /* PDCquery_sel_region(q, &region); */
   
    uint64_t nhits;
    pdcselection_t sel;
    /* PDCquery_get_nhits(q, &nhits); */
    /* printf("Query result: %" PRIu64 " hits\n", nhits); */

    // Get selection
    gettimeofday(&pdc_timer_start, 0);

    PDCquery_get_selection(q, &sel);

    gettimeofday(&pdc_timer_end, 0);
    double get_sel_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("Get selection time: %.4f\n", get_sel_time);

    printf("  Query results:\n");
    if (sel.nhits < 500) 
        PDCselection_print_all(&sel);
    else
        PDCselection_print(&sel);

    float *energy_data;
    if (sel.nhits > 0) 
        energy_data = (float*)calloc(sel.nhits, sizeof(float));

    // Get data
    gettimeofday(&pdc_timer_start, 0);

    PDCquery_get_data(energy_id, &sel, energy_data);

    gettimeofday(&pdc_timer_end, 0);
    double get_data_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("Get data time: %.4f\n", get_data_time);

    printf("Query result energy data (%" PRIu64 " hits):\n", sel.nhits);
    for (i = 0; i < sel.nhits; i++) {
        /* printf(" , %.3f", energy_data[i]); */
        if (energy_data[i] > energy_hi0 || energy_data[i] < energy_lo0) {
            printf("Error with result %" PRIu64 ": %.4f\n", i, energy_data[i]);
        }
    }
    printf("Verified: all correct!\n");


    PDCquery_free_all(q);
    /* PDCregion_free(&region); */
    PDCselection_free(&sel);


done:
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

     return 0;
}
