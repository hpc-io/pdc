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
    /* struct PDC_region_info region; */
    float energy_lo = 3.0, energy_hi = 1000.0;
    float x_lo = 300, x_hi = 310;
    float y_lo = 140, y_hi = 150;
    float z_lo = 0,   z_hi = 66;

    pdc_metadata_t *x_meta, *y_meta, *z_meta, *energy_meta;
    pdcid_t pdc, x_id, y_id, z_id, energy_id;

    uint64_t nhits;
    pdcselection_t sel;
    double get_sel_time, get_data_time;
    float *energy_data = NULL, *x_data = NULL, *y_data = NULL, *z_data = NULL;

    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;

    if (argc > 8) {
        energy_lo  = atof(argv[1]);
        energy_hi  = atof(argv[2]);
        x_lo       = atof(argv[3]);
        x_hi       = atof(argv[4]);
        y_lo       = atof(argv[5]);
        y_hi       = atof(argv[6]);
        z_lo       = atof(argv[7]);
        z_hi       = atof(argv[8]);
    }
    else {
        printf("Not sufficient query conditions!\n");
    }

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
    printf("Preload the data\n");
    float preload_value = -10000.0;
    pdcquery_t *qpreload_energy = PDCquery_create(energy_id, PDC_GT, PDC_FLOAT, &preload_value);
    pdcquery_t *qpreload_x      = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &preload_value);
    /* pdcquery_t *qpreload_y      = PDCquery_create(y_id, PDC_GT, PDC_FLOAT, &preload_value); */
    /* pdcquery_t *qpreload_z      = PDCquery_create(z_id, PDC_GT, PDC_FLOAT, &preload_value); */

    /* pdcquery_t *qpreload_ex     = PDCquery_or(qpreload_x, qpreload_energy); */
    /* pdcquery_t *qpreload_exy    = PDCquery_or(qpreload_y, qpreload_ex); */
    /* pdcquery_t *qpreload        = PDCquery_or(qpreload_z, qpreload_exy); */

    pdcquery_t *qpreload        = PDCquery_or(qpreload_x, qpreload_energy);

    PDCquery_get_nhits(qpreload, &nhits);
    printf("Preload data, total %" PRIu64 " elements\n", nhits);
    PDCquery_free_all(qpreload);

    pdcquery_t *q1_lo = PDCquery_create(energy_id, PDC_GT, PDC_FLOAT, &energy_lo);
    pdcquery_t *q1_hi = PDCquery_create(energy_id, PDC_LT, PDC_FLOAT, &energy_hi);
    pdcquery_t *q1    = PDCquery_and(q1_lo, q1_hi);

    pdcquery_t *q2_lo = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &x_lo);
    pdcquery_t *q2_hi = PDCquery_create(x_id, PDC_LT, PDC_FLOAT, &x_hi);
    pdcquery_t *q2    = PDCquery_and(q2_lo, q2_hi);

    /* pdcquery_t *q12 = PDCquery_and(q2, q1); */

    /* pdcquery_t *q3_lo = PDCquery_create(y_id, PDC_GT, PDC_FLOAT, &y_lo); */
    /* pdcquery_t *q3_hi = PDCquery_create(y_id, PDC_LT, PDC_FLOAT, &y_hi); */
    /* pdcquery_t *q3    = PDCquery_and(q3_lo, q3_hi); */

    /* pdcquery_t *q123 = PDCquery_and(q3, q12); */

    /* pdcquery_t *q4_lo = PDCquery_create(z_id, PDC_GT, PDC_FLOAT, &z_lo); */
    /* pdcquery_t *q4_hi = PDCquery_create(z_id, PDC_LT, PDC_FLOAT, &z_hi); */
    /* pdcquery_t *q4    = PDCquery_and(q4_lo, q4_hi); */

    /* pdcquery_t *q = PDCquery_and(q4, q123); */

    pdcquery_t *q = PDCquery_and(q2, q1);

    printf("Query: %.1f < Energy < %.1f && %.1f < X < %.1f && %.1f < Y < %.1f, && %.1f < z < %.1f\n", 
            energy_lo, energy_hi, x_lo, x_hi, y_lo, y_hi, z_lo, z_hi);


    // Get selection
    gettimeofday(&pdc_timer_start, 0);

    PDCquery_get_selection(q, &sel);

    gettimeofday(&pdc_timer_end, 0);
    get_sel_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("Query result in (%" PRIu64 " hits):\n", sel.nhits);
    printf("Get selection time: %.4f\n", get_sel_time);

    if (sel.nhits > 0) {
        energy_data = (float*)calloc(sel.nhits, sizeof(float));
        x_data      = (float*)calloc(sel.nhits, sizeof(float));
        /* y_data      = (float*)calloc(sel.nhits, sizeof(float)); */
        /* z_data      = (float*)calloc(sel.nhits, sizeof(float)); */

        // Get data
        gettimeofday(&pdc_timer_start, 0);

        PDCquery_get_data(energy_id, &sel, energy_data);
        PDCquery_get_data(x_id, &sel, x_data);
        /* PDCquery_get_data(y_id, &sel, y_data); */
        /* PDCquery_get_data(z_id, &sel, z_data); */

        gettimeofday(&pdc_timer_end, 0);
        get_data_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        printf("Get data time: %.4f\n", get_data_time);

        fflush(stdout);
    }

    PDCselection_free(&sel);
    if(energy_data) if (energy_data) free(energy_data);
    if(x_data)      if (x_data) free(x_data);
    if(y_data)      if (y_data) free(y_data);
    if(z_data)      free(z_data);

    PDCquery_free_all(q);

done:
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

     return 0;
}
