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

    pdc_metadata_t *x_meta, *y_meta, *z_meta, *energy_meta;
    pdcid_t pdc, x_id, y_id, z_id, energy_id;
    float energy_lo = 1.2, energy_hi = 1000.0;
    float x_lo = 308, x_hi = 309;
    float y_lo = 149, y_hi = 150;
    float z_lo = 0,   z_hi = 66;

    pdcselection_t sel;
    double get_sel_time, get_data_time;
    float *energy_data = NULL, *x_data = NULL, *y_data = NULL, *z_data = NULL;
    uint64_t nhits, i;

    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;

    pdc = PDC_init("pdc");

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


    // Query the created object
    PDC_Client_query_metadata_name_timestep("x", 0, &x_meta);
    if (x_meta == NULL || x_meta->obj_id == 0) {
        printf("Error with x metadata!\n");
        goto done;
    }
    x_id = x_meta->obj_id;
    printf("x_id = %" PRIu64 "\n", x_id);
    
    
    PDC_Client_query_metadata_name_timestep("y", 0, &y_meta);
    if (y_meta == NULL || y_meta->obj_id == 0) {
        printf("Error with y metadata!\n");
        goto done;
    }
    y_id = y_meta->obj_id;
    printf("y_id = %" PRIu64 "\n", y_id);

    PDC_Client_query_metadata_name_timestep("z", 0, &z_meta);
    if (z_meta == NULL || z_meta->obj_id == 0) {
        printf("Error with z metadata!\n");
        goto done;
    }
    z_id = z_meta->obj_id;
    printf("z_id = %" PRIu64 "\n", z_id);

    PDC_Client_query_metadata_name_timestep("Energy", 0, &energy_meta);
    if (energy_meta == NULL || energy_meta->obj_id == 0) {
        printf("Error with energy metadata!\n");
        goto done;
    }
    energy_id = energy_meta->obj_id;
    printf("energy_id = %" PRIu64 "\n", energy_id);

    printf("Distribute the metadata\n");
    float preload_value = 1000000.0;
    pdcquery_t *qpreload_energy = PDCquery_create(energy_id, PDC_GT, PDC_FLOAT, &preload_value);
    pdcquery_t *qpreload_x      = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &preload_value);
    /* pdcquery_t *qpreload_y      = PDCquery_create(y_id, PDC_GT, PDC_FLOAT, &preload_value); */
    /* pdcquery_t *qpreload_z      = PDCquery_create(z_id, PDC_GT, PDC_FLOAT, &preload_value); */

    /* pdcquery_t *qpreload_ex     = PDCquery_or(qpreload_x, qpreload_energy); */
    /* pdcquery_t *qpreload_exy    = PDCquery_or(qpreload_y, qpreload_ex); */
    /* pdcquery_t *qpreload        = PDCquery_or(qpreload_z, qpreload_exy); */

    pdcquery_t *qpreload        = PDCquery_or(qpreload_x, qpreload_energy);

    PDCquery_get_nhits(qpreload, &nhits);
    printf("Done... %" PRIu64 " hits\n", nhits);
    PDCquery_free_all(qpreload);

    // Construct query constraints
    pdcquery_t *q1_lo = PDCquery_create(energy_id, PDC_GT, PDC_FLOAT, &energy_lo);
    pdcquery_t *q1_hi = PDCquery_create(energy_id, PDC_LT, PDC_FLOAT, &energy_hi);
    pdcquery_t *q1    = PDCquery_and(q1_lo, q1_hi);

    pdcquery_t *q2_lo = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &x_lo);
    pdcquery_t *q2_hi = PDCquery_create(x_id, PDC_LT, PDC_FLOAT, &x_hi);
    pdcquery_t *q2    = PDCquery_and(q2_lo, q2_hi);

    /* pdcquery_t *q3_lo = PDCquery_create(y_id, PDC_GT, PDC_FLOAT, &y_lo); */
    /* pdcquery_t *q3_hi = PDCquery_create(y_id, PDC_LT, PDC_FLOAT, &y_hi); */
    /* pdcquery_t *q3    = PDCquery_and(q3_lo, q3_hi); */

    /* pdcquery_t *q4_lo = PDCquery_create(z_id, PDC_GT, PDC_FLOAT, &z_lo); */
    /* pdcquery_t *q4_hi = PDCquery_create(z_id, PDC_LT, PDC_FLOAT, &z_hi); */
    /* pdcquery_t *q4    = PDCquery_and(q4_lo, q4_hi); */

    /* pdcquery_t *q12   = PDCquery_and(q2, q1); */
    /* pdcquery_t *q123  = PDCquery_and(q3, q12); */
    /* pdcquery_t *q     = PDCquery_and(q4, q123); */

    pdcquery_t *q     = PDCquery_and(q2, q1);

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
        y_data      = (float*)calloc(sel.nhits, sizeof(float));
        z_data      = (float*)calloc(sel.nhits, sizeof(float));

        // Get data
        gettimeofday(&pdc_timer_start, 0);

        PDCquery_get_data(energy_id, &sel, energy_data);
        PDCquery_get_data(x_id, &sel, x_data);
        /* PDCquery_get_data(y_id, &sel, y_data); */
        /* PDCquery_get_data(z_id, &sel, z_data); */

        gettimeofday(&pdc_timer_end, 0);
        get_data_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        printf("Get data time: %.4f\n", get_data_time);

        for (i = 0; i < sel.nhits; i++) {
            if (energy_data[i] < energy_lo ) {
                printf("Error with energy_data = %.1f\n", energy_data[i]);
                break;
            }
            if (x_data[i] > x_hi || x_data[i] < x_lo) {
                printf("Error with x_data = %.1f\n", x_data[i]);
                break;
            }
            /* if (y_data[i] > y_hi || y_data[i] < y_lo) { */
            /*     printf("Error with y_data = %.1f\n", y_data[i]); */
            /*     break; */
            /* } */
            /* if (z_data[i] > z_hi || z_data[i] < z_lo) { */
            /*     printf("Error with z_data = %.1f\n", z_data[i]); */
            /*     break; */
            /* } */
        }

        fflush(stdout);
    }

    PDCselection_free(&sel);
    if(energy_data) free(energy_data);
    if(x_data)      free(x_data);
    if(y_data)      free(y_data);
    if(z_data)      free(z_data);

    PDCquery_free_all(q);

done:
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

     return 0;
}
