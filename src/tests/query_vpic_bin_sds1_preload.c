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
main(void)
{
    pdc_metadata_t *x_meta, *y_meta, *energy_meta;
    pdcid_t         pdc, x_id, y_id, energy_id;

    uint64_t        nhits;
    pdc_selection_t sel;
    double          get_sel_time, get_data_time;
    float *         energy_data = NULL, *x_data = NULL, *y_data = NULL;
    float           preload_value = 0;
    int             preload_int   = 0;
    pdc_query_t *qpreload_energy, *qpreload_x, *qpreload, *ql, *q2_lo, *q2_hi, *q2, *q3_lo, *q3_hi, *q3, *q,
        *q12, *qpreload_y, *qpreload_xy;
    float energy_lo0 = 3.0;
    float x_lo = 300, x_hi = 310;
    float y_lo = 140, y_hi = 150;

    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;

    pdc = PDCinit("pdc");

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

    PDC_Client_query_metadata_name_timestep("Energy", 0, &energy_meta);
    if (energy_meta == NULL || energy_meta->obj_id == 0) {
        printf("Error with energy metadata!\n");
        goto done;
    }
    energy_id = energy_meta->obj_id;

    // Construct query constraints
    printf("Preload the data\n");
    qpreload_energy = PDCquery_create(energy_id, PDC_GT, PDC_FLOAT, &preload_value);
    qpreload_x      = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &preload_int);
    qpreload_y      = PDCquery_create(y_id, PDC_GT, PDC_FLOAT, &preload_int);

    qpreload_xy = PDCquery_or(qpreload_x, qpreload_y);
    qpreload    = PDCquery_or(qpreload_energy, qpreload_xy);

    PDCquery_get_nhits(qpreload, &nhits);
    printf("Preload data, total %" PRIu64 " elements\n", nhits);
    PDCquery_free_all(qpreload);

    ql = PDCquery_create(energy_id, PDC_GT, PDC_FLOAT, &energy_lo0);

    q2_lo = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &x_lo);
    q2_hi = PDCquery_create(x_id, PDC_LT, PDC_FLOAT, &x_hi);
    q2    = PDCquery_and(q2_lo, q2_hi);

    q12 = PDCquery_and(q2, ql);

    q3_lo = PDCquery_create(y_id, PDC_GT, PDC_FLOAT, &y_lo);
    q3_hi = PDCquery_create(y_id, PDC_LT, PDC_FLOAT, &y_hi);

    q3 = PDCquery_and(q3_lo, q3_hi);

    q = PDCquery_and(q3, q12);
    printf("Query: Energy > %.1f && %.1f < X < %.1f && %.1f < Y < %.1f\n", energy_lo0, x_lo, x_hi, y_lo,
           y_hi);

    // Get selection
    gettimeofday(&pdc_timer_start, 0);

    PDCquery_get_selection(q, &sel);

    gettimeofday(&pdc_timer_end, 0);
    get_sel_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("Query result in (%" PRIu64 " hits):\n", sel.nhits);
    printf("Get selection time: %.4f\n", get_sel_time);

    if (sel.nhits > 0) {
        energy_data = (float *)calloc(sel.nhits, sizeof(float));
        x_data      = (float *)calloc(sel.nhits, sizeof(float));
        y_data      = (float *)calloc(sel.nhits, sizeof(float));

        // Get data
        gettimeofday(&pdc_timer_start, 0);

        PDCquery_get_data(energy_id, &sel, energy_data);
        PDCquery_get_data(x_id, &sel, x_data);
        PDCquery_get_data(y_id, &sel, y_data);

        gettimeofday(&pdc_timer_end, 0);
        get_data_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        printf("Get data time: %.4f\n", get_data_time);

        fflush(stdout);
    }

    PDCselection_free(&sel);
    if (energy_data)
        if (energy_data)
            free(energy_data);
    if (y_data)
        if (y_data)
            free(y_data);
    if (x_data)
        if (x_data)
            free(x_data);

    PDCquery_free_all(q);

done:
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

    return 0;
}
