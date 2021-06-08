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
    int rank = 0;

    pdc_metadata_t *x_meta, *y_meta, *z_meta, *energy_meta;
    pdcid_t         pdc, x_id, y_id, z_id, energy_id;

    // Construct query constraints
    pdc_selection_t sel;
    double          get_sel_time, get_data_time;
    float *         energy_data = NULL, *x_data = NULL, *y_data = NULL;

    float energy_lo = 1.2, energy_hi = 1.3;
    float x_lo = 308, x_hi = 309;
    float y_lo = 149, y_hi = 150;

    pdc_query_t *q1_lo, *q1_hi, *q1, *q2_lo, *q2_hi, *q2, *q12, *q3_lo, *q3_hi, *q3, *q;

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
    if (rank == 0)
        printf("x_id = %" PRIu64 "\n", x_id);

    PDC_Client_query_metadata_name_timestep("y", 0, &y_meta);
    if (y_meta == NULL || y_meta->obj_id == 0) {
        printf("Error with y metadata!\n");
        goto done;
    }
    y_id = y_meta->obj_id;
    if (rank == 0)
        printf("y_id = %" PRIu64 "\n", y_id);

    PDC_Client_query_metadata_name_timestep("z", 0, &z_meta);
    if (z_meta == NULL || z_meta->obj_id == 0) {
        printf("Error with z metadata!\n");
        goto done;
    }
    z_id = z_meta->obj_id;
    if (rank == 0)
        printf("z_id = %" PRIu64 "\n", z_id);

    PDC_Client_query_metadata_name_timestep("Energy", 0, &energy_meta);
    if (energy_meta == NULL || energy_meta->obj_id == 0) {
        printf("Error with energy metadata!\n");
        goto done;
    }
    energy_id = energy_meta->obj_id;
    if (rank == 0)
        printf("energy_id = %" PRIu64 "\n", energy_id);

    q1_lo = PDCquery_create(energy_id, PDC_GT, PDC_FLOAT, &energy_lo);
    q1_hi = PDCquery_create(energy_id, PDC_LT, PDC_FLOAT, &energy_hi);
    q1    = PDCquery_and(q1_lo, q1_hi);

    q2_lo = PDCquery_create(x_id, PDC_GT, PDC_FLOAT, &x_lo);
    q2_hi = PDCquery_create(x_id, PDC_LT, PDC_FLOAT, &x_hi);
    q2    = PDCquery_and(q2_lo, q2_hi);

    q12 = PDCquery_and(q2, q1);

    q3_lo = PDCquery_create(y_id, PDC_GT, PDC_FLOAT, &y_lo);
    q3_hi = PDCquery_create(y_id, PDC_LT, PDC_FLOAT, &y_hi);

    q3 = PDCquery_and(q3_lo, q3_hi);

    q = PDCquery_and(q3, q12);

    printf("Query: %.1f< Energy < %.1f && %.1f < X < %.1f && %.1f < Y < %.1f\n", energy_lo, energy_hi, x_lo,
           x_hi, y_lo, y_hi);

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
        free(energy_data);
    if (y_data)
        free(y_data);
    if (x_data)
        free(x_data);

    PDCquery_free_all(q);

done:
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

    return 0;
}
