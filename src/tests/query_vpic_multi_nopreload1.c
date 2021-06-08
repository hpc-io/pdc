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
    uint64_t        i;
    int             j;
    pdc_metadata_t *energy_meta;
    pdcid_t         pdc, energy_id;

    // Construct query constraints
    pdc_selection_t sel;
    double          get_sel_time, get_data_time;
    float *         energy_data;
    pdc_query_t *   ql, *qh, *q;
    float           energy_lo0 = 2.8, energy_hi0 = 2.9;

    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;

    pdc = PDCinit("pdc");

    // Query the created object
    PDC_Client_query_metadata_name_timestep("Energy", 0, &energy_meta);
    if (energy_meta == NULL || energy_meta->obj_id == 0) {
        printf("Error with energy metadata!\n");
        goto done;
    }
    energy_id = energy_meta->obj_id;

    for (j = 0; j < 10; j++) {
        ql = PDCquery_create(energy_id, PDC_GTE, PDC_FLOAT, &energy_lo0);
        qh = PDCquery_create(energy_id, PDC_LTE, PDC_FLOAT, &energy_hi0);
        q  = PDCquery_and(ql, qh);

        // Get selection
        gettimeofday(&pdc_timer_start, 0);

        PDCquery_get_selection(q, &sel);

        gettimeofday(&pdc_timer_end, 0);
        get_sel_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        printf("Querying Energy in [%.2f, %.2f]\n", energy_lo0, energy_hi0);
        printf("Get selection time: %.4f\n", get_sel_time);

        if (sel.nhits > 0) {
            energy_data = (float *)calloc(sel.nhits, sizeof(float));

            // Get data
            gettimeofday(&pdc_timer_start, 0);

            PDCquery_get_data(energy_id, &sel, energy_data);

            gettimeofday(&pdc_timer_end, 0);
            get_data_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
            printf("Get data time: %.4f\n", get_data_time);

            printf("Query result energy data (%" PRIu64 " hits):\n", sel.nhits);
            for (i = 0; i < sel.nhits; i++) {
                if (energy_data[i] > energy_hi0 || energy_data[i] < energy_lo0) {
                    printf("Error with result %" PRIu64 ": %.4f\n", i, energy_data[i]);
                }
            }
            printf("Verified: all correct!\n");
            PDCselection_free(&sel);
            fflush(stdout);
            sleep(5);
        }

        free(energy_data);
        PDCquery_free_all(q);
        energy_lo0 -= 0.1;
        energy_hi0 -= 0.1;
    }

done:
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

    return 0;
}
