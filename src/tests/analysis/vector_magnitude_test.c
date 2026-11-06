/**
 * Test Description
 * -----------------------------------------------------------------------------
 *
 * Exercises the region-analysis feature end to end: three input objects
 * (vx, vy, vz) and one output object (magnitude) are attached to a
 * "vector_magnitude" analysis graph. Writing vx/vy/vz is ordinary region
 * I/O -- nothing analysis-related runs. Reading magnitude for the first
 * time transparently computes it via the builtin multi-input
 * "vector_magnitude" transformation (sqrt(vx^2+vy^2+vz^2)) before serving
 * the read; a second read of the same region exercises the
 * already-materialized path (read straight from storage, no recompute).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

#include "pdc.h"
#include "test_helper.h"

#define BUF_LEN 128
#define EPSILON 1e-6

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop_in, obj_prop_out, reg, reg_global;
    pdcid_t vx_obj, vy_obj, vz_obj, mag_obj;
    pdcid_t dg_id;
    pdcid_t transfer_request;
    char    cont_name[128];
    int     rank      = 0, i;
    int     ret_value = TSUCCEED;

    uint64_t offset[1], offset_length[1];
    uint64_t dims[1];
    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    dims[0]          = PDC_SIZE_UNLIMITED;

    float * vx_data  = (float *)malloc(sizeof(float) * BUF_LEN);
    float * vy_data  = (float *)malloc(sizeof(float) * BUF_LEN);
    float * vz_data  = (float *)malloc(sizeof(float) * BUF_LEN);
    double *mag_data = (double *)malloc(sizeof(double) * BUF_LEN);

    for (i = 0; i < BUF_LEN; ++i) {
        vx_data[i] = (float)i;
        vy_data[i] = (float)(i + 1);
        vz_data[i] = (float)(i + 2);
    }

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif

    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    sprintf(cont_name, "c%d", rank);
    TASSERT((cont = PDCcont_create(cont_name, cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");

    // Input objects (vx, vy, vz) are PDC_FLOAT
    TASSERT((obj_prop_in = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT(PDCprop_set_obj_type(obj_prop_in, PDC_FLOAT) >= 0, "Call to PDCprop_set_obj_type succeeded",
            "Call to PDCprop_set_obj_type failed");
    TASSERT(PDCprop_set_obj_dims(obj_prop_in, 1, dims) >= 0, "Call to PDCprop_set_obj_dims succeeded",
            "Call to PDCprop_set_obj_dims failed");
    TASSERT(PDCprop_set_obj_user_id(obj_prop_in, getuid()) >= 0, "Call to PDCprop_set_obj_user_id succeeded",
            "Call to PDCprop_set_obj_user_id failed");
    TASSERT(PDCprop_set_obj_time_step(obj_prop_in, 0) >= 0, "Call to PDCprop_set_obj_time_step succeeded",
            "Call to PDCprop_set_obj_time_step failed");
    TASSERT(PDCprop_set_obj_app_name(obj_prop_in, "AnalysisTest") >= 0,
            "Call to PDCprop_set_obj_app_name succeeded", "Call to PDCprop_set_obj_app_name failed");
    TASSERT(PDCprop_set_obj_tags(obj_prop_in, "tag0=1") >= 0, "Call to PDCprop_set_obj_tags succeeded",
            "Call to PDCprop_set_obj_tags failed");
    TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop_in, PDC_REGION_STATIC) >= 0,
            "Call to PDCprop_set_obj_transfer_region_type succeeded",
            "Call to PDCprop_set_obj_transfer_region_type failed");

    // Output object (magnitude) is PDC_DOUBLE
    TASSERT((obj_prop_out = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT(PDCprop_set_obj_type(obj_prop_out, PDC_DOUBLE) >= 0, "Call to PDCprop_set_obj_type succeeded",
            "Call to PDCprop_set_obj_type failed");
    TASSERT(PDCprop_set_obj_dims(obj_prop_out, 1, dims) >= 0, "Call to PDCprop_set_obj_dims succeeded",
            "Call to PDCprop_set_obj_dims failed");
    TASSERT(PDCprop_set_obj_user_id(obj_prop_out, getuid()) >= 0, "Call to PDCprop_set_obj_user_id succeeded",
            "Call to PDCprop_set_obj_user_id failed");
    TASSERT(PDCprop_set_obj_time_step(obj_prop_out, 0) >= 0, "Call to PDCprop_set_obj_time_step succeeded",
            "Call to PDCprop_set_obj_time_step failed");
    TASSERT(PDCprop_set_obj_app_name(obj_prop_out, "AnalysisTest") >= 0,
            "Call to PDCprop_set_obj_app_name succeeded", "Call to PDCprop_set_obj_app_name failed");
    TASSERT(PDCprop_set_obj_tags(obj_prop_out, "tag0=1") >= 0, "Call to PDCprop_set_obj_tags succeeded",
            "Call to PDCprop_set_obj_tags failed");
    TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop_out, PDC_REGION_STATIC) >= 0,
            "Call to PDCprop_set_obj_transfer_region_type succeeded",
            "Call to PDCprop_set_obj_transfer_region_type failed");

    TASSERT((vx_obj = PDCobj_create(cont, "vx", obj_prop_in)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
    TASSERT((vy_obj = PDCobj_create(cont, "vy", obj_prop_in)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
    TASSERT((vz_obj = PDCobj_create(cont, "vz", obj_prop_in)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
    TASSERT((mag_obj = PDCobj_create(cont, "magnitude", obj_prop_out)) != 0,
            "Call to PDCobj_create succeeded", "Call to PDCobj_create failed");

    TASSERT((reg = PDCregion_create(1, offset, offset_length)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    // Attach the analysis graph: vx/vy/vz are inputs, magnitude is the output
    TASSERT((dg_id = PDCan_dg_json_create(AN_GRAPHS_DIR "vector_magnitude.json")) != 0,
            "Call to PDCan_dg_json_create succeeded", "Call to PDCan_dg_json_create failed");
    TASSERT(PDCan_attach_to_region(dg_id, "vx", vx_obj, reg_global) == SUCCEED,
            "Call to PDCan_attach_to_region succeeded for vx",
            "Call to PDCan_attach_to_region failed for vx");
    TASSERT(PDCan_attach_to_region(dg_id, "vy", vy_obj, reg_global) == SUCCEED,
            "Call to PDCan_attach_to_region succeeded for vy",
            "Call to PDCan_attach_to_region failed for vy");
    TASSERT(PDCan_attach_to_region(dg_id, "vz", vz_obj, reg_global) == SUCCEED,
            "Call to PDCan_attach_to_region succeeded for vz",
            "Call to PDCan_attach_to_region failed for vz");
    TASSERT(PDCan_attach_to_region(dg_id, "magnitude", mag_obj, reg_global) == SUCCEED,
            "Call to PDCan_attach_to_region succeeded for magnitude",
            "Call to PDCan_attach_to_region failed for magnitude");

    // Write vx, vy, vz -- ordinary region writes, nothing analysis-related runs
    TASSERT((transfer_request = PDCregion_transfer_create(vx_data, PDC_WRITE, vx_obj, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    TASSERT((transfer_request = PDCregion_transfer_create(vy_data, PDC_WRITE, vy_obj, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    TASSERT((transfer_request = PDCregion_transfer_create(vz_data, PDC_WRITE, vz_obj, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    // First read of magnitude: not materialized yet, should transparently
    // compute it via the analysis graph before serving the read.
    memset(mag_data, 0, sizeof(double) * BUF_LEN);
    TASSERT((transfer_request = PDCregion_transfer_create(mag_data, PDC_READ, mag_obj, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    for (i = 0; i < BUF_LEN; ++i) {
        double expected = sqrt((double)vx_data[i] * vx_data[i] + (double)vy_data[i] * vy_data[i] +
                               (double)vz_data[i] * vz_data[i]);
        if (fabs(mag_data[i] - expected) > EPSILON)
            TGOTO_ERROR(TFAIL, "First read: magnitude[%d] = %f, expected %f", i, mag_data[i], expected);
    }
    LOG_INFO("First read of magnitude (transparent compute) matched expected values\n");

    // Second read of the same region: should already be materialized, no
    // recompute, just a plain storage read -- verify it's still correct.
    memset(mag_data, 0, sizeof(double) * BUF_LEN);
    TASSERT((transfer_request = PDCregion_transfer_create(mag_data, PDC_READ, mag_obj, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    for (i = 0; i < BUF_LEN; ++i) {
        double expected = sqrt((double)vx_data[i] * vx_data[i] + (double)vy_data[i] * vy_data[i] +
                               (double)vz_data[i] * vz_data[i]);
        if (fabs(mag_data[i] - expected) > EPSILON)
            TGOTO_ERROR(TFAIL, "Second read: magnitude[%d] = %f, expected %f", i, mag_data[i], expected);
    }
    LOG_INFO("Second read of magnitude (already materialized) matched expected values\n");

    TASSERT(PDCan_close_dg(dg_id) >= 0, "Call to PDCan_close_dg succeeded", "Call to PDCan_close_dg failed");
    TASSERT(PDCregion_close(reg) >= 0, "Call to PDCregion_close succeeded", "Call to PDCregion_close failed");
    TASSERT(PDCregion_close(reg_global) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");
    TASSERT(PDCobj_close(vx_obj) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(vy_obj) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(vz_obj) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(mag_obj) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    TASSERT(PDCprop_close(obj_prop_in) >= 0, "Call to PDCprop_close succeeded",
            "Call to PDCprop_close failed");
    TASSERT(PDCprop_close(obj_prop_out) >= 0, "Call to PDCprop_close succeeded",
            "Call to PDCprop_close failed");
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

    free(vx_data);
    free(vy_data);
    free(vz_data);
    free(mag_data);

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
