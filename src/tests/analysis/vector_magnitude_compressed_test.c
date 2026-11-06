/**
 * Test Description
 * -----------------------------------------------------------------------------
 *
 * Same as vector_magnitude_test.c, except the "magnitude" output object
 * also has a PDC TF compression graph (zfp) attached to it. Verifies that
 * an analysis output composes correctly with a separately-attached PDC TF
 * transform: PDCan_exec_graph's write of magnitude should be transparently
 * compressed on the way to storage, and a client read back should be
 * transparently decompressed -- exercising both the write-side composition
 * (already worked, since the executor writes through the generic
 * PDC_Server_transfer_request_io entry point) and the read-side
 * composition (the fix that made the "already materialized" read path
 * also check for an attached TF graph instead of reading raw bytes).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

#include "pdc.h"
#include "test_helper.h"

#define BUF_LEN 128
#define EPSILON                                                                                              \
    1e-2 /* zfp is lossy compression; verify "round-tripped                                                  \
            correctly", not bit-exact */

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop_in, obj_prop_out, reg, reg_global;
    pdcid_t vx_obj, vy_obj, vz_obj, mag_obj;
    pdcid_t dg_id, tf_dg_id;
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
    sprintf(cont_name, "ccomp%d", rank);
    TASSERT((cont = PDCcont_create(cont_name, cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");

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
    TASSERT(PDCprop_set_obj_app_name(obj_prop_in, "AnalysisCompressedTest") >= 0,
            "Call to PDCprop_set_obj_app_name succeeded", "Call to PDCprop_set_obj_app_name failed");
    TASSERT(PDCprop_set_obj_tags(obj_prop_in, "tag0=1") >= 0, "Call to PDCprop_set_obj_tags succeeded",
            "Call to PDCprop_set_obj_tags failed");
    TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop_in, PDC_REGION_STATIC) >= 0,
            "Call to PDCprop_set_obj_transfer_region_type succeeded",
            "Call to PDCprop_set_obj_transfer_region_type failed");

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
    TASSERT(PDCprop_set_obj_app_name(obj_prop_out, "AnalysisCompressedTest") >= 0,
            "Call to PDCprop_set_obj_app_name succeeded", "Call to PDCprop_set_obj_app_name failed");
    TASSERT(PDCprop_set_obj_tags(obj_prop_out, "tag0=1") >= 0, "Call to PDCprop_set_obj_tags succeeded",
            "Call to PDCprop_set_obj_tags failed");
    TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop_out, PDC_REGION_STATIC) >= 0,
            "Call to PDCprop_set_obj_transfer_region_type succeeded",
            "Call to PDCprop_set_obj_transfer_region_type failed");

    TASSERT((vx_obj = PDCobj_create(cont, "vxc", obj_prop_in)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
    TASSERT((vy_obj = PDCobj_create(cont, "vyc", obj_prop_in)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
    TASSERT((vz_obj = PDCobj_create(cont, "vzc", obj_prop_in)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
    TASSERT((mag_obj = PDCobj_create(cont, "magnitudec", obj_prop_out)) != 0,
            "Call to PDCobj_create succeeded", "Call to PDCobj_create failed");

    TASSERT((reg = PDCregion_create(1, offset, offset_length)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    // Attach a PDC TF compression graph to magnitude's own region, on top
    // of (separately) attaching it as an analysis output below.
    TASSERT((tf_dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "zfp.json")) != 0,
            "Call to PDCtf_dg_json_create succeeded", "Call to PDCtf_dg_json_create failed");
    TASSERT(PDCtf_attach_to_region(tf_dg_id, mag_obj, reg_global, "decompressed", "compressed") == SUCCEED,
            "Call to PDCtf_attach_to_region succeeded for magnitude",
            "Call to PDCtf_attach_to_region failed for magnitude");

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

    // Writing vz is the last input -- this should eagerly compute AND
    // compress magnitude before this write call even returns.
    TASSERT((transfer_request = PDCregion_transfer_create(vz_data, PDC_WRITE, vz_obj, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    // Read magnitude back -- should be transparently decompressed.
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
            TGOTO_ERROR(TFAIL, "magnitude[%d] = %f, expected ~%f (compressed round trip)", i, mag_data[i],
                        expected);
    }
    LOG_INFO("Compressed magnitude read (analysis + PDC TF composed) matched expected values\n");

    TASSERT(PDCtf_close_dg(tf_dg_id) >= 0, "Call to PDCtf_close_dg succeeded",
            "Call to PDCtf_close_dg failed");
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
