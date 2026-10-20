#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"
#include "pdc_client_connect.h"

#ifdef ENABLE_MPI
#include <mpi.h>
#endif

#define OBJECTS_PER_RANK 8

static int rank;

static void
require(int condition, const char *message)
{
    if (condition)
        return;
    fprintf(stderr, "SOMETA rank %d: %s\n", rank, message);
#ifdef ENABLE_MPI
    MPI_Abort(MPI_COMM_WORLD, 1);
#endif
    exit(1);
}

static int
compare_ids(const void *left, const void *right)
{
    uint64_t a = *(const uint64_t *)left, b = *(const uint64_t *)right;
    return (a > b) - (a < b);
}

static void
check_query(uint64_t *expected, int count)
{
    char        query_value[] = "\"selected\"";
    pdc_kvtag_t query         = {
        .name = "someta_group", .value = query_value, .type = PDC_STRING, .size = sizeof(query_value)};
    uint64_t *actual = NULL;
    int       found  = -1;
    require(PDC_Client_query_kvtag(&query, &found, &actual) >= 0, "query failed");
    require(found == count, "query returned an incorrect object count");
    qsort(actual, found, sizeof(*actual), compare_ids);
    qsort(expected, count, sizeof(*expected), compare_ids);
    require(memcmp(actual, expected, count * sizeof(*expected)) == 0,
            "query returned incorrect or duplicate object IDs");
    free(actual);
}

int
main(int argc, char **argv)
{
    int size = 1;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    int servers = argc > 1 ? atoi(argv[1]) : 1;
    require(servers > 0, "invalid expected server count");
    pdcid_t pdc = PDCinit("pdc");
    require(pdc != 0, "PDCinit failed");
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    pdcid_t obj_prop  = PDCprop_create(PDC_OBJ_CREATE, pdc);
    require(cont_prop && obj_prop, "property creation failed");
    char name[128];
    snprintf(name, sizeof(name), "someta_container_%d", rank);
    pdcid_t cont = PDCcont_create(name, cont_prop);
    require(cont != 0, "container creation failed");
    int container_value = rank + 17;
    require(PDCcont_put_tag(cont, "someta_container", &container_value, PDC_INT, sizeof(container_value)) >=
                0,
            "container tag insertion failed");
    void *         value = NULL;
    pdc_var_type_t type;
    psize_t        bytes;
    require(PDCcont_get_tag(cont, "someta_container", &value, &type, &bytes) >= 0,
            "container tag read failed");
    require(value && type == PDC_INT && bytes == sizeof(container_value) &&
                memcmp(value, &container_value, bytes) == 0,
            "container tag payload changed");
    free(value);

    pdcid_t  objects[OBJECTS_PER_RANK];
    uint64_t local_ids[OBJECTS_PER_RANK], *all_ids = malloc(size * sizeof(local_ids));
    require(all_ids != NULL, "ID allocation failed");
    for (int i = 0; i < OBJECTS_PER_RANK; i++) {
        snprintf(name, sizeof(name), "someta_object_%d_%d", rank, i);
        objects[i] = PDCobj_create(cont, name, obj_prop);
        require(objects[i] != 0, "object creation failed");
        struct pdc_obj_info *info = PDCobj_get_info(objects[i]);
        require(info != NULL, "object metadata unavailable");
        local_ids[i]           = info->meta_id;
        char *        group    = i % 2 == 0 ? "selected" : "other";
        unsigned char binary[] = {0, 0xff, (unsigned char)rank, (unsigned char)i, 0x80};
        require(PDCobj_put_tag(objects[i], "someta_group", group, PDC_STRING, strlen(group) + 1) >= 0,
                "string tag insertion failed");
        require(PDCobj_put_tag(objects[i], "someta_binary", binary, PDC_UINT8, sizeof(binary)) >= 0,
                "binary tag insertion failed");
        value = NULL;
        require(PDCobj_get_tag(objects[i], "someta_binary", &value, &type, &bytes) >= 0,
                "binary tag read failed");
        require(value && type == PDC_UINT8 && bytes == sizeof(binary) && memcmp(value, binary, bytes) == 0,
                "binary tag payload changed");
        free(value);
    }
#ifdef ENABLE_MPI
    MPI_Allgather(local_ids, OBJECTS_PER_RANK, MPI_UINT64_T, all_ids, OBJECTS_PER_RANK, MPI_UINT64_T,
                  MPI_COMM_WORLD);
#else
    memcpy(all_ids, local_ids, sizeof(local_ids));
#endif
    int *     owners   = calloc(servers, sizeof(*owners));
    uint64_t *expected = malloc(size * OBJECTS_PER_RANK * sizeof(*expected));
    require(owners && expected, "query expectation allocation failed");
    int count = 0;
    for (int i = 0; i < size * OBJECTS_PER_RANK; i++) {
        uint32_t owner = PDC_get_server_by_obj_id(all_ids[i], servers);
        require(owner < (uint32_t)servers, "invalid metadata server mapping");
        owners[owner]++;
        if (i % 2 == 0)
            expected[count++] = all_ids[i];
    }
    for (int i = 0; i < servers; i++)
        require(owners[i] > 0, "test objects did not cover every DHT server");
    check_query(expected, count);
    char        missing_value[] = "\"missing\"";
    pdc_kvtag_t missing_query   = {
        .name = "someta_missing", .value = missing_value, .type = PDC_STRING, .size = sizeof(missing_value)};
    uint64_t *missing_ids   = NULL;
    int       missing_count = -1;
    require(PDC_Client_query_kvtag(&missing_query, &missing_count, &missing_ids) >= 0 && missing_count == 0,
            "empty query failed");
    free(missing_ids);
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    require(PDCobj_del_tag(objects[0], "someta_group") >= 0, "tag deletion failed");
    char replacement[] = "replacement";
    require(PDCobj_put_tag(objects[0], "someta_group", replacement, PDC_STRING, sizeof(replacement)) >= 0,
            "tag reinsertion failed");
    value = NULL;
    require(PDCobj_get_tag(objects[0], "someta_group", &value, &type, &bytes) >= 0,
            "replacement tag read failed");
    require(value && type == PDC_STRING && bytes == sizeof(replacement) &&
                memcmp(value, replacement, bytes) == 0,
            "replacement tag payload changed");
    free(value);
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    count = 0;
    for (int i = 0; i < size * OBJECTS_PER_RANK; i++)
        if (i % 2 == 0 && i % OBJECTS_PER_RANK != 0)
            expected[count++] = all_ids[i];
    check_query(expected, count);
    for (int i = 0; i < OBJECTS_PER_RANK; i++)
        require(PDCobj_close(objects[i]) >= 0, "object close failed");
    require(PDCcont_close(cont) >= 0, "container close failed");
    require(PDCprop_close(obj_prop) >= 0 && PDCprop_close(cont_prop) >= 0, "property close failed");
    require(PDCclose(pdc) >= 0, "PDCclose failed");
    free(expected);
    free(owners);
    free(all_ids);
    if (rank == 0)
        printf("SOMETA payload/query regression passed: %d servers, %d clients\n", servers, size);
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
