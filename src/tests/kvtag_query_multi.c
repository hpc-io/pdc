/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"

int
assign_work_to_rank(int rank, int size, int nwork, int *my_count, int *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        printf("assign_work_to_rank(): Error with input!\n");
        return -1;
    }
    if (nwork < size) {
        if (rank < nwork)
            *my_count = 1;
        else
            *my_count = 0;
        (*my_start) = rank * (*my_count);
    }
    else {
        (*my_count) = nwork / size;
        (*my_start) = rank * (*my_count);

        // Last few ranks may have extra work
        if (rank >= size - nwork % size) {
            (*my_count)++;
            (*my_start) += (rank - (size - nwork % size));
        }
    }

    return 1;
}

void
print_usage(char *name)
{
    printf("%s n_obj n_tag_per_obj selectivity n_condition n_round is_using_dart\n", name);
    printf("Summary: This test will create n_obj objects, and add n_tag_per_obj * 4 tags to each on \n"
           "it will perform n_round queries against the tags with specified selectivity, \n"
           "each query from each client should get the entire result set.\n");
    printf("Parameters:\n");
    printf("  n_obj: number of objects\n");
    printf("  n_tag_per_obj: number of tags per datatype (int, float, double, string) per object\n");
    printf("                 total tags = n_obj * n_tag_per_obj * 4\n");
    printf("  selectivity: selectivity, on a 100 scale, 0 to 25,\n");
    printf("               selectivity * n_tag_per_obj must also be an integer and dividible by 2.\n");
    printf("  n_condition: number of conditions in a query, currently only supports 4, 8, 16\n");
    printf("  n_round: number of rounds to perform the same query\n");
    printf("  is_using_dart: 1 for using dart, 0 for not using dart\n");
}

int
convert_str_to_prefix(char *str, int cut_digit)
{
    // Convert to a prefix string query by cutting off the string after the leading digit and add * after
    int str_len = strlen(str);
    int i       = str_len - 10;
    while (i < str_len) {
        if (str[i] != '0') {
            str[i + cut_digit]     = '*';
            str[i + cut_digit + 1] = '\0';
            break;
        }
        i++;
    }
    return 1;
}

int
main(int argc, char *argv[])
{
    pdcid_t     pdc, cont_prop, cont, obj_prop;
    pdcid_t *   obj_ids;
    int         n_obj, my_obj, my_obj_s;
    int         proc_num = 1, my_rank = 0, ntag_per_obj, n_condition, round, is_using_dart;
    int         i, j, int_val, tag_total_per_dtype, nsel;
    char        obj_name[1024], tag_name[1024], query[1024], value[1024];
    char        tmp_str1[1024], tmp_str2[1024], tmp_str3[1024], tmp_str4[1024];
    char *      char_ptr;
    double      stime, total_time, double_val, selectivity;
    float       float_val;
    pdc_kvtag_t kvtag;
    pdc_kvtag_t query_kvtag;
    uint64_t *  pdc_ids;
    int         nres, ntotal;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif

    if (argc < 5) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj         = atoi(argv[1]);
    ntag_per_obj  = atoi(argv[2]);
    selectivity   = atof(argv[3]);
    n_condition   = atoi(argv[4]);
    round         = atoi(argv[5]);
    is_using_dart = atoi(argv[6]);

    tag_total_per_dtype = n_obj * ntag_per_obj;
    nsel                = (int)tag_total_per_dtype * selectivity / 100.0;

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    // Distribute obj create operations evenly
    assign_work_to_rank(my_rank, proc_num, n_obj, &my_obj, &my_obj_s);
    if (my_rank == 0)
        printf("I will create %d obj\n", my_obj);

    // Create objects
    obj_ids = (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));
    for (i = 0; i < my_obj; i++) {
        sprintf(obj_name, "obj%d", my_obj_s + i);
        obj_ids[i] = PDCobj_create(cont, obj_name, obj_prop);
        if (obj_ids[i] <= 0)
            printf("Fail to create object @ line  %d!\n", __LINE__);
    }

    if (my_rank == 0)
        printf("Created %d objects\n", n_obj);
    fflush(stdout);

    dart_object_ref_type_t ref_type  = REF_PRIMARY_ID;
    dart_hash_algo_t       hash_algo = DART_HASH;

    // Add tag to objects just created
    for (i = 0; i < my_obj; i++) {
        for (j = 0; j < ntag_per_obj; j++) {

            // int
            sprintf(tag_name, "int_val", i);
            int_val     = i;
            kvtag.name  = tag_name;
            kvtag.value = (void *)&int_val;
            kvtag.type  = PDC_INT;
            kvtag.size  = sizeof(int);

            if (is_using_dart) {
                sprintf(value, "%d", int_val);
                if (PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, value, ref_type,
                                                        (uint64_t)obj_ids[i]) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }
            else {
                if (ntag_per_obj < 10)
                    println("Rank %d: obj %llu [%s] [%d], len %d\n", my_rank, obj_ids[i], kvtag.name,
                            *((int *)kvtag.value), kvtag.size);
                if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }

            // float
            sprintf(tag_name, "float_val");
            float_val   = (float)i;
            kvtag.name  = tag_name;
            kvtag.value = (void *)&float_val;
            kvtag.type  = PDC_FLOAT;
            kvtag.size  = sizeof(float);

            if (is_using_dart) {
                sprintf(value, "%f", float_val);
                if (PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, value, ref_type,
                                                        (uint64_t)obj_ids[i]) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }
            else {
                if (ntag_per_obj < 10)
                    println("Rank %d: obj %llu [%s] [%f], len %d\n", my_rank, obj_ids[i], kvtag.name,
                            *((float *)kvtag.value), kvtag.size);
                if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }

            // double
            sprintf(tag_name, "double_val", i);
            double_val  = (double)i;
            kvtag.name  = tag_name;
            kvtag.value = (void *)&double_val;
            kvtag.type  = PDC_DOUBLE;
            kvtag.size  = sizeof(double);

            if (is_using_dart) {
                sprintf(value, "%lf", double_val);
                if (PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, value, ref_type,
                                                        (uint64_t)obj_ids[i]) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }
            else {
                if (ntag_per_obj < 10)
                    println("Rank %d: obj %llu [%s] [%lf], len %d\n", my_rank, obj_ids[i], kvtag.name,
                            *((double *)kvtag.value), kvtag.size);
                if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }

            // string
            sprintf(tag_name, "string_val", i);
            sprintf(tmp_str1, "%010d", i);
            kvtag.name  = tag_name;
            kvtag.value = (void *)tmp_str1;
            kvtag.type  = PDC_STRING;
            kvtag.size  = strlen(tmp_str1) + 1;

            if (is_using_dart) {
                sprintf(value, "%s", tmp_str1);
                if (PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, value, ref_type,
                                                        (uint64_t)obj_ids[i]) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }
            else {
                if (ntag_per_obj < 10)
                    println("Rank %d: obj %llu [%s] [%s], len %d\n", my_rank, obj_ids[i], kvtag.name,
                            (char *)kvtag.value, kvtag.size);
                if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0) {
                    printf("fail to add a kvtag to obj %d\n", i + my_obj_s);
                }
            }
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (n_condition == 4) {
        sprintf(tmp_str1, "string_val=string(%010d)", nsel);
        convert_str_to_prefix(tmp_str1, 0);

        // Use double_val as the real selectivity, select more with int, float, and string
        sprintf(query, "int_val<=int(%d) AND float_val<float(%f) AND double_val<double(%lf) AND %s)",
                nsel * 4, (float)nsel * 2, (double)nsel, tmp_str1);
    }
    else if (n_condition == 8) {

        sprintf(tmp_str1, "string_val=string(%010d)", nsel);
        convert_str_to_prefix(tmp_str1, -1);

        sprintf(tmp_str2, "string_val=string(%010d)", nsel);
        convert_str_to_prefix(tmp_str2, 0);

        // Use double_val as the real selectivity, select more with int, float, and string
        sprintf(query,
                "int_val>int(0) AND int_val<=int(%d) AND float_val>=float(0.0) AND float_val<float(%f) "
                "AND double_val>double(0.0) AND double_val<double(%lf) AND %s) AND %s)",
                nsel * 4, (float)nsel * 2, (double)nsel, tmp_str1, tmp_str2);
    }
    else if (n_condition == 16) {
        sprintf(tmp_str1, "string_val=string(%010d)", nsel);
        convert_str_to_prefix(tmp_str1, 0);

        sprintf(tmp_str2, "string_val=string(%010d)", nsel);
        convert_str_to_prefix(tmp_str2, -1);

        sprintf(tmp_str3, "string_val=string(%010d)", nsel);
        convert_str_to_prefix(tmp_str3, -2);

        sprintf(tmp_str4, "string_val=string(%010d)", nsel);
        convert_str_to_prefix(tmp_str4, -3);

        // Use double_val as the real selectivity, select more with int, float, and string
        sprintf(query,
                "int_val>int(0) AND int_val<=int(%d) AND "
                "int_val>int(%d) AND int_val<=int(%d) AND "
                "float_val>=float(0.0) AND float_val<float(%f) AND "
                "float_val>=float(%f) AND float_val<float(%f) "
                "AND double_val>double(0.0) AND double_val<double(%lf) AND "
                "AND double_val>double(0.0) AND double_val<double(%lf) AND "
                "%s) AND %s) AND %s) AND %s)",
                nsel * 2, tag_total_per_dtype - nsel * 2, tag_total_per_dtype, (float)nsel,
                (float)tag_total_per_dtype - nsel, (float)tag_total_per_dtype, (double)nsel / 2,
                (double)tag_total_per_dtype - nsel / 2, (double)tag_total_per_dtype, tmp_str1, tmp_str2,
                tmp_str3, tmp_str4);
    }

    query_kvtag.name  = tag_name;
    query_kvtag.value = (void *)query;
    query_kvtag.type  = PDC_STRING;
    query_kvtag.size  = strlen(query) + 1;
    if (my_rank == 0)
        println("Rank %d: query kvtag [%s] [%s]\n", my_rank, query_kvtag.name, (char *)query_kvtag.value);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
    if (PDC_Client_query_kvtag_mpi(&query_kvtag, &nres, &pdc_ids, MPI_COMM_WORLD) < 0) {
#else
    if (PDC_Client_query_kvtag(&query_kvtag, &nres, &pdc_ids) < 0) {
#endif
        printf("fail to query kvtag [%s] with rank %d\n", query_kvtag.name, my_rank);
    }
    println("Query found %d objects", nres);
    for (i = 0; i < nres; i++) {
        printf("%llu\n", pdc_ids[i]);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    /* MPI_Reduce(&nres, &ntotal, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD); */
    total_time = MPI_Wtime() - stime;

    if (my_rank == 0) {
        if (nres != nsel)
            println("Query result %d does not match expected %d", nres, nsel);
        else
            println("Total time to query %d objects with tag: %.5f", nres, total_time);
    }
#endif

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");
    else
        printf("successfully close container c1\n");

    // close an object property
    if (PDCprop_close(obj_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close object property\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close container property\n");

    // close pdc
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");
done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
