#include "pdc_server.h"

/*
 * Main function of PDC server
 *
 * \param  argc[IN]     Number of command line arguments
 * \param  argv[IN]     Command line arguments
 *
 * \return Non-negative on success/Negative on failure
 */
int
main(int argc, char *argv[])
{
    int    port;
    perr_t ret;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_server_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_server_size_g);
#else
    pdc_server_rank_g = 0;
    pdc_server_size_g = 1;
#endif

#ifdef PDC_TIMING
    struct timeval start_time;
    struct timeval end_time;
    double         server_init_time;
    gettimeofday(&start_time, 0);

#ifdef ENABLE_MPI
    double start = MPI_Wtime();
    PDC_server_timing_init();
#endif
#endif
    if (argc > 1)
        if (strcmp(argv[1], "restart") == 0)
            is_restart_g = 1;

    // Init rand seed
    srand(time(NULL));

    // Get environmental variables
    PDC_Server_get_env();

    port = pdc_server_rank_g % 32 + 7000;
    ret  = PDC_Server_init(port, &hg_class_g, &hg_context_g);
    if (ret != SUCCEED || hg_class_g == NULL || hg_context_g == NULL) {
        printf("==PDC_SERVER[%d]: Error with Mercury init\n", pdc_server_rank_g);
        goto done;
    }
    // Register Mercury RPC/bulk
    PDC_Server_mercury_register();

#ifdef ENABLE_MPI
    // Need a barrier so that all servers finish init before the lookup process
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Lookup and get addresses of other servers
    char *lookup_on_demand = getenv("PDC_LOOKUP_ON_DEMAND");
    if (lookup_on_demand != NULL) {
        if (pdc_server_rank_g == 0)
            printf("==PDC_SERVER[0]: will lookup other PDC servers on demand\n");
    }
    else
        PDC_Server_lookup_all_servers();

    // Write server addrs to the config file for client to read from
    if (pdc_server_rank_g == 0)
        if (PDC_Server_write_addr_to_file(all_addr_strings_g, pdc_server_size_g) != SUCCEED)
            printf("==PDC_SERVER[%d]: Error with write config file\n", pdc_server_rank_g);
#ifdef PDC_TIMING
#ifdef ENABLE_MPI
    pdc_server_timings->PDCserver_start_total += MPI_Wtime() - start;
#endif

    gettimeofday(&end_time, 0);
    server_init_time = PDC_get_elapsed_time_double(&start_time, &end_time);
#endif

    if (pdc_server_rank_g == 0) {
#ifdef PDC_TIMING
        printf("==PDC_SERVER[%d]: total startup time = %.6f\n", pdc_server_rank_g, server_init_time);
#endif
#ifdef ENABLE_MPI
        printf("==PDC_SERVER[%d]: Server ready!\n\n\n", pdc_server_rank_g);
#else
        printf("==PDC_SERVER[%d]: Server ready (no MPI)!\n\n\n", pdc_server_rank_g);
#endif
    }
    fflush(stdout);

    // Main loop to handle Mercury RPC/Bulk requests
#ifdef ENABLE_MULTITHREAD
    PDC_Server_multithread_loop(hg_context_g);
#else
    PDC_Server_loop(hg_context_g);
#endif

#ifdef ENABLE_TIMING
    PDC_print_IO_stats();
#endif

done:
#ifdef PDC_TIMING
    PDC_server_timing_report();
#endif
    PDC_Server_finalize();
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}