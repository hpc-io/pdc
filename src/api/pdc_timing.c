#include "pdc_timing.h"

#ifdef PDC_TIMING
static double pdc_base_time;

static int
pdc_timestamp_clean(pdc_timestamp *timestamp)
{
    if (timestamp->timestamp_size) {
        free(timestamp->start);
    }
    return 0;
}

static int
timestamp_log(FILE *stream, const char *header, pdc_timestamp *timestamp)
{
    size_t i;
    double total = 0.0;
    fprintf(stream, "%s", header);
    for (i = 0; i < timestamp->timestamp_size; ++i) {
        fprintf(stream, ",%4f-%4f", timestamp->start[i], timestamp->end[i]);
        total += timestamp->end[i] - timestamp->start[i];
    }
    fprintf(stream, "\n");

    if (i > 0)
        fprintf(stream, "%s_total, %f\n", header, total);

    return 0;
}

int
PDC_timing_init()
{
    char           hostname[HOST_NAME_MAX];
    int            rank;
    pdc_timestamp *ptr;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    gethostname(hostname, HOST_NAME_MAX);
    if (!(rank % 32)) {
        printf("client process rank %d, hostname = %s\n", rank, hostname);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    memset(&pdc_timings, 0, sizeof(pdc_timing));

    pdc_client_buf_obj_map_timestamps   = calloc(16, sizeof(pdc_timestamp));
    ptr                                 = pdc_client_buf_obj_map_timestamps + 1;
    pdc_client_buf_obj_unmap_timestamps = ptr;
    ptr++;
    pdc_client_obtain_lock_write_timestamps = ptr;
    ptr++;
    pdc_client_obtain_lock_read_timestamps = ptr;
    ptr++;
    pdc_client_release_lock_write_timestamps = ptr;
    ptr++;
    pdc_client_release_lock_read_timestamps = ptr;
    ptr++;

    pdc_client_transfer_request_start_write_timestamps = ptr;
    ptr++;
    pdc_client_transfer_request_start_read_timestamps = ptr;
    ptr++;
    pdc_client_transfer_request_wait_write_timestamps = ptr;
    ptr++;
    pdc_client_transfer_request_wait_read_timestamps = ptr;
    ptr++;

    pdc_client_transfer_request_start_all_write_timestamps = ptr;
    ptr++;
    pdc_client_transfer_request_start_all_read_timestamps = ptr;
    ptr++;
    pdc_client_transfer_request_wait_all_timestamps = ptr;
    ptr++;

    pdc_client_create_cont_timestamps = ptr;
    ptr++;
    pdc_client_create_obj_timestamps = ptr;

    ptr++;
    pdc_client_transfer_request_metadata_query_timestamps = ptr;

    return 0;
}

int
PDC_timing_finalize()
{
    pdc_timestamp_clean(pdc_client_buf_obj_map_timestamps);
    pdc_timestamp_clean(pdc_client_buf_obj_unmap_timestamps);

    pdc_timestamp_clean(pdc_client_obtain_lock_write_timestamps);
    pdc_timestamp_clean(pdc_client_obtain_lock_read_timestamps);
    pdc_timestamp_clean(pdc_client_release_lock_write_timestamps);
    pdc_timestamp_clean(pdc_client_release_lock_read_timestamps);

    pdc_timestamp_clean(pdc_client_transfer_request_start_write_timestamps);
    pdc_timestamp_clean(pdc_client_transfer_request_start_read_timestamps);
    pdc_timestamp_clean(pdc_client_transfer_request_wait_write_timestamps);
    pdc_timestamp_clean(pdc_client_transfer_request_wait_read_timestamps);
    pdc_timestamp_clean(pdc_client_create_cont_timestamps);
    pdc_timestamp_clean(pdc_client_create_obj_timestamps);
    pdc_timestamp_clean(pdc_client_transfer_request_start_all_write_timestamps);
    pdc_timestamp_clean(pdc_client_transfer_request_start_all_read_timestamps);
    pdc_timestamp_clean(pdc_client_transfer_request_wait_all_timestamps);
    pdc_timestamp_clean(pdc_client_transfer_request_metadata_query_timestamps);

    free(pdc_client_buf_obj_map_timestamps);
    return 0;
}

int
PDC_timing_report(const char *prefix)
{
    pdc_timing max_timings;
    int        rank;
    char       filename[256], header[256];
    FILE *     stream;
    char       hostname[HOST_NAME_MAX];
    time_t     now;

    time(&now);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    gethostname(hostname, HOST_NAME_MAX);
    if (!(rank % 32)) {
        printf("client process rank %d, hostname = %s\n", rank, hostname);
    }
    MPI_Reduce(&pdc_timings, &max_timings, sizeof(pdc_timing) / sizeof(double), MPI_DOUBLE, MPI_MAX, 0,
               MPI_COMM_WORLD);
    if (rank == 0) {
        printf("PDCbuf_obj_map_rpc = %lf, wait = %lf\n", max_timings.PDCbuf_obj_map_rpc,
               max_timings.PDCbuf_obj_map_rpc_wait);
        printf("PDCreg_obtain_lock_write_rpc = %lf, wait = %lf\n", max_timings.PDCreg_obtain_lock_write_rpc,
               max_timings.PDCreg_obtain_lock_write_rpc_wait);
        printf("PDCreg_obtain_lock_read_rpc = %lf, wait = %lf\n", max_timings.PDCreg_obtain_lock_read_rpc,
               max_timings.PDCreg_obtain_lock_read_rpc_wait);

        printf("PDCreg_release_lock_write_rpc = %lf, wait = %lf\n", max_timings.PDCreg_release_lock_write_rpc,
               max_timings.PDCreg_release_lock_write_rpc_wait);
        printf("PDCreg_release_lock_read_rpc = %lf, wait = %lf\n", max_timings.PDCreg_release_lock_read_rpc,
               max_timings.PDCreg_release_lock_read_rpc_wait);
        printf("PDCbuf_obj_unmap_rpc = %lf, wait = %lf\n", max_timings.PDCbuf_obj_unmap_rpc,
               max_timings.PDCbuf_obj_unmap_rpc_wait);

        printf("PDCtransfer_request_start_write = %lf, wait = %lf\n",
               max_timings.PDCtransfer_request_start_write_rpc,
               max_timings.PDCtransfer_request_start_write_rpc_wait);
        printf("PDCtransfer_request_start_read = %lf, wait = %lf\n",
               max_timings.PDCtransfer_request_start_read_rpc,
               max_timings.PDCtransfer_request_start_read_rpc_wait);
        printf("PDCtransfer_request_wait_write = %lf, wait = %lf\n",
               max_timings.PDCtransfer_request_wait_write_rpc,
               max_timings.PDCtransfer_request_wait_write_rpc_wait);
        printf("PDCtransfer_request_wait_read = %lf, wait = %lf\n",
               max_timings.PDCtransfer_request_wait_read_rpc,
               max_timings.PDCtransfer_request_wait_read_rpc_wait);
        printf("PDCtransfer_request_start_all_write = %lf, wait = %lf\n",
               max_timings.PDCtransfer_request_start_all_write_rpc,
               max_timings.PDCtransfer_request_start_all_write_rpc_wait);
        printf("PDCtransfer_request_start_all_read = %lf, wait = %lf\n",
               max_timings.PDCtransfer_request_start_all_read_rpc,
               max_timings.PDCtransfer_request_start_all_read_rpc_wait);
        printf("PDCtransfer_request_wait_write = %lf, wait = %lf\n",
               max_timings.PDCtransfer_request_wait_all_rpc,
               max_timings.PDCtransfer_request_wait_all_rpc_wait);
    }

    sprintf(filename, "pdc_client_log_rank_%d.csv", rank);
    stream = fopen(filename, "r");
    if (stream) {
        fclose(stream);
        stream = fopen(filename, "a");
    }
    else {
        stream = fopen(filename, "w");
    }

    fprintf(stream, "%s", ctime(&now));

    sprintf(header, "buf_obj_map_%s", prefix);
    timestamp_log(stream, header, pdc_client_buf_obj_map_timestamps);
    sprintf(header, "buf_obj_unmap_%s", prefix);
    timestamp_log(stream, header, pdc_client_buf_obj_unmap_timestamps);

    sprintf(header, "obtain_lock_write_%s", prefix);
    timestamp_log(stream, header, pdc_client_obtain_lock_write_timestamps);
    sprintf(header, "obtain_lock_read_%s", prefix);
    timestamp_log(stream, header, pdc_client_obtain_lock_read_timestamps);

    sprintf(header, "release_lock_write_%s", prefix);
    timestamp_log(stream, header, pdc_client_release_lock_write_timestamps);
    sprintf(header, "release_lock_read_%s", prefix);
    timestamp_log(stream, header, pdc_client_release_lock_read_timestamps);

    sprintf(header, "transfer_request_start_write_%s", prefix);
    timestamp_log(stream, header, pdc_client_transfer_request_start_write_timestamps);

    sprintf(header, "transfer_request_start_read_%s", prefix);
    timestamp_log(stream, header, pdc_client_transfer_request_start_read_timestamps);

    sprintf(header, "transfer_request_wait_write_%s", prefix);
    timestamp_log(stream, header, pdc_client_transfer_request_wait_write_timestamps);

    sprintf(header, "transfer_request_wait_read_%s", prefix);
    timestamp_log(stream, header, pdc_client_transfer_request_wait_read_timestamps);

    sprintf(header, "transfer_request_start_all_write_%s", prefix);
    timestamp_log(stream, header, pdc_client_transfer_request_start_all_write_timestamps);

    sprintf(header, "transfer_request_start_all_read_%s", prefix);
    timestamp_log(stream, header, pdc_client_transfer_request_start_all_read_timestamps);

    sprintf(header, "transfer_request_wait_all_%s", prefix);
    timestamp_log(stream, header, pdc_client_transfer_request_wait_all_timestamps);

    sprintf(header, "create_cont");
    timestamp_log(stream, header, pdc_client_create_cont_timestamps);

    sprintf(header, "create_obj");
    timestamp_log(stream, header, pdc_client_create_obj_timestamps);

    fprintf(stream, "\n");
    fclose(stream);

    pdc_client_buf_obj_map_timestamps->timestamp_size   = 0;
    pdc_client_buf_obj_unmap_timestamps->timestamp_size = 0;

    pdc_client_obtain_lock_write_timestamps->timestamp_size  = 0;
    pdc_client_obtain_lock_read_timestamps->timestamp_size   = 0;
    pdc_client_release_lock_write_timestamps->timestamp_size = 0;
    pdc_client_release_lock_read_timestamps->timestamp_size  = 0;

    pdc_client_transfer_request_start_write_timestamps->timestamp_size = 0;
    pdc_client_transfer_request_start_read_timestamps->timestamp_size  = 0;
    pdc_client_transfer_request_wait_write_timestamps->timestamp_size  = 0;
    pdc_client_transfer_request_wait_read_timestamps->timestamp_size   = 0;

    pdc_client_transfer_request_start_all_write_timestamps->timestamp_size = 0;
    pdc_client_transfer_request_start_all_read_timestamps->timestamp_size  = 0;
    pdc_client_transfer_request_wait_all_timestamps->timestamp_size        = 0;

    pdc_client_create_cont_timestamps->timestamp_size = 0;
    pdc_client_create_obj_timestamps->timestamp_size  = 0;

    pdc_client_transfer_request_metadata_query_timestamps->timestamp_size = 0;

    memset(&pdc_timings, 0, sizeof(pdc_timings));

    return 0;
}

int
PDC_server_timing_init()
{
    char hostname[HOST_NAME_MAX];
    int  rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    gethostname(hostname, HOST_NAME_MAX);

    printf("server process rank %d, hostname = %s\n", rank, hostname);
    /*
        printf("rank = %d, hostname = %s, PDCbuf_obj_map_rpc = %lf, PDCreg_obtain_lock_rpc = %lf, "
               "PDCreg_release_lock_write_rpc = "
               "%lf, PDCreg_release_lock_read_rpc = %lf, PDCbuf_obj_unmap_rpc = %lf, "
               "region_release_bulk_transfer_cb = %lf\n",
               rank, hostname, server_timings->PDCbuf_obj_map_rpc, server_timings->PDCreg_obtain_lock_rpc,
               server_timings->PDCreg_release_lock_write_rpc, server_timings->PDCreg_release_lock_read_rpc,
               server_timings->PDCbuf_obj_unmap_rpc, server_timings->PDCreg_release_lock_bulk_transfer_rpc);
    */
    MPI_Barrier(MPI_COMM_WORLD);

    pdc_server_timings         = calloc(1, sizeof(pdc_server_timing));
    pdc_timestamp *ptr         = calloc(25, sizeof(pdc_timestamp));
    pdc_buf_obj_map_timestamps = ptr;
    ptr++;
    pdc_buf_obj_unmap_timestamps = ptr;
    ptr++;
    pdc_obtain_lock_write_timestamps = ptr;
    ptr++;
    pdc_obtain_lock_read_timestamps = ptr;
    ptr++;
    pdc_release_lock_write_timestamps = ptr;
    ptr++;
    pdc_release_lock_read_timestamps = ptr;
    ptr++;
    pdc_release_lock_bulk_transfer_write_timestamps = ptr;
    ptr++;
    pdc_release_lock_bulk_transfer_read_timestamps = ptr;
    ptr++;
    pdc_release_lock_bulk_transfer_inner_write_timestamps = ptr;
    ptr++;
    pdc_release_lock_bulk_transfer_inner_read_timestamps = ptr;
    ptr++;

    pdc_transfer_request_start_write_timestamps = ptr;
    ptr++;
    pdc_transfer_request_start_read_timestamps = ptr;
    ptr++;
    pdc_transfer_request_wait_write_timestamps = ptr;
    ptr++;
    pdc_transfer_request_wait_read_timestamps = ptr;
    ptr++;
    pdc_transfer_request_start_write_bulk_timestamps = ptr;
    ptr++;
    pdc_transfer_request_start_read_bulk_timestamps = ptr;
    ptr++;
    pdc_transfer_request_inner_write_bulk_timestamps = ptr;
    ptr++;
    pdc_transfer_request_inner_read_bulk_timestamps = ptr;
    ptr++;

    pdc_transfer_request_start_all_write_timestamps = ptr;
    ptr++;
    pdc_transfer_request_start_all_read_timestamps = ptr;
    ptr++;
    pdc_transfer_request_wait_all_timestamps = ptr;
    ptr++;
    pdc_transfer_request_start_all_write_bulk_timestamps = ptr;
    ptr++;
    pdc_transfer_request_start_all_read_bulk_timestamps = ptr;
    ptr++;
    pdc_transfer_request_inner_write_all_bulk_timestamps = ptr;
    ptr++;
    pdc_transfer_request_inner_read_all_bulk_timestamps = ptr;
    ptr++;

    // 25 timestamps

    pdc_base_time = MPI_Wtime();
    return 0;
}

int
pdc_timestamp_register(pdc_timestamp *timestamp, double start, double end)
{
    double *temp;

    if (timestamp->timestamp_max_size == 0) {
        timestamp->timestamp_max_size = 256;
        timestamp->start              = (double *)malloc(sizeof(double) * timestamp->timestamp_max_size * 2);
        timestamp->end                = timestamp->start + timestamp->timestamp_max_size;
        timestamp->timestamp_size     = 0;
    }
    else if (timestamp->timestamp_size == timestamp->timestamp_max_size) {
        temp = (double *)malloc(sizeof(double) * timestamp->timestamp_max_size * 4);
        memcpy(temp, timestamp->start, sizeof(double) * timestamp->timestamp_max_size);
        memcpy(temp + timestamp->timestamp_max_size * 2, timestamp->end,
               sizeof(double) * timestamp->timestamp_max_size);
        timestamp->start = temp;
        timestamp->end   = temp + timestamp->timestamp_max_size * 2;
        timestamp->timestamp_max_size *= 2;
    }
    timestamp->start[timestamp->timestamp_size] = start;
    timestamp->end[timestamp->timestamp_size]   = end;
    timestamp->timestamp_size++;
    return 0;
}

int
PDC_server_timing_report()
{
    pdc_server_timing max_timings;
    int               rank;
    char              filename[256];
    FILE *            stream;

    //    char              hostname[HOST_NAME_MAX];
    time_t now;

    time(&now);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Reduce(pdc_server_timings, &max_timings, sizeof(pdc_server_timing) / sizeof(double), MPI_DOUBLE,
               MPI_MAX, 0, MPI_COMM_WORLD);
    sprintf(filename, "pdc_server_log_rank_%d.csv", rank);

    stream = fopen(filename, "w");

    fprintf(stream, "%s", ctime(&now));
    timestamp_log(stream, "buf_obj_map", pdc_buf_obj_map_timestamps);
    timestamp_log(stream, "buf_obj_unmap", pdc_buf_obj_unmap_timestamps);

    timestamp_log(stream, "obtain_lock_write", pdc_obtain_lock_write_timestamps);
    timestamp_log(stream, "obtain_lock_read", pdc_obtain_lock_read_timestamps);
    timestamp_log(stream, "release_lock_write", pdc_release_lock_write_timestamps);
    timestamp_log(stream, "release_lock_read", pdc_release_lock_read_timestamps);
    timestamp_log(stream, "release_lock_bulk_transfer_write",
                  pdc_release_lock_bulk_transfer_write_timestamps);
    timestamp_log(stream, "release_lock_bulk_transfer_read", pdc_release_lock_bulk_transfer_read_timestamps);
    timestamp_log(stream, "release_lock_bulk_transfer_inner_write",
                  pdc_release_lock_bulk_transfer_inner_write_timestamps);
    timestamp_log(stream, "release_lock_bulk_transfer_inner_read",
                  pdc_release_lock_bulk_transfer_inner_read_timestamps);

    timestamp_log(stream, "transfer_request_start_write", pdc_transfer_request_start_write_timestamps);
    timestamp_log(stream, "transfer_request_wait_write", pdc_transfer_request_wait_write_timestamps);
    timestamp_log(stream, "transfer_request_start_write_bulk",
                  pdc_transfer_request_start_write_bulk_timestamps);
    timestamp_log(stream, "transfer_request_inner_write_bulk",
                  pdc_transfer_request_inner_write_bulk_timestamps);
    timestamp_log(stream, "transfer_request_start_read", pdc_transfer_request_start_read_timestamps);
    timestamp_log(stream, "transfer_request_wait_read", pdc_transfer_request_wait_read_timestamps);
    timestamp_log(stream, "transfer_request_start_read_bulk",
                  pdc_transfer_request_start_read_bulk_timestamps);
    timestamp_log(stream, "transfer_request_inner_read_bulk",
                  pdc_transfer_request_inner_read_bulk_timestamps);

    timestamp_log(stream, "transfer_request_start_all_write",
                  pdc_transfer_request_start_all_write_timestamps);
    timestamp_log(stream, "transfer_request_start_all_write_bulk",
                  pdc_transfer_request_start_all_write_bulk_timestamps);
    timestamp_log(stream, "transfer_request_start_all_read", pdc_transfer_request_start_all_read_timestamps);
    timestamp_log(stream, "transfer_request_start_all_read_bulk",
                  pdc_transfer_request_start_all_read_bulk_timestamps);
    timestamp_log(stream, "transfer_request_inner_write_all_bulk",
                  pdc_transfer_request_inner_write_all_bulk_timestamps);
    timestamp_log(stream, "transfer_request_inner_read_all_bulk",
                  pdc_transfer_request_inner_read_all_bulk_timestamps);
    timestamp_log(stream, "transfer_request_wait_all", pdc_transfer_request_wait_all_timestamps);

    /* timestamp_log(stream, "create_obj", create_obj_timestamps); */
    /* timestamp_log(stream, "create_cont", create_cont_timestamps); */
    fclose(stream);

    sprintf(filename, "pdc_server_timings_%d.csv", rank);
    stream = fopen(filename, "w");
    fprintf(stream, "%s", ctime(&now));
    fprintf(stream, "PDCbuf_obj_map_rpc, %lf\n", pdc_server_timings->PDCbuf_obj_map_rpc);
    fprintf(stream, "PDCreg_obtain_lock_write_rpc, %lf\n", pdc_server_timings->PDCreg_obtain_lock_write_rpc);
    fprintf(stream, "PDCreg_obtain_lock_read_rpc, %lf\n", pdc_server_timings->PDCreg_obtain_lock_read_rpc);
    fprintf(stream, "PDCreg_release_lock_write_rpc, %lf\n",
            pdc_server_timings->PDCreg_release_lock_write_rpc);
    fprintf(stream, "PDCreg_release_lock_read_rpc, %lf\n", pdc_server_timings->PDCreg_release_lock_read_rpc);
    fprintf(stream, "PDCbuf_obj_unmap_rpc, %lf\n", pdc_server_timings->PDCbuf_obj_unmap_rpc);
    fprintf(stream, "PDCreg_release_lock_bulk_transfer_write_rpc, %lf\n",
            pdc_server_timings->PDCreg_release_lock_bulk_transfer_write_rpc);
    fprintf(stream, "PDCreg_release_lock_bulk_transfer_read_rpc, %lf\n",
            pdc_server_timings->PDCreg_release_lock_bulk_transfer_read_rpc);
    fprintf(stream, "PDCreg_release_lock_bulk_transfer_inner_write_rpc, %lf\n",
            pdc_server_timings->PDCreg_release_lock_bulk_transfer_inner_write_rpc);
    fprintf(stream, "PDCreg_release_lock_bulk_transfer_inner_read_rpc, %lf\n",
            pdc_server_timings->PDCreg_release_lock_bulk_transfer_inner_read_rpc);
    fprintf(stream, "PDCregion_transfer_start_write_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_start_write_rpc);
    fprintf(stream, "PDCregion_transfer_wait_write_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_wait_write_rpc);
    fprintf(stream, "PDCregion_transfer_start_write_bulk_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_start_write_bulk_rpc);
    fprintf(stream, "PDCregion_transfer_request_inner_write_bulk_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_inner_write_bulk_rpc);
    fprintf(stream, "PDCregion_transfer_start_read_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_start_read_rpc);
    fprintf(stream, "PDCregion_transfer_wait_read_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_wait_read_rpc);
    fprintf(stream, "PDCregion_transfer_start_read_bulk_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_start_read_bulk_rpc);
    fprintf(stream, "PDCregion_transfer_request_inner_read_bulk_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_inner_read_bulk_rpc);

    fprintf(stream, "PDCregion_transfer_start_write_all_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_start_all_write_rpc);
    fprintf(stream, "PDCregion_transfer_request_inner_write_all_bulk_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_inner_write_all_bulk_rpc);
    fprintf(stream, "PDCregion_transfer_start_all_read_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_start_all_read_rpc);
    fprintf(stream, "PDCregion_transfer_request_inner_read_all_bulk_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_inner_read_all_bulk_rpc);
    fprintf(stream, "PDCregion_transfer_wait_all_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_wait_all_rpc);
    fprintf(stream, "PDCregion_transfer_wait_all_bulk_rpc, %lf\n",
            pdc_server_timings->PDCreg_transfer_request_wait_all_bulk_rpc);

    fprintf(stream, "PDCserver_obj_create_rpc, %lf\n", pdc_server_timings->PDCserver_obj_create_rpc);
    fprintf(stream, "PDCserver_cont_create_rpc, %lf\n", pdc_server_timings->PDCserver_cont_create_rpc);

    fprintf(stream, "PDCdata_server_write_out, %lf\n", pdc_server_timings->PDCdata_server_write_out);
    fprintf(stream, "PDCdata_server_read_from, %lf\n", pdc_server_timings->PDCdata_server_read_from);
    fprintf(stream, "PDCcache_write, %lf\n", pdc_server_timings->PDCcache_write);
    fprintf(stream, "PDCcache_read, %lf\n", pdc_server_timings->PDCcache_read);
    fprintf(stream, "PDCcache_flush, %lf\n", pdc_server_timings->PDCcache_flush);
    fprintf(stream, "PDCcache_clean, %lf\n", pdc_server_timings->PDCcache_clean);
    fprintf(stream, "PDCdata_server_write_posix, %lf\n", pdc_server_timings->PDCdata_server_write_posix);
    fprintf(stream, "PDCdata_server_read_posix, %lf\n", pdc_server_timings->PDCdata_server_read_posix);

    fprintf(stream, "PDCserver_restart, %lf\n", pdc_server_timings->PDCserver_restart);
    fprintf(stream, "PDCserver_checkpoint, %lf\n", pdc_server_timings->PDCserver_checkpoint);
    fprintf(stream, "PDCstart_server_total, %lf\n", pdc_server_timings->PDCserver_start_total);

    fclose(stream);

    free(pdc_server_timings);
    pdc_timestamp_clean(pdc_buf_obj_map_timestamps);
    pdc_timestamp_clean(pdc_buf_obj_unmap_timestamps);

    pdc_timestamp_clean(pdc_obtain_lock_write_timestamps);
    pdc_timestamp_clean(pdc_obtain_lock_read_timestamps);
    pdc_timestamp_clean(pdc_release_lock_write_timestamps);
    pdc_timestamp_clean(pdc_release_lock_read_timestamps);
    pdc_timestamp_clean(pdc_release_lock_bulk_transfer_write_timestamps);
    pdc_timestamp_clean(pdc_release_lock_bulk_transfer_read_timestamps);
    pdc_timestamp_clean(pdc_release_lock_bulk_transfer_inner_write_timestamps);
    pdc_timestamp_clean(pdc_release_lock_bulk_transfer_inner_read_timestamps);

    pdc_timestamp_clean(pdc_transfer_request_start_write_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_start_read_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_wait_write_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_wait_read_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_start_write_bulk_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_start_read_bulk_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_inner_write_bulk_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_inner_read_bulk_timestamps);

    pdc_timestamp_clean(pdc_transfer_request_start_all_write_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_start_all_read_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_start_all_write_bulk_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_start_all_read_bulk_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_wait_all_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_inner_write_all_bulk_timestamps);
    pdc_timestamp_clean(pdc_transfer_request_inner_read_all_bulk_timestamps);

    /* pdc_timestamp_clean(pdc_create_obj_timestamps); */
    /* pdc_timestamp_clean(pdc_create_cont_timestamps); */

    free(pdc_buf_obj_map_timestamps);
    return 0;
}

#endif
