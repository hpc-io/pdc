#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <sys/time.h> 

#define OP_TIMER 1
#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#define MAX_LEN 128
int proc_num_g = 1, my_rank_g = 0;

double get_elapsed_time(struct timeval *tstart, struct timeval *tend)
{
    return (double)(((tend->tv_sec-tstart->tv_sec)*1000000LL + tend->tv_usec-tstart->tv_usec) / 1000000.0);
}

int get_file_count(char *filename, int *count)
{
    char c;
    *count = 0;

    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("Could not open file [%s]", filename);
        return -1;
    }

    for (c = getc(fp); c != EOF; c = getc(fp))
        if (c == '\n') (*count)++;

    fclose(fp);
    return 0;
}

int read_filelist(char *filename, int count, char **filelist, uint64_t **filesize)
{
    int i;
    char c;
    FILE *data_file;

    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("Could not open file [%s]", filename);
        return -1;
    }

    *filesize = (uint64_t*)calloc(sizeof(uint64_t), count);
    for (i = 0; i < count; i++) {
        fscanf(fp, "%llu %s", &(*filesize)[i], filelist[i]);
        /* fgets(filelist[i], MAX_LEN, fp); */
        /* filelist[i][strlen(filelist[i])-1] = 0; */
        /* data_file = fopen(filelist[i], "r"); */
        /* if (data_file == NULL) { */
        /*     printf("Could not open file [%s]", filelist[i]); */
        /*     return -1; */
        /* } */
        /* fseek(data_file, 0L, SEEK_END); */
        /* (*filesize)[i] = ftell(data_file); */
        /* fclose(data_file); */

        // Debug print
        /* printf("%llu [%s]\n", (*filesize)[i], filelist[i]); */
    }

    fclose(fp);
    return 0;
}

int prepare_filelist(char *filename, char ***filelist, uint64_t **filesize, int *nfile)
{
    int i, ret;
    char *filelist_1d;

    // Count number of files from the input filelist file
    if (0 == my_rank_g) 
        ret = get_file_count(filename, nfile);

#ifdef ENABLE_MPI
    MPI_Bcast(&ret, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(nfile, 1, MPI_INT, 0, MPI_COMM_WORLD);
#endif
    if (-1 == ret) 
        return -1;

    *filelist = (char**)calloc(sizeof(char*), *nfile);
    filelist_1d = (char*)calloc(sizeof(char), *nfile * MAX_LEN);
    for (i = 0; i < *nfile; i++) 
        (*filelist)[i] = filelist_1d + i*MAX_LEN;

    // Get the file list
    if (0 == my_rank_g) 
        ret = read_filelist(filename, *nfile, *filelist, filesize);
    else 
        *filesize = (uint64_t*)calloc(sizeof(uint64_t), *nfile);

#ifdef ENABLE_MPI
    MPI_Bcast(&ret, 1, MPI_INT, 0, MPI_COMM_WORLD);
#endif
    if (-1 == ret) 
        return -1;

#ifdef ENABLE_MPI
    MPI_Bcast(filelist_1d, MAX_LEN*(*nfile), MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast(*filesize, *nfile, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
#endif

    return 0;
}

void get_op_timer_stats(double open_time, double seek_time, double read_time, double io_time)
{

    double open_time_max = open_time, open_time_min = open_time, open_time_avg = open_time;
    double seek_time_max = seek_time, seek_time_min = seek_time, seek_time_avg = seek_time;
    double read_time_max = read_time, read_time_min = read_time, read_time_avg = read_time;
    double io_time_max = io_time, io_time_min = io_time, io_time_avg = io_time;

    #ifdef ENABLE_MPI
    MPI_Reduce(&open_time,  &open_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&open_time,  &open_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&open_time,  &open_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    open_time_avg /= proc_num_g;

    MPI_Reduce(&seek_time,  &seek_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&seek_time,  &seek_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&seek_time,  &seek_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    seek_time_avg /= proc_num_g;

    MPI_Reduce(&read_time,  &read_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&read_time,  &read_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&read_time,  &read_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    read_time_avg /= proc_num_g;

    MPI_Reduce(&io_time,  &io_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&io_time,  &io_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&io_time,  &io_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    io_time_avg /= proc_num_g;
    #endif

    if (0 == my_rank_g) {
        printf("type,    min,    avg,    max\n");
        printf("open, %.4f, %.4f, %.4f\n", open_time_min, open_time_avg, open_time_max);
        printf("seek, %.4f, %.4f, %.4f\n", seek_time_min, seek_time_avg, seek_time_max);
        printf("read, %.4f, %.4f, %.4f\n", read_time_min, read_time_avg, read_time_max);
        printf(" io , %.4f, %.4f, %.4f\n",   io_time_min,   io_time_avg,   io_time_max);
        fflush(stdout);
    }
}

// It's ok if the number of files is less than the number of readers
int read_within_one_file_contig(int nfile, char **filelist, uint64_t *filesize, uint64_t read_size)
{
    int i, fid;
    char *my_filename;
    void *buf;
    FILE *fp;
    uint64_t my_start_off;
    double read_time, seek_time, open_time, total_time = 0.0, all_time;
    struct timeval t1, t2;
    struct timeval tall1, tall2;

    buf = malloc(read_size);
    
    if (0 == my_rank_g)
        printf("Start read within one file contig\n");
    fflush(stdout);
    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    gettimeofday(&tall1, NULL);

    fid = my_rank_g % nfile;
    my_filename = filelist[fid];

    #ifdef OP_TIMER
    gettimeofday(&t1, NULL);
    #endif
    fp = fopen(my_filename, "r");
    if (fp == NULL) {
        printf("Rank %3d: Could not open file [%s]", my_rank_g, my_filename);
        return -1;
    }
    #ifdef OP_TIMER
    gettimeofday(&t2, NULL);
    open_time = get_elapsed_time(&t1, &t2);
    total_time += open_time;
    #endif

    // In case multiple process reads from one file, differ their start offset
    my_start_off = (rand() % 1024) * read_size % filesize[fid];

    // Make sure starting offset + read size is within the file
    if (my_start_off + read_size >= filesize[fid]) {
        if (my_start_off - read_size < 0) 
            my_start_off = 0;
        else
            my_start_off -= read_size;
    }


    // Debug print
    /* printf("%4d: offset %llu, size %llu, [%s]\n", my_rank_g, my_start_off, read_size, &(my_filename[64])); */

    #ifdef OP_TIMER
    gettimeofday(&t1, NULL);
    #endif

    fseek(fp, my_start_off, SEEK_SET);

    #ifdef OP_TIMER
    gettimeofday(&t2, NULL);
    seek_time = get_elapsed_time(&t1, &t2);
    total_time += seek_time;
    gettimeofday(&t1, NULL);
    #endif

    fread(buf, read_size, 1, fp);
    #ifdef OP_TIMER
    gettimeofday(&t2, NULL);
    read_time = get_elapsed_time(&t1, &t2);
    total_time += read_time;
    #endif

    fclose(fp);
    free(buf);

    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    gettimeofday(&tall2, NULL);
    all_time = get_elapsed_time(&tall1, &tall2);

    #ifdef OP_TIMER
    get_op_timer_stats(open_time, seek_time, read_time, total_time);
    #endif

    fflush(stdout);
    if (0 == my_rank_g)
        printf("Read within one file contig total time: %.4f\n\n", all_time);
    fflush(stdout);

    return 0;
}

int read_within_one_file_noncontig(int nfile, char **filelist, uint64_t *filesize, uint64_t ind_size, int count)
{
    int i, fid;
    char *my_filename;
    void *buf;
    FILE *fp;
    uint64_t my_start_off, read_size, cur_pos, disp;
    double read_time=0.0, seek_time=0.0, open_time=0.0, total_time=0.0, all_time;
    struct timeval t1, t2;
    struct timeval tall1, tall2;
    
    buf = malloc(ind_size);

    if (0 == my_rank_g)
        printf("Start read within one file noncontig\n");
    fflush(stdout);

    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    gettimeofday(&tall1, NULL);

    read_size = ind_size * count;

    fid = my_rank_g % nfile;
    my_filename = filelist[fid];

    #ifdef OP_TIMER
    gettimeofday(&t1, NULL);
    #endif
    fp = fopen(my_filename, "r");
    if (fp == NULL) {
        printf("Rank %3d: Could not open file [%s]", my_rank_g, my_filename);
        return -1;
    }
    #ifdef OP_TIMER
    gettimeofday(&t2, NULL);
    open_time = get_elapsed_time(&t1, &t2);
    total_time += open_time;
    #endif

    // In case multiple process reads from one file, differ their start offset
    my_start_off = (rand() % 1024) * read_size % filesize[fid];

    // Make sure starting offset + read size is within the file
    if (my_start_off + ind_size >= filesize[fid]) {
        if (my_start_off - ind_size < 0) 
            my_start_off = 0;
        else
            my_start_off -= ind_size;
    }


    for (i = 0; i < count; i++) {
        #ifdef OP_TIMER
        gettimeofday(&t1, NULL);
        #endif

        if (i == 0) {
            fseek(fp, my_start_off, SEEK_SET);
            cur_pos = ftell(fp);
        }
        else {
            disp = ind_size * (rand()%32);
            if (disp + cur_pos + ind_size > filesize[fid]) {
                disp = (disp + ind_size) % filesize[fid];
                fseek(fp, disp, SEEK_SET);
            }
            else 
                fseek(fp, disp, SEEK_CUR);
            cur_pos = ftell(fp);
        }

        // Debug print
        /* printf("%4d: offset %llu, size %llu, [%s]\n", my_rank_g, cur_pos, ind_size, &(my_filename[64])); */

        #ifdef OP_TIMER
        gettimeofday(&t2, NULL);
        seek_time += get_elapsed_time(&t1, &t2);
        total_time += seek_time;
        gettimeofday(&t1, NULL);
        #endif

        fread(buf, ind_size, 1, fp);
        #ifdef OP_TIMER
        gettimeofday(&t2, NULL);
        read_time += get_elapsed_time(&t1, &t2);
        #endif
    }

    #ifdef OP_TIMER
    total_time = open_time+seek_time+read_time;
    #endif

    fclose(fp);
    free(buf);

    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    gettimeofday(&tall2, NULL);
    all_time = get_elapsed_time(&tall1, &tall2);

    #ifdef OP_TIMER
    get_op_timer_stats(open_time, seek_time, read_time, total_time);
    #endif

    fflush(stdout);
    if (0 == my_rank_g)
        printf("Read within one file %d non contig total time: %.4f\n\n", count, all_time);
    fflush(stdout);

    return 0;
}

int read_multi_files(int nfile, char **filelist, uint64_t *filesize, uint64_t ind_size, int count)
{
    int i, fid;
    char *my_filename;
    void *buf;
    FILE *fp;
    uint64_t my_start_off, read_size, cur_pos, disp;
    double read_time=0.0, seek_time=0.0, open_time=0.0, total_time=0.0, all_time;
    struct timeval t1, t2;
    struct timeval tall1, tall2;

    read_size = ind_size * count;
    buf = malloc(ind_size);
    
    if (0 == my_rank_g)
        printf("Start read multi files \n");
    fflush(stdout);

    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    gettimeofday(&tall1, NULL);

    for (i = 0; i < count; i++) {

        fid = rand() % nfile;
        my_filename = filelist[fid];

        #ifdef OP_TIMER
        gettimeofday(&t1, NULL);
        #endif
        fp = fopen(my_filename, "r");
        if (fp == NULL) {
            printf("Rank %3d: Could not open file [%s]", my_rank_g, my_filename);
            return -1;
        }
        #ifdef OP_TIMER
        gettimeofday(&t2, NULL);
        open_time = get_elapsed_time(&t1, &t2);
        total_time += open_time;
        #endif

        #ifdef OP_TIMER
        gettimeofday(&t1, NULL);
        #endif

        disp = ind_size * rand() % (filesize[fid] - ind_size);
        fseek(fp, disp, SEEK_SET);

        // Debug print
        /* printf("%4d: offset %llu, size %llu, [%s]\n", my_rank_g, disp, ind_size, &(my_filename[64])); */

        #ifdef OP_TIMER
        gettimeofday(&t2, NULL);
        seek_time += get_elapsed_time(&t1, &t2);
        total_time += seek_time;
        gettimeofday(&t1, NULL);
        #endif

        fread(buf, ind_size, 1, fp);
        #ifdef OP_TIMER
        gettimeofday(&t2, NULL);
        read_time += get_elapsed_time(&t1, &t2);
        #endif
    }

    #ifdef OP_TIMER
    total_time = open_time+seek_time+read_time;
    #endif


    fclose(fp);
    free(buf);

    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif
    gettimeofday(&tall2, NULL);
    all_time = get_elapsed_time(&tall1, &tall2);

    #ifdef OP_TIMER
    get_op_timer_stats(open_time, seek_time, read_time, total_time);
    #endif

    if (0 == my_rank_g)
        printf("Read multi files with %d reads total time: %.4f\n\n", count, all_time);
    fflush(stdout);

    return 0;
}

int main(int argc, char *argv[])
{
    int ret, i, n_reader, total_size_mb, ind_size_mb, ind_count, nfile, repeat;
    uint64_t total_size, ind_size;
    char **filelist = NULL;
    uint64_t *filesize;
    char *filename;
    double total_time = 0.0;
    struct timeval t1, t2;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num_g);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank_g);
#endif


    // Fix total read size
    // input: total read size, individual read size, 
    n_reader      = proc_num_g;
    filename      = argv[1];
    total_size_mb = atoi(argv[2]);
    ind_size_mb   = atoi(argv[3]);
    repeat        = atoi(argv[4]);

    if (ind_size_mb > total_size_mb) 
        ind_size_mb = total_size_mb;

    if (repeat <=0 || repeat > 10) 
        repeat = 1;

    // Convert to bytes
    total_size    = total_size_mb * 1048576ull;
    ind_size      = ind_size_mb * 1048576ull;
    ind_count     = total_size_mb / ind_size_mb / proc_num_g;
  
    ret = prepare_filelist(filename, &filelist, &filesize, &nfile);
 
    if (0 == my_rank_g) {
        printf("Read from %d files, total size %d MB, individual size %d MB, individual count %d, repeat %d\n", 
                nfile, total_size_mb, ind_size_mb, ind_count, repeat);
    }

    // Debug print
    /* printf("filelist %d:\n", nfile); */
    /* for (i = 0; i < nfile; i++) */ 
    /*     printf("%d %llu %s\n", i, filesize[i], filelist[i]); */

    srand(my_rank_g);
    for (i = 0; i < repeat; i++) {
        /* srand((my_rank_g+i)%proc_num_g); */
        if (0 == my_rank_g)
            printf("Iter %d Start\n", i);
        read_within_one_file_contig(nfile, filelist, filesize, total_size/proc_num_g);
        read_within_one_file_noncontig(nfile, filelist, filesize, ind_size, ind_count);
        read_multi_files(nfile, filelist, filesize, ind_size, ind_count);
        if (0 == my_rank_g)
            printf("Iter %d Done\n\n", i);
    }

    if (0 == my_rank_g)
        printf("All Tests Done!\n");
done:
    fflush(stdout);
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
