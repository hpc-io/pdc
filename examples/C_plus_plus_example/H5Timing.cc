#include "H5Timing.h"

#ifdef H5_TIMING_ENABLE

static H5TimerClass* timer_class;

int init_timers() {
    struct timeval temp_time;

    timer_class = (H5TimerClass*) malloc(sizeof(H5TimerClass));

    gettimeofday(&temp_time, NULL);
    timer_class->total_start_time = (temp_time.tv_usec + temp_time.tv_sec * 1000000) + .0;

    timer_class->dataset_timers = new std::vector<H5Timer>;
    timer_class->dataset_sz_timers = new std::vector<H5Timer>;
    timer_class->dataset_read_timers = new std::vector<H5Timer>;
    timer_class->dataset_sz_read_timers = new std::vector<H5Timer>;
#ifdef PDC_PATCH
    timer_class->PDCwrite_count = 0;
    timer_class->PDCread_count = 0;
    timer_class->PDCstart_time = .0;
    timer_class->PDCwait_time = .0;
#else
    timer_class->H5Dwrite_count = 0;
    timer_class->H5Dread_count = 0;
    timer_class->H5Dwrite_time = .0;
    timer_class->H5Dread_time = .0;
    timer_class->wrap_requests_time = .0;
    timer_class->merge_requests_time = .0;
    timer_class->H5Dclose_time = .0;
#endif
    return 0;
}

// timers are C++ std::vector<H5Timer>
#define TIMER_START(timers) {                                   \
    struct timeval temp_time;                                       \
    H5Timer temp;                              \
    gettimeofday(&temp_time, NULL); \
    temp.start = (temp_time.tv_usec + temp_time.tv_sec * 1000000) + .0; \
    temp.name = strdup(name); \
    timers->push_back(temp);          \
}

#define TIMER_END(timers, data_size) { \
    struct timeval temp_time;     \
    gettimeofday(&temp_time, NULL); \
    timers->back().end = (temp_time.tv_usec + temp_time.tv_sec * 1000000) + .0; \
    timers->back().data_size = data_size; \
}

int register_timer_start(double *start_time) {
    struct timeval temp_time;
    gettimeofday(&temp_time, NULL);
    *start_time = (temp_time.tv_usec + temp_time.tv_sec * 1000000);
    return 0;
}
#ifdef PDC_PATCH
int register_PDCstart_timer_end(double start_time) {
    struct timeval temp_time;
    gettimeofday(&temp_time, NULL);
    timer_class->PDCstart_time += (temp_time.tv_usec + temp_time.tv_sec * 1000000) - start_time;
    return 0;
}

int register_PDCwait_timer_end(double start_time) {
    struct timeval temp_time;
    gettimeofday(&temp_time, NULL);
    timer_class->PDCwait_time += (temp_time.tv_usec + temp_time.tv_sec * 1000000) - start_time;
    return 0;
}

int increment_PDCwrite() {
    timer_class->PDCwrite_count++;
    return 0;
}

int increment_PDCread() {
    timer_class->PDCread_count++;
    return 0;
}
#else
int register_H5Dclose_timer_end(double start_time) {
    struct timeval temp_time;
    gettimeofday(&temp_time, NULL);
    timer_class->H5Dclose_time += (temp_time.tv_usec + temp_time.tv_sec * 1000000) - start_time;
    return 0;
}

int register_wrap_requests_timer_end(double start_time) {
    struct timeval temp_time;
    gettimeofday(&temp_time, NULL);
    timer_class->wrap_requests_time += (temp_time.tv_usec + temp_time.tv_sec * 1000000) - start_time;
    return 0;
}

int register_H5Dwrite_timer_end(double start_time) {
    struct timeval temp_time;
    gettimeofday(&temp_time, NULL);
    timer_class->H5Dwrite_time += (temp_time.tv_usec + temp_time.tv_sec * 1000000) - start_time;
    return 0;
}

int increment_H5Dwrite() {
    timer_class->H5Dwrite_count++;
    return 0;
}

int increment_H5Dread() {
    timer_class->H5Dread_count++;
    return 0;
}
#endif

int register_merge_requests_timer_end(double start_time) {
    struct timeval temp_time;
    gettimeofday(&temp_time, NULL);
    timer_class->merge_requests_time += (temp_time.tv_usec + temp_time.tv_sec * 1000000) - start_time;
    return 0;
}

int register_dataset_timer_start(const char *name) {
    TIMER_START(timer_class->dataset_timers);
    return 0;
}
int register_dataset_timer_end(size_t data_size) {
    TIMER_END(timer_class->dataset_timers, data_size);
    return 0;
}

int register_dataset_sz_timer_start(const char *name) {
    TIMER_START(timer_class->dataset_sz_timers);
    return 0;
}

int register_dataset_sz_timer_end(size_t data_size) {
    TIMER_END(timer_class->dataset_sz_timers, data_size);
    return 0;
}

int register_dataset_read_timer_start(const char *name) {
    TIMER_START(timer_class->dataset_read_timers);
    return 0;
}
int register_dataset_read_timer_end(size_t data_size) {
    TIMER_END(timer_class->dataset_read_timers, data_size);
    return 0;
}

int register_dataset_sz_read_timer_start(const char *name) {
    TIMER_START(timer_class->dataset_sz_read_timers);
    return 0;
}

int register_dataset_sz_read_timer_end(size_t data_size) {
    TIMER_END(timer_class->dataset_sz_read_timers, data_size);
    return 0;
}

static int record_timer(std::vector<H5Timer> *timer, const char* filename) {
    FILE *stream;
    size_t i, total_mem_size;
    double total_time = 0, min_time = -1, max_time = -1;
    stream = fopen(filename, "w");
    fprintf(stream, "dataset_name,start,end,elapse,data_size\n");
    total_mem_size = 0;
    for ( i = 0; i < timer->size(); ++i ) {
        fprintf(stream, "%s, %lf, %lf, %lf, %zu\n", timer[0][i].name, timer[0][i].start, timer[0][i].end, (timer[0][i].end - timer[0][i].start)/1000000, timer[0][i].data_size);
        total_time += (timer[0][i].end - timer[0][i].start)/1000000;
        if (min_time == -1 || min_time > timer[0][i].start) {
            min_time = timer[0][i].start;
        }
        if (max_time == -1 || max_time < timer[0][i].end) {
            max_time = timer[0][i].end;
        }
        total_mem_size += timer[0][i].data_size;
	free(timer[0][i].name);
    }
    fprintf(stream, "total,%lf,%lf,%lf,%zu\n", min_time, max_time, total_time, total_mem_size);
    fclose(stream);
    timer->clear();
    return 0;
}

static int output_results() {
#ifdef PDC_PATCH
    printf("total PDCwrite calls = %d, PDCread calls = %d\n", timer_class->PDCwrite_count, timer_class->PDCread_count);
#else
    printf("total H5Dwrite calls = %d, H5Dread calls = %d\n", timer_class->H5Dwrite_count, timer_class->H5Dread_count);
#endif
    if ( timer_class->dataset_timers->size() ) {
        record_timer(timer_class->dataset_timers, "dataset_write_record.csv");
    }
    if ( timer_class->dataset_sz_timers->size() ) {
        record_timer(timer_class->dataset_sz_timers, "dataset_sz_write_record.csv");
    }
    if ( timer_class->dataset_read_timers->size() ) {
        record_timer(timer_class->dataset_read_timers, "dataset_read_record.csv");
    }
    if ( timer_class->dataset_sz_read_timers->size() ) {
        record_timer(timer_class->dataset_sz_read_timers, "dataset_sz_read_record.csv");
    }
    return 0;
}

int finalize_timers() {
    struct timeval temp_time;
    output_results();

    gettimeofday(&temp_time, NULL);
    timer_class->total_end_time = (temp_time.tv_usec + temp_time.tv_sec * 1000000) + .0;
#ifdef PDC_PATCH
    printf("total program time is %lf, PDCstart time = %lf, PDCwait time = %lf\n", (timer_class->total_end_time - timer_class->total_start_time) / 1000000.0, timer_class->PDCstart_time / 1000000.0, timer_class->PDCwait_time / 1000000.0);
#else
    printf("total program time is %lf, H5Dwrite time = %lf, H5Dread time = %lf\n", (timer_class->total_end_time - timer_class->total_start_time) / 1000000.0, timer_class->H5Dwrite_time / 1000000.0, timer_class->H5Dread_time / 1000000.0);
    printf("merge requests time = %lf, wrap requests time = %lf, H5Dclose = %lf\n", timer_class->merge_requests_time / 1000000.0, timer_class->wrap_requests_time / 1000000.0, timer_class->H5Dclose_time / 1000000.0);
#endif
    free(timer_class);

    return 0;
}
#endif
