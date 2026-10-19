/**
 * Header-only client-side PDC API call timing/aggregation, for benchmarks
 * that want a per-call-name (mean, stdev, count) report pooled across every
 * rank -- not just rank 0's own timings. Every rank must execute the exact
 * same sequence of PDC_TIMED-wrapped call names, in the same order (true
 * for any benchmark with no rank-dependent branching in its call
 * sequence), since pdc_call_stats_print_csv reduces entry i across ranks
 * positionally rather than by matching names.
 *
 * Usage:
 *   pdc_call_stats_t stats;
 *   pdc_call_stats_init(&stats);
 *   PDC_TIMED(&stats, "PDCobj_create_mpi", obj_id = PDCobj_create_mpi(...));
 *   ...
 *   pdc_call_stats_print_csv(&stats, stdout, rank, MPI_COMM_WORLD);
 */
#ifndef PDC_CALL_STATS_H
#define PDC_CALL_STATS_H

#include <stdio.h>
#include <string.h>
#include <math.h>
#ifdef ENABLE_MPI
#include <mpi.h>
#else
#include <time.h>
#endif

#define PDC_CALL_STATS_MAX_NAMES 64
#define PDC_CALL_STATS_NAME_LEN  63

typedef struct {
    char   name[PDC_CALL_STATS_NAME_LEN + 1];
    double sum;
    double sumsq;
    long   count;
} pdc_call_stat_entry_t;

typedef struct {
    pdc_call_stat_entry_t entries[PDC_CALL_STATS_MAX_NAMES];
    int                   n_entries;
} pdc_call_stats_t;

static inline void
pdc_call_stats_init(pdc_call_stats_t *s)
{
    memset(s, 0, sizeof(*s));
}

static inline pdc_call_stat_entry_t *
pdc_call_stats_find_or_add(pdc_call_stats_t *s, const char *name)
{
    for (int i = 0; i < s->n_entries; i++)
        if (strcmp(s->entries[i].name, name) == 0)
            return &s->entries[i];
    pdc_call_stat_entry_t *e = &s->entries[s->n_entries++];
    strncpy(e->name, name, PDC_CALL_STATS_NAME_LEN);
    return e;
}

static inline void
pdc_call_stats_record(pdc_call_stats_t *s, const char *name, double elapsed_s)
{
    pdc_call_stat_entry_t *e = pdc_call_stats_find_or_add(s, name);
    e->sum += elapsed_s;
    e->sumsq += elapsed_s * elapsed_s;
    e->count += 1;
}

#ifdef ENABLE_MPI
#define PDC_TIMED(stats, name, call)                                                                         \
    do {                                                                                                     \
        double _pdc_timed_t0 = MPI_Wtime();                                                                  \
        call;                                                                                                \
        double _pdc_timed_t1 = MPI_Wtime();                                                                  \
        pdc_call_stats_record((stats), (name), _pdc_timed_t1 - _pdc_timed_t0);                               \
    } while (0)
#else
static inline double
pdc_call_stats_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}
#define PDC_TIMED(stats, name, call)                                                                         \
    do {                                                                                                     \
        double _pdc_timed_t0 = pdc_call_stats_now();                                                         \
        call;                                                                                                \
        double _pdc_timed_t1 = pdc_call_stats_now();                                                         \
        pdc_call_stats_record((stats), (name), _pdc_timed_t1 - _pdc_timed_t0);                               \
    } while (0)
#endif

#ifdef ENABLE_MPI
/**
 * Reduces (sum, sumsq, count) for each recorded call name across every
 * rank in `comm` and, on `rank`==0, writes one CSV row per name:
 *   <record_type>,<name>,,<mean_s>,<stdev_s>,<count>,
 * (the trailing empty fields keep column count consistent with the
 * throughput/data-size rows a caller may also write to the same file --
 * see pdc_call_stats.h's own header comment in the benchmark that uses
 * this, e.g. vpic_bdcats.c, for the full CSV schema). `record_type` lets
 * a caller keeping more than one pdc_call_stats_t (e.g. one per benchmark
 * phase, so the same call name's timing doesn't get pooled across
 * phases) tag each one distinctly -- pass "api_call" for the traditional
 * single-stats-object behavior.
 */
static inline void
pdc_call_stats_print_csv(pdc_call_stats_t *s, FILE *out, int rank, MPI_Comm comm, const char *record_type)
{
    for (int i = 0; i < s->n_entries; i++) {
        double local[3]  = {s->entries[i].sum, s->entries[i].sumsq, (double)s->entries[i].count};
        double global[3] = {0.0, 0.0, 0.0};
        MPI_Reduce(local, global, 3, MPI_DOUBLE, MPI_SUM, 0, comm);
        if (rank == 0) {
            double count = global[2];
            double mean  = (count > 0.0) ? global[0] / count : 0.0;
            double var   = (count > 0.0) ? (global[1] / count - mean * mean) : 0.0;
            if (var < 0.0) /* floating-point noise around a true variance of ~0 */
                var = 0.0;
            double stdev = sqrt(var);
            fprintf(out, "%s,%s,,%.9f,%.9f,%.0f,\n", record_type, s->entries[i].name, mean, stdev, count);
        }
    }
}
#endif

#endif /* PDC_CALL_STATS_H */
