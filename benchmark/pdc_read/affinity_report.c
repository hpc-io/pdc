#define _GNU_SOURCE
#include <mpi.h>
#include <dlfcn.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
static void
report_affinity(void)
{
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof set, &set))
        abort();
    char host[128], line[8192];
    gethostname(host, sizeof host);
    size_t n = snprintf(line, sizeof line, "AFFINITY rank=%d host=%s cpus=", rank, host);
    for (int i = 0; i < CPU_SETSIZE; i++)
        if (CPU_ISSET(i, &set))
            n += snprintf(line + n, sizeof line - n, "%d,", i);
    puts(line);
    fflush(stdout);
}
int
MPI_Init(int *argc, char ***argv)
{
    static int (*next)(int *, char ***);
    if (!next)
        next = dlsym(RTLD_NEXT, "MPI_Init");
    if (!next)
        abort();
    int ret = next(argc, argv);
    if (ret == MPI_SUCCESS)
        report_affinity();
    return ret;
}
int
MPI_Init_thread(int *argc, char ***argv, int required, int *provided)
{
    static int (*next)(int *, char ***, int, int *);
    if (!next)
        next = dlsym(RTLD_NEXT, "MPI_Init_thread");
    if (!next)
        abort();
    int ret = next(argc, argv, required, provided);
    if (ret == MPI_SUCCESS) {
        report_affinity();
        printf("MPI_THREAD_LEVEL required=%d provided=%d\n", required, *provided);
        fflush(stdout);
    }
    return ret;
}
