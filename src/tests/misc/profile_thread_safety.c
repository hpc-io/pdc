/* Concurrent nested profiling must retain every completed call. */
#include <dlfcn.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define NTHREADS 8
#define NAMES    64
#define ROUNDS   4096
static void (*profile_push)(const char *, const char *);
static void (*profile_pop)(void);
static char            names[NAMES][32];
static const char      outer[]    = "pdc_thread_outer";
static pthread_mutex_t gate_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  gate_cond  = PTHREAD_COND_INITIALIZER;
static int             ready, released, finished, unloaded;
static void *
worker(void *unused)
{
    (void)unused;
    pthread_mutex_lock(&gate_mutex);
    ready++;
    pthread_cond_broadcast(&gate_cond);
    while (!released)
        pthread_cond_wait(&gate_cond, &gate_mutex);
    pthread_mutex_unlock(&gate_mutex);
    for (int i = 0; i < ROUNDS; i++) {
        profile_push(outer, NULL);
        profile_push(names[i % NAMES], NULL);
        profile_pop();
        profile_pop();
    }
    pthread_mutex_lock(&gate_mutex);
    finished++;
    pthread_cond_broadcast(&gate_cond);
    while (!unloaded)
        pthread_cond_wait(&gate_cond, &gate_mutex);
    pthread_mutex_unlock(&gate_mutex);
    return NULL;
}
int
main(void)
{
    setenv("PROFILE_ENABLE", "true", 1);
    setenv("PROFILE_HASHTABLESIZE", "1", 1);
    void *library = dlopen(PDC_TEST_LIBRARY, RTLD_NOW | RTLD_LOCAL);
    if (!library) {
        fprintf(stderr, "%s\n", dlerror());
        return EXIT_FAILURE;
    }
    profile_push         = (void (*)(const char *, const char *))dlsym(library, "push");
    profile_pop          = (void (*)(void))dlsym(library, "pop");
    void (*finish)(void) = (void (*)(void))dlsym(library, "finalize_profile");
    if (!profile_push || !profile_pop || !finish)
        return EXIT_FAILURE;
    for (int i = 0; i < NAMES; i++)
        snprintf(names[i], sizeof(names[i]), "pdc_thread_inner_%d", i);
    pthread_t threads[NTHREADS];
    for (int i = 0; i < NTHREADS; i++)
        if (pthread_create(&threads[i], NULL, worker, NULL))
            return EXIT_FAILURE;
    pthread_mutex_lock(&gate_mutex);
    while (ready != NTHREADS)
        pthread_cond_wait(&gate_cond, &gate_mutex);
    released = 1;
    pthread_cond_broadcast(&gate_cond);
    while (finished != NTHREADS)
        pthread_cond_wait(&gate_cond, &gate_mutex);
    pthread_mutex_unlock(&gate_mutex);
    FILE *capture = tmpfile();
    if (!capture)
        return EXIT_FAILURE;
    fflush(stdout);
    int saved = dup(STDOUT_FILENO);
    if (saved < 0 || dup2(fileno(capture), STDOUT_FILENO) < 0)
        return EXIT_FAILURE;
    /* Reporting may run while a profiled caller is still on this thread's stack. */
    profile_push("pdc_thread_finalize_parent", NULL);
    finish();
    profile_pop();
    fflush(stdout);
    if (dup2(saved, STDOUT_FILENO) < 0)
        return EXIT_FAILURE;
    close(saved);
    rewind(capture);
    char     line[512], name[256];
    unsigned item;
    int64_t  count, seconds, nanoseconds;
    int      seen_outer = 0, seen_inner[NAMES] = {0};
    while (fgets(line, sizeof(line), capture)) {
        if (sscanf(line, "%u %" SCNd64 " %" SCNd64 ",%" SCNd64 " %255s", &item, &count, &seconds,
                   &nanoseconds, name) != 5)
            continue;
        if (strcmp(name, outer) == 0) {
            if (count != NTHREADS * ROUNDS || seen_outer++)
                return EXIT_FAILURE;
        }
        for (int i = 0; i < NAMES; i++) {
            if (strcmp(name, names[i]) == 0) {
                if (count != NTHREADS * (ROUNDS / NAMES) || seen_inner[i]++)
                    return EXIT_FAILURE;
            }
        }
    }
    fclose(capture);
    if (seen_outer != 1)
        return EXIT_FAILURE;
    for (int i = 0; i < NAMES; i++)
        if (seen_inner[i] != 1)
            return EXIT_FAILURE;
    if (dlclose(library) != 0)
        return EXIT_FAILURE;
    /* Release the application threads only after the profiled library unloads. */
    pthread_mutex_lock(&gate_mutex);
    unloaded = 1;
    pthread_cond_broadcast(&gate_cond);
    pthread_mutex_unlock(&gate_mutex);
    for (int i = 0; i < NTHREADS; i++)
        if (pthread_join(threads[i], NULL))
            return EXIT_FAILURE;
    puts("Concurrent nested profiling and thread exit after library unload verified");
    return EXIT_SUCCESS;
}
