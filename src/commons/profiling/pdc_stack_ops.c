#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "pdc_stack_ops.h"
#include "pdc_logger.h"
#include "pdc_hashtab.h"
#include "pdc_malloc.h"
#include <pthread.h>

/* A callback may run on a worker while another thread records a nested call. */
typedef struct {
    profileEntry_t *calltree;
    profileEntry_t *freelist;
    int             errors;
} profile_stack_t;
static pthread_key_t   profile_stack_key;
static pthread_once_t  profile_stack_once = PTHREAD_ONCE_INIT;
static int             profile_stack_key_status;
static pthread_mutex_t profile_table_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
profile_stack_destroy(void *value)
{
    profile_stack_t *stack = value;
    profileEntry_t * entry;
    while ((entry = stack->calltree) != NULL) {
        stack->calltree = entry->prev;
        free(entry);
    }
    while ((entry = stack->freelist) != NULL) {
        stack->freelist = entry->next;
        free(entry);
    }
    free(stack);
}

static void
profile_stack_key_create(void)
{
    profile_stack_key_status = pthread_key_create(&profile_stack_key, profile_stack_destroy);
}

static profile_stack_t *
profile_stack_get(void)
{
    profile_stack_t *stack;
    pthread_once(&profile_stack_once, profile_stack_key_create);
    if (profile_stack_key_status != 0)
        return NULL;
    stack = pthread_getspecific(profile_stack_key);
    if (stack == NULL) {
        stack = calloc(1, sizeof(*stack));
        if (stack != NULL && pthread_setspecific(profile_stack_key, stack) != 0) {
            free(stack);
            return NULL;
        }
    }
    return stack;
}

hash_table_t hashtable;
htab_t       thisHashTable;

/**
 * We need to disable these macros in certain
 * files (pdc_stack_ops.c, pdc_malloc.c, and pdc_hashtab.c)
 * so we do not create an infinite recursion chain
 *
 * Only the FUNC_* need to be disabled in pdc_malloc.c
 */
#ifdef ENABLE_PROFILING
pbool_t enableProfiling = TRUE;
#undef PDC_Malloc
#undef PDC_Realloc
#undef PDC_Free
#undef PDC_Calloc
#define PDC_Malloc  malloc
#define PDC_Realloc realloc
#define PDC_Free(p) ((void *)free(p), (void *)NULL)
#define PDC_Calloc  calloc

#undef FUNC_ENTER
#undef FUNC_LEAVE
#undef FUNC_LEAVE_VOID
#define FUNC_ENTER(x)
#define FUNC_LEAVE(x)     return (x)
#define FUNC_LEAVE_VOID() return
#else
pbool_t enableProfiling = FALSE;
#endif

/*
 *  The idea of this implementation is to simulate the call stack
 *  of the running application.  Each function that we care about
 *  begins with a FUNC_ENTER(x) declaration and finishes with
 *  FUNC_LEAVE(ret).   These of course are macros and under
 *  the condition that we enable profiling, these expand into
 *  push and pop operations which we define below.
 *
 *  Example: suppose that a user application is defined as follows
 *  int main() {
 *       a();
 *       b();
 *       c();
 *       return 0;
 *  }
 *
 *  void a() {
 *       aa();
 *       aaa();
 *       ab();
 *  }
 *
 *  void b() {
 *       bb();
 *       bbb();
 *       bc();
 *  }
 *
 *  void c() {
 *       cc();
 *       ccc();
 *       ca();
 *  }
 *
 *  Assume that all of the internal functions only make system calls
 *  or 3rd party libraries, i.e. the underlying functions will NOT
 *  be profiled.
 *
 *  The implementation of stack_ops will maintain a call tree
 *  that mirrors that of the actual program, i.e. the alltree data
 *  structure will contain something like the following as we enter
 *  the first function contained by a():
 *
 *     ("main") --> ("a") --> ("aa")
 *
 *  The entry for "main" has a /start_time and no /total_time
 *  Similarly, "a" has it's own /start_time and no /total_time
 *  The final entry: "aa" has a start-time and just prior to
 *  the return to it's parent ("a"), we sample the real-time
 *  clock as part of the POP functionality. Using the current
 *  time minus the start-time we establish the raw total elapsed
 *  time for the current function.
 *  NOTE: The actual runtime spent within the function is
 *  a calculation which subtracts out the total elapsed times
 *  of all of the lower-level functions, e.g. suppose ("a")
 *  has a total runtime of 10.  If the total runtime of ("aa")
 *  in the simple call chain shown above is 5, then the actual
 *  profiled time spent in ("a") is 10 - 5 = 5.
 *  Ultimately, if were to execute the entire program and then
 *  sum all of the individual profile times, the total should
 *  match the execution time of the program.
 */
void
push(const char *ftnkey, const char *tags)
{
    profile_stack_t *stack = profile_stack_get();
    profileEntry_t * thisEntry;
    if (stack == NULL || stack->errors)
        return;
    if (stack->freelist != NULL) {
        thisEntry       = stack->freelist;
        stack->freelist = thisEntry->next;
    }
    else {
        if ((thisEntry = (profileEntry_t *)malloc(sizeof(profileEntry_t))) == NULL) {
            perror("malloc");
            stack->errors++;
        }
    }

    if (stack->errors)
        return;
    thisEntry->ftnkey = ftnkey;
    thisEntry->tags   = tags;
    thisEntry->prev   = stack->calltree;
    thisEntry->next   = NULL;
    stack->calltree   = thisEntry;

    /* Timing */
    clock_gettime(CLOCK_REALTIME, &thisEntry->startTime);
    RESET_TIMER(thisEntry->callTime);
    return;
}

void
pop()
{
    profile_stack_t *stack = profile_stack_get();
    if (stack == NULL || stack->errors)
        return;
    struct timespec current_time;
    profileEntry_t *master;
    profileEntry_t *thisEntry    = stack->calltree;
    int             update_entry = TRUE;
    if (thisEntry == NULL)
        return; /* This shouldn't happen */

    /* Timing */
    clock_gettime(CLOCK_REALTIME, &current_time);
    TIMER_DIFF(thisEntry->totalTime, current_time, thisEntry->startTime);
    TIMER_DIFF(thisEntry->selfTime, thisEntry->totalTime, thisEntry->callTime);
    stack->calltree = thisEntry->prev;
    if (stack->calltree != NULL) {
        TIMER_ADD(stack->calltree->callTime, thisEntry->totalTime);
    }
    /* Check to see if this function has already been added to the hashtable */
    pthread_mutex_lock(&profile_table_mutex);
    void **tableEntry = htab_find_slot(thisHashTable, thisEntry, INSERT);
    if (*tableEntry == NULL) {
        /* No table entry found so add it now ... */
        /* Profiling bookkeeping must not enter profiled allocation or shared memory counters. */
        master = (profileEntry_t *)malloc(sizeof(profileEntry_t));
        if (master) {
            thisEntry->count = 1;
            memcpy(master, thisEntry, sizeof(profileEntry_t));
            *tableEntry = master;
        }
        update_entry = FALSE;
    }

    if (update_entry) {
        master = *(profileEntry_t **)tableEntry;
        master->count++;
        TIMER_ADD(master->totalTime, thisEntry->totalTime);
        TIMER_ADD(master->selfTime, thisEntry->selfTime);
    }

    pthread_mutex_unlock(&profile_table_mutex);

    /* Rather than freeing the container, we add the
     * current entry onto the stack->freelist.
     */
    thisEntry->next = stack->freelist;
    stack->freelist = thisEntry;
}

hashval_t
hash_profile_entry(const void *p)
{
    const profileEntry_t *thisEntry = (const profileEntry_t *)p;
    return htab_hash_string(thisEntry->ftnkey);
}

int
eq_profile_entry(const void *a, const void *b)
{
    const profileEntry_t *tp_a = (const profileEntry_t *)a;
    const profileEntry_t *tp_b = (const profileEntry_t *)b;
    return (tp_a->ftnkey == tp_b->ftnkey);
}

void
initialize_profile(void **hashtab, size_t size)
{
    pthread_mutex_lock(&profile_table_mutex);
    if (*hashtab == NULL) {
        if ((thisHashTable = htab_try_create(size, hash_profile_entry, eq_profile_entry, free)) == NULL) {
            pthread_mutex_unlock(&profile_table_mutex);
            return;
        }
        *hashtab = thisHashTable;
    }
    pthread_mutex_unlock(&profile_table_mutex);
}

int
show_profile_info(void **ht_live_entry, void *extraInfo ATTRIBUTE(unused))
{
    static int count     = 0;
    char *     LineBreak = "------------------------------------------------------------------------------";
    char *     header    = " item  calls Time/call [Sec,nSec]\tftn_name";
    const profileEntry_t *thisEntry = *(const profileEntry_t **)ht_live_entry;

    if (thisEntry) {
        struct timespec totalTime;
        int64_t         totalCalls = thisEntry->count;
        if (count == 0)
            puts(header);
        totalTime = thisEntry->totalTime;
        /* The logger is profiled and must not mutate the table during traversal. */
        printf("%s\n %d\t%-6" PRId64 " %6" PRId64 ",%6" PRId64 "\t\t %s\n", LineBreak, ++count, totalCalls,
               totalTime.tv_sec / totalCalls, totalTime.tv_nsec / totalCalls, thisEntry->ftnkey);
    }

    return TRUE;
}

/*  Returns 1 if we set enableProfiling to TRUE
 *  otherwise returns 0.
 */
int
toggle_profile_enable()
{
    if (enableProfiling == FALSE)
        enableProfiling = TRUE;
    else
        enableProfiling = FALSE;

    return (enableProfiling ? 1 : 0);
}

/* These functions should be used when we've actually built the profiler as a shared library.
 * Note: One might check an environment variable to see if a non-default size
 * for the hashtable initialization should be used...
 * The profile_fini should probably be used to dump the contents of the profile
 * hashtable.
 */
static void
pdc_profile_initialize(void)
{
    int   default_HashtableSize = 128;
    char *size_override         = NULL;
    char *profile_enable        = getenv("PROFILE_ENABLE");
    if (profile_enable != NULL) {
        if (strcasecmp(profile_enable, "true") == 0) {
            enableProfiling = TRUE;
        }
        else if (strcasecmp(profile_enable, "false") == 0) {
            enableProfiling = FALSE;
        }
    }
    // While it is tempting to skip creating a hashtable
    // if we've disabled profiling (see above), I want
    // to give the user the ability at runtime to
    // possibly enable everything...
    // I don't currently include any APIs to enable
    // or disable profiling at runtime, but that is
    // on the TODO list.

    size_override = getenv("PROFILE_HASHTABLESIZE");
    if (size_override != NULL) {
        int override_value = atoi(size_override);
        /* Bound the user-supplied size on both ends to avoid an unbounded
         * allocation in the hashtable backend. */
        if (override_value > 0 && override_value <= (1 << 20)) {
            default_HashtableSize = override_value;
        }
    }
    initialize_profile(&hashtable, default_HashtableSize);
}

static void
pdc_profile_finalize(void)
{
    int count = 1;
    pthread_mutex_lock(&profile_table_mutex);
    if (thisHashTable != NULL) {
        htab_traverse(thisHashTable, show_profile_info, &count);
    }
    pthread_mutex_unlock(&profile_table_mutex);
}

/* Keep legacy entry points, but never use interposable symbols as loader hooks.
 * Kerberos also exports profile_init and may already be loaded by Python/MPI. */
void
profile_init(void)
{
    pdc_profile_initialize();
}

void
finalize_profile(void)
{
    pdc_profile_finalize();
}

static void __attribute__((constructor)) pdc_profile_load(void)
{
    pdc_profile_initialize();
}

static void __attribute__((destructor)) pdc_profile_unload(void)
{
    pdc_profile_finalize();
}
