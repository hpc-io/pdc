#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "pdc_stack_ops.h"
#include "pdc_hashtab.h"

profileEntry_t *calltree = NULL;
profileEntry_t *freelist = NULL;

static int profilerrors = 0;

hash_table_t hashtable;

htab_t thisHashTable;

/* For now we disable profiling (by default)
 * Note that one can always ENABLE it by set the
 * environment variable "PROFILE_ENABLE=true"
 */
pbool_t enableProfiling = FALSE;

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
    profileEntry_t *thisEntry;
    if (freelist != NULL) {
        thisEntry = freelist;
        freelist  = thisEntry->next;
    }
    else {
        if ((thisEntry = (profileEntry_t *)malloc(sizeof(profileEntry_t))) == NULL) {
            perror("malloc");
            profilerrors++;
        }
    }

    if (profilerrors)
        return;
    thisEntry->ftnkey = ftnkey;
    thisEntry->tags   = tags;
    thisEntry->prev   = calltree;
    thisEntry->next   = NULL;
    calltree          = thisEntry;

    /* Timing */
    clock_gettime(CLOCK_REALTIME, &thisEntry->startTime);
    RESET_TIMER(thisEntry->callTime);
    return;
}

void
pop()
{
    struct timespec current_time;
    profileEntry_t *master;
    profileEntry_t *thisEntry    = calltree;
    int             update_entry = TRUE;
    if (thisEntry == NULL)
        return; /* This shouldn't happen */

    /* Timing */
    clock_gettime(CLOCK_REALTIME, &current_time);
    TIMER_DIFF(thisEntry->totalTime, current_time, thisEntry->startTime);
    TIMER_DIFF(thisEntry->selfTime, thisEntry->totalTime, thisEntry->callTime);
    calltree = thisEntry->prev;
    if (calltree != NULL) {
        TIMER_ADD(calltree->callTime, thisEntry->totalTime);
    }
    /* Check to see if this function has already been added to the hashtable */
    void **tableEntry = htab_find_slot(thisHashTable, thisEntry, INSERT);
    if (*tableEntry == NULL) {
        /* No table entry found so add it now ... */
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

    /* Rather than freeing the container, we add the
     * current entry onto the freelist.
     */
    thisEntry->next = freelist;
    freelist        = thisEntry;
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
    if (*hashtab == NULL) {
        if ((thisHashTable = htab_try_create(size, hash_profile_entry, eq_profile_entry, free)) == NULL) {
            return;
        }
        *hashtab = thisHashTable;
    }
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

void __attribute__((constructor)) profile_init(void)
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
        if (override_value > 0) {
            default_HashtableSize = override_value;
        }
    }
    initialize_profile(&hashtable, default_HashtableSize);
}

void __attribute__((destructor)) finalize_profile(void)
{
    int count = 1;
    if (thisHashTable != NULL) {
        htab_traverse(thisHashTable, show_profile_info, &count);
    }
}
