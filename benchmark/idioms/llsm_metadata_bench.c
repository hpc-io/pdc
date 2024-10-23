#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include "cjson/cJSON.h"
#include <dirent.h>
#include <unistd.h>
#include "fs/fs_ops.h"
#include "string_utils.h"
#include "timer_utils.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

const char *LLSM_query_templates = {
    "Scan_Iter=%d AND Cam=%s AND Ch=%d AND stackn=%d AND laser_nm=%d AND "
    "abstime=%d AND fpgatime=%d AND x_str=%d AND y_str=%d AND z_str=%d AND "
    "t_str=%d", // locating a particular
                // object with a
                // particular set of
                // metadata attributes (for the purpose of studying AND operator)
    "Scan_Iter=%d AND (Cam=A OR Cam=B) AND Ch=%d AND stackn=%d AND "
    "laser_nm=%d AND "
    "abstime=%d AND fpgatime=%d AND x_str=%d AND y_str = %d "
    "AND z_str=%d AND t_str=%d", // locating all objects
                                 // collected in both
                                 // camara A and B such
                                 // that these objects
                                 // also match with the
                                 // given set of other
                                 // attributes, and in particular,
                                 // with y_str matching either 002 or 004 (for the purpose of studying OR
                                 // operator)
    "Scan_Iter=%d AND Cam=%s AND Ch=%d AND stackn=%d AND laser_nm=%d AND "
    "abstime=%d AND fpgatime=%d AND x_str=%d AND NOT (y_str=002 AND y_str=004) AND z_str=%d AND "
    "t_str=%d", // locating all objects with a particular set of metadata attributes, and in particular, with
                // y_str not matching either 002 or 004 (for the purpose of studying negation query)
    ""};

const char *BOSS_query_templates = {
    "ra > %.4f AND ra < %.4f AND dec > %.4f AND dec < %.4f", // locating objects in a sky region
    "( ra > %.4f AND ra < %.4f AND dec > %.4f AND dec < %.4f ) OR (ra > %.4f "
    "AND ra < %.4f AND dec > %.4f AND dec < %.4f)", // locating objects in two sky regions
    "NOT (( ra > %.4f AND ra < %.4f AND dec > %.4f AND dec < %.4f ) OR (ra > %.4f "
    "AND ra < %.4f AND dec > %.4f AND dec < %.4f))", // locating objects outside two sky regions
    ""};

/**
 * locating an object by a list of predicates that match all metadata attributes (predicates connected
 * only by AND).
 */
void
perform_object_locating_search()
{
    // Q. shall we change the parameter of the query everytime? There is no cache for metadata search
    // currently
    // P. Run this query for 100 times and report the histogram or breakdown of the time spent on each query
    // 16, 32, 64, 128 servers, 120:1 C/S ratio
    // Overall Measure -> timing of each query v.s. throughput
    // Breakdown -> server time, client time, network time.
    //
}

/**
 * perform a search for objects that match a set of metadata attributes, with some of them matching
 * different specified values (OR involved).
 */
void
perform_object_selecting_search_with_OR()
{
}

/**
 * perform a search for objects that match a set of metadata attributes, with some of them not matching
 * specified values (NOT involved).
 */
void
perform_object_selecting_search_with_NOT()
{
}