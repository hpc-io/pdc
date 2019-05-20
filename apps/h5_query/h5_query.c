#include <stdio.h>
#include <stdlib.h>
#include <float.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <stdint.h>

#include <hdf5.h>

#define ENABLE_MPI 1
#define NVAR 7
/* #define QUERY_ENERGY_ONLY 1 */

struct timeval start_time[5];
float          elapse[5];

typedef struct vpic_data_t {
    size_t nparticle;
    float  *energy;
    #ifndef QUERY_ENERGY_ONLY
    float  *x;
    float  *y;
    float  *z;
    float  *ux;
    float  *uy;
    float  *uz;
    #endif
} vpic_data_t;

typedef struct query_constraint_t {
    float energy_lo;
    float energy_hi;
    #ifndef QUERY_ENERGY_ONLY
    float x_lo;
    float x_hi;
    float y_lo;
    float y_hi;
    float z_lo;
    float z_hi;
    float ux_lo;
    float ux_hi;
    float uy_lo;
    float uy_hi;
    float uz_lo;
    float uz_hi;
    #endif
} query_constraint_t;

query_constraint_t query_constraints[] = {
    {
        1.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -200, -100, 
           0,  100,
        -100,    0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        1.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -200, -100, 
           0,  100,
        -100,    0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        1.6, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -200, -100, 
           0,  100,
        -100,    0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        1.8, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -200, -100, 
           0,  100,
        -100,    0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -200, -100, 
           0,  100,
        -100,    0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -200, -100, 
           0,  100,
        -100,    0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -200, -100, 
           0,  100,
        -100,    0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },



    {
        1.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -180, -120,
          20,   80, 
         -80,  -20, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -180, -120,
          20,   80, 
         -80,  -20, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.6, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -180, -120,
          20,   80, 
         -80,  -20, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.8, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -180, -120,
          20,   80, 
         -80,  -20, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -180, -120,
          20,   80, 
         -80,  -20, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -180, -120,
          20,   80, 
         -80,  -20, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -180, -120,
          20,   80, 
         -80,  -20, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },




    {
        1.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
         -60,    0, 
          40,  100, 
        -160, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
         -60,    0, 
          40,  100, 
        -160, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.6, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
         -60,    0, 
          40,  100, 
        -160, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.8, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
         -60,    0, 
          40,  100, 
        -160, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
         -60,    0, 
          40,  100, 
        -160, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
         -60,    0, 
          40,  100, 
        -160, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
         -60,    0, 
          40,  100, 
        -160, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },





    {
        1.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        130, 170,
        -50, -30, 
         30,  70, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        130, 170,
        -50, -30, 
         30,  70, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.6, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        130, 170,
        -50, -30, 
         30,  70, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        1.8, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        130, 170,
        -50, -30, 
         30,  70, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        130, 170,
        -50, -30, 
         30,  70, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        130, 170,
        -50, -30, 
         30,  70, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },

    {
        2.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        130, 170,
        -50, -30, 
         30,  70, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },





    {
        1.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        145, 155, 
        -55, -45, 
         45,  55, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        1.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        145, 155, 
        -55, -45, 
         45,  55, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        1.6, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        145, 155, 
        -55, -45, 
         45,  55, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        1.8, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        145, 155, 
        -55, -45, 
         45,  55, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        145, 155, 
        -55, -45, 
         45,  55, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.2, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        145, 155, 
        -55, -45, 
         45,  55, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.4, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        145, 155, 
        -55, -45, 
         45,  55, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },




    {
        2.5, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        100, 109, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.6, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        100, 109, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.7, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        100, 109, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.8, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        100, 109, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.9, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        100, 109, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -120, -100, 
        -150, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -120, -110, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        2.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -120, -110, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        3.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -120, -110, 
        -150, -100, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        3.0, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -120, -110, 
        -150, -140, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },



    {
        -FLT_MAX, FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        3.9,     4.00, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        3.8,     3.9, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    // Energy > 1.55 && < 1.56 
    {
        3.7,     3.8, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    // Energy > 1.555 && < 1.556 
    {
        3.6,     3.7, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    // Energy > 1.5555 && < 1.5556 
    {
        3.5,     3.6, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    // Energy > 1.55555 && < 1.55556 
    {
        3.4,     3.5, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    // Energy > 1.555555 && < 1.555556 
    {
        3.3,     3.4, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        3.2,     3.3, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        3.1,     3.2, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        3.0,     3.1, 
        #ifndef QUERY_ENERGY_ONLY
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    {
        -FLT_MAX, 1.3, 
        #ifndef QUERY_ENERGY_ONLY
        308.0,   309.0, 
        149.0,   150.0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    },
    // Energy > 1.3&&300 < X < 310&&140 < Y < 150
    {
        1.3,     FLT_MAX, 
        #ifndef QUERY_ENERGY_ONLY
        300.0,   310.0, 
        140.0,   150.0, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX, 
        -FLT_MAX, FLT_MAX
        #endif
    }
};

#define timer_on(id) gettimeofday (&start_time[id], NULL)
#define timer_off(id) 	\
		{	\
		     struct timeval result, now; \
		     gettimeofday (&now, NULL);  \
		     timeval_subtract(&result, &now, &start_time[id]);	\
		     elapse[id] += result.tv_sec+ (double) (result.tv_usec)/1000000.;	\
		}

#define timer_msg(id, msg) \
	printf("%f seconds elapsed in %s\n", (double)(elapse[id]), msg);  \

#define timer_reset(id) elapse[id] = 0

int
timeval_subtract (struct timeval *result, struct timeval *x, struct timeval *y)
{
    /* Perform the carry for the later subtraction by updating y. */
    if (x->tv_usec < y->tv_usec) {
        int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
        y->tv_usec -= 1000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_usec - y->tv_usec > 1000000) {
        int nsec = (y->tv_usec - x->tv_usec) / 1000000;
        y->tv_usec += 1000000 * nsec;
        y->tv_sec -= nsec;
    }

    result->tv_sec = x->tv_sec - y->tv_sec;
    result->tv_usec = x->tv_usec - y->tv_usec;

    return x->tv_sec < y->tv_sec;
}

void print_usage(char *name)
{
    printf("Usage: %s /path/to/file query_id_from [query_id_to]\n", name);
}

void print_query_constraint(query_constraint_t *constraint)
{
    if (constraint == NULL) 
        return;

    printf("Query constraint:\n");
    if (constraint->energy_lo != -FLT_MAX || constraint->energy_hi != FLT_MAX) 
        printf("  %.1f < Energy < %.1f\t", constraint->energy_lo, constraint->energy_hi);
    #ifndef QUERY_ENERGY_ONLY
    if (constraint->x_lo != -FLT_MAX || constraint->x_hi != FLT_MAX) 
        printf("  %.1f < x < %.1f\t", constraint->x_lo, constraint->x_hi);

    if (constraint->y_lo != -FLT_MAX || constraint->y_hi != FLT_MAX) 
        printf("  %.1f < y < %.1f\t", constraint->y_lo, constraint->y_hi);

    if (constraint->z_lo != -FLT_MAX || constraint->z_hi != FLT_MAX) 
        printf("  %.1f < z < %.1f\t", constraint->z_lo, constraint->z_hi);

    #endif
    printf("\n\n");
}

int do_query(int qid, vpic_data_t *vpic_data, uint64_t **coords)
{
    int i, j, nhits = 0, alloc_cnt;
    int    is_satisfy;
    query_constraint_t *constraint = &query_constraints[qid];


    alloc_cnt = 100;
    *coords = (uint64_t*)malloc(alloc_cnt * sizeof(uint64_t));
    for (i = 0; i < vpic_data->nparticle; i++) {
        is_satisfy = 1;
        #ifndef QUERY_ENERGY_ONLY
        if (vpic_data->energy[i] <= constraint->energy_lo || vpic_data->energy[i] > constraint->energy_hi ||
            vpic_data->x[i]  <= constraint->x_lo  || vpic_data->x[i]  >= constraint->x_hi  ||
            vpic_data->y[i]  <= constraint->y_lo  || vpic_data->y[i]  >= constraint->y_hi  ||
            vpic_data->z[i]  <= constraint->z_lo  || vpic_data->z[i]  >= constraint->z_hi  
            /* || vpic_data->ux[i] <= constraint->ux_lo || vpic_data->ux[i] >= constraint->ux_hi || */
            /* vpic_data->uy[i] <= constraint->uy_lo || vpic_data->uy[i] >= constraint->uy_hi || */
            /* vpic_data->uz[i] <= constraint->uz_lo || vpic_data->uz[i] >= constraint->uz_hi */ 
           ) {
            is_satisfy = 0;
        }
        #else
        if (vpic_data->energy[i] < constraint->energy_lo || vpic_data->energy[i] > constraint->energy_hi) {
            is_satisfy = 0;
        }
        #endif
        else {
            //TMP
            /* if (qid == 8) { */
                /* printf(" , %lu", i); */
            /* } */
            if (nhits + 2 > alloc_cnt) {
                alloc_cnt *= 2;
                *coords = realloc(*coords, alloc_cnt*sizeof(uint64_t));
            }
            (*coords)[nhits] = i;

            nhits++;
        }
    }

    /* printf("Query %d has %lu hits\n", qid, nhits); */
    return nhits;
}

int main (int argc, char* argv[])
{
    int         my_rank = 0, num_procs = 1, i, j, ndim, query_id = 0, query_id2;
    char        *file_name, *group_name;
    char        *dset_names[7] = {"Energy", "x", "y", "z", "Ux", "Uy", "Uz"};
    hid_t       file_id, group_id, dset_ids[NVAR], filespace, memspace, fapl;
    vpic_data_t vpic_data;
    hsize_t     dims[3], my_offset, my_size;

    memset(&vpic_data, 0, sizeof(vpic_data_t));

    #ifdef ENABLE_MPI
    MPI_Info    info = MPI_INFO_NULL;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank (MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size (MPI_COMM_WORLD, &num_procs);
    #endif


    if(argc < 2) {
        print_usage(argv[0]);
        return 0;
    }

    file_name  = argv[1];
    group_name = "Step#0";
    if (argc >= 3) 
        query_id = atoi(argv[2]);

    query_id2 = query_id;
    if (argc >= 4) 
        query_id2 = atoi(argv[3]);

    if (my_rank == 0) {
        printf ("Query id = %d to %d, reading from file [%s], \n", query_id, query_id2, file_name);
        fflush(stdout);
    }

    if((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        printf("Error with fapl create!\n");
        goto error;
    }

    #ifdef ENABLE_MPI
    if(H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        printf("Error with H5Pset_fapl_mpio!\n");
        goto error;
    }
    #endif

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on (0);

    file_id = H5Fopen(file_name, H5F_ACC_RDONLY, fapl);
    if(file_id < 0) {
        printf("Error opening file [%s]!\n", file_name);
        goto error;
    }

    group_id = H5Gopen(file_id, group_name, H5P_DEFAULT);
    if(group_id < 0) {
        printf("Error opening group [%s]!\n", group_name);
        goto error;
    }

    for (i = 0; i < NVAR; i++) {
        dset_ids[i] = H5Dopen(group_id, dset_names[i], H5P_DEFAULT);
        if(dset_ids[i] < 0) {
            printf("Error opening dataset [%s]!\n", dset_names[i]);
            goto error;
        }
    }

    filespace = H5Dget_space(dset_ids[0]);
    ndim      = H5Sget_simple_extent_ndims(filespace);
    H5Sget_simple_extent_dims(filespace, dims, NULL);

    my_size   = dims[0] / num_procs;
    my_offset = my_rank * my_size;
    if (my_rank == num_procs - 1) 
        my_size += (dims[0] % my_size);
    
    vpic_data.nparticle = my_size;

    vpic_data.energy = (float*)malloc(my_size * sizeof(float));
    if (NULL == vpic_data.energy) 
        printf("%d: error with energy malloc!\n", my_rank);
    #ifndef QUERY_ENERGY_ONLY
    vpic_data.x      = (float*)malloc(my_size * sizeof(float));
    if (NULL == vpic_data.x) 
        printf("%d: error with x malloc!\n", my_rank);
    vpic_data.y      = (float*)malloc(my_size * sizeof(float));
    if (NULL == vpic_data.y) 
        printf("%d: error with y malloc!\n", my_rank);
    vpic_data.z      = (float*)malloc(my_size * sizeof(float));
    if (NULL == vpic_data.z) 
        printf("%d: error with z malloc!\n", my_rank);
    /* vpic_data.ux     = (float*)malloc(my_size * sizeof(float)); */
    /* if (NULL == vpic_data.ux) */ 
    /*     printf("%d: error with ux malloc!\n", my_rank); */
    /* vpic_data.uy     = (float*)malloc(my_size * sizeof(float)); */
    /* if (NULL == vpic_data.uy) */ 
    /*     printf("%d: error with uy malloc!\n", my_rank); */
    /* vpic_data.uz     = (float*)malloc(my_size * sizeof(float)); */
    /* if (NULL == vpic_data.uz) */ 
    /*     printf("%d: error with uz malloc!\n", my_rank); */
    #endif

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_on(1);

    if (my_rank == 0) 
        printf ("Reading data \n");

    memspace =  H5Screate_simple(1, (hsize_t *) &my_size, NULL);
    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &my_offset, NULL, &my_size, NULL);

    H5Dread(dset_ids[0], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.energy);
    if (my_rank == 0) 
        printf("Finished reading energy!\n");
    #ifndef QUERY_ENERGY_ONLY
    H5Dread(dset_ids[1], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.x);
    if (my_rank == 0) {
        printf("Finished reading x!\n");
        fflush(stdout);
    }
    H5Dread(dset_ids[2], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.y);
    if (my_rank == 0) {
        printf("Finished reading y!\n");
        fflush(stdout);
    }
    H5Dread(dset_ids[3], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.z);
    if (my_rank == 0) {
        printf("Finished reading z!\n");
        fflush(stdout);
    }
    /* H5Dread(dset_ids[4], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.ux); */
    /* if (my_rank == 0) */ 
    /*     printf("Finished reading ux!\n"); */
    /* H5Dread(dset_ids[5], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.uy); */
    /* if (my_rank == 0) */ 
    /*     printf("Finished reading uy!\n"); */
    /* H5Dread(dset_ids[6], H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, vpic_data.uz); */
    /* if (my_rank == 0) */ 
    /*     printf("Finished reading uz!\n"); */
    #endif

    fflush(stdout);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(1);

    if (my_rank == 0) {
        timer_msg (1, "reading data");
        printf ("Processing query\n");
    }

    if (my_rank == 0) 

    timer_on(2);

    uint64_t *coords = NULL, *all_coords = NULL;
    int nhits, total_nhits, *all_nhits = NULL, nhashits, off;

    if (my_rank == 0) {
        all_nhits  = (int*)malloc(num_procs * sizeof(int)) ;
    }

    MPI_Request *request = (MPI_Request*)calloc(num_procs, sizeof(MPI_Request));
    MPI_Status status;

    for (i = query_id; i <= query_id2; i++) {

        #ifdef ENABLE_MPI
        MPI_Barrier (MPI_COMM_WORLD);
        #endif
        timer_on(3);

        if (my_rank == 0) 
            print_query_constraint(&query_constraints[i]);

        nhits = do_query(i, &vpic_data, &coords);
        total_nhits = nhits;

        // Gather results;
        #ifdef ENABLE_MPI

        MPI_Gather(&nhits, 1, MPI_INT, all_nhits, 1, MPI_INT, 0, MPI_COMM_WORLD);

        nhashits = 0;

        if (my_rank == 0) {
            for (j = 1; j < num_procs; j++) {
                /* printf("all_nhits[%d]=%d\n", j, all_nhits[j]); */
                total_nhits += all_nhits[j];
                if (all_nhits[j] > 0) 
                    nhashits++;
            }

            all_coords = (uint64_t*)malloc(total_nhits * sizeof(uint64_t));
            void * buf;

            off = 0;
            for (j = 1; j < num_procs; j++) {
                if (all_nhits[j] > 0) {
                    buf = (void*)(all_coords + off);
                    MPI_Irecv(buf, all_nhits[j], MPI_LONG_LONG, j, 0, MPI_COMM_WORLD, &request[j]);
                    off += all_nhits[j-1];
                }
            }
            int flag, all_done = 1;
            do {
                all_done = 1;
                for (j = 1; j < num_procs; j++) {
                    if (all_nhits[j] > 0) {
                        MPI_Test(&request[j], &flag, &status);
                        if (flag != 1) 
                            all_done = 0;
                    }
                }
            } while (all_done == 0);
        }
        else if (nhits > 0) {
            MPI_Send(coords, nhits, MPI_LONG_LONG, 0, 0, MPI_COMM_WORLD);
            /* printf("%d: send %d hits\n", my_rank, nhits); */
            fflush(stdout);
        }

        
        if (coords) free(coords);
        if (all_coords) free(all_coords);
        coords     = NULL;
        all_coords = NULL;

        #endif
        timer_off(3);

        if (my_rank == 0) 
            printf("Query %d: %d hits, %f seconds \n\n\n", i, total_nhits, (double)(elapse[3])); 
        timer_reset(3);
        fflush(stdout);
    }


    timer_off(2);


    H5Sclose(memspace);
    H5Sclose(filespace);
    for (i = 0; i < NVAR; i++) 
        H5Dclose(dset_ids[i]);
    H5Gclose(group_id);
    H5Pclose(fapl);
    H5Fclose(file_id);

    #ifdef ENABLE_MPI
    MPI_Barrier (MPI_COMM_WORLD);
    #endif
    timer_off(0);

    if (my_rank == 0) {
        timer_msg (2, "total querying");
        timer_msg (0, "opening, reading, querying, and closing file");
        printf ("\n");
    }

    goto done;

error:
    H5Pclose(fapl);
    H5Fclose(file_id);

done:
    if(vpic_data.energy) free(vpic_data.energy);
    #ifndef QUERY_ENERGY_ONLY
    if(vpic_data.x) free(vpic_data.x);
    if(vpic_data.y) free(vpic_data.y);
    if(vpic_data.z) free(vpic_data.z);
    /* if(vpic_data.ux) free(vpic_data.ux); */
    /* if(vpic_data.uy) free(vpic_data.uy); */
    /* if(vpic_data.uz) free(vpic_data.uz); */
    #endif

    H5close();

    #ifdef ENABLE_MPI
    MPI_Finalize();
    #endif

    return 0;
}
