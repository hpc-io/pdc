#!/bin/bash

# Per node configuration of your HPC system. 
MAX_PYSICAL_CORE=128
MAX_HYPERTHREADING=2

# Designated number of threads per process on each node 
# (this should be associated with -c option in srun)
NUM_THREAD_PER_SERVER_PROC=2
NUM_THREAD_PER_CLIENT_PROC=2


# Designated number of processes for server anc client on each node
# (this should be associated with -n option in srun)
NUM_SERVER_PROC_PER_NODE=1
NUM_CLIENT_PROC_PER_NODE=120


MAX_NODE=512
MAX_ATTR=1024
MAX_ATTRLEN=1000

PROG_BASENAME=kvtag_query_scale_col

for (( i = 1; i <= $MAX_NODE; i*=2 )); do
    for (( j = 0; j <= 1; j+=1 )); do
        mkdir -p $i
        JOBNAME=${PROG_BASENAME}_${i}
        TARGET=./$i/$JOBNAME.sbatch.${j}
        cp template.sh $TARGET
        sed -i "s/JOBNAME/${JOBNAME}/g"           $TARGET
        sed -i "s/NODENUM/${i}/g"           $TARGET
        sed -i "s/MPHYSICALCORE/${MAX_PYSICAL_CORE}/g"           $TARGET
        sed -i "s/MHYPERTHREADING/${MAX_HYPERTHREADING}/g"           $TARGET
        sed -i "s/N_SERVER_PROC/${NUM_SERVER_PROC_PER_NODE}/g"           $TARGET
        sed -i "s/N_CLIENT_PROC/${NUM_CLIENT_PROC_PER_NODE}/g"           $TARGET
        sed -i "s/NTHREAD_PER_SPROC/${NUM_THREAD_PER_SERVER_PROC}/g"           $TARGET
        sed -i "s/NTHREAD_PER_CPROC/${NUM_THREAD_PER_CLIENT_PROC}/g"           $TARGET
        sed -i "s/USING_DART/${j}/g"           $TARGET
        if [[ "$i" -gt "4" ]]; then
            sed -i "s/REG//g"           $TARGET
        else
            sed -i "s/DBG//g"           $TARGET
        fi
    done
done
