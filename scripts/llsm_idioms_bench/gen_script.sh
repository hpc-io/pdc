#!/bin/bash

if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 <min_node> <proj_name>"
    exit 1
fi

# test if $1 is a number
re='^[0-9]+$'
if ! [[ $1 =~ $re ]] ; then
    echo "Error: min_node is not a number" >&2; exit 1
fi

if [[ "$1" -lt "1" ]]; then
    echo "Error: min_node should be larger than 0"
    exit 1
fi

if [[ "$1" -gt "512" ]]; then
    echo "Error: min_node should be smaller than 512"
    exit 1
fi

MIN_NODE=$1
MAX_NODE=512
MAX_ATTR=1024
MAX_ATTRLEN=1000



# Per node configuration of your HPC system. 
MAX_PYSICAL_CORE=128
MAX_HYPERTHREADING=2

# Designated number of threads per process on each node 
# (this should be associated with -c option in srun)
NUM_THREAD_PER_SERVER_PROC=2
NUM_THREAD_PER_CLIENT_PROC=2

# test if $2 is a string starting with a letter, and followed by a 4-digit number
re='^[a-zA-Z][0-9]{4}$'
if ! [[ $2 =~ $re ]] ; then
    echo "Error: proj_name should be a string starting with a letter, and followed by a 4-digit number, e.g. m2021" >&2; exit 1
fi

PROJECT_NAME=$2


# Designated number of processes for server anc client on each node
# (this should be associated with -n option in srun)
TOTAL_NUM_CLIENT_PROC=$((128 * MIN_NODE))
NUM_SERVER_PROC_PER_NODE=1
NUM_CLIENT_PROC_PER_NODE=$((TOTAL_NUM_CLIENT_PROC))


PROG_BASENAME=idioms

for (( i = $MIN_NODE; i <= $MAX_NODE; i*=2 )); do
    mkdir -p $i
    NUM_CLIENT_PROC_PER_NODE=$((TOTAL_NUM_CLIENT_PROC/i))
    AVAIL_CLIENT_THREAD_CORES=$((MAX_PYSICAL_CORE * MAX_HYPERTHREADING - NUM_SERVER_PROC_PER_NODE * NUM_THREAD_PER_SERVER_PROC))
    AVAIL_CLIENT_PHYSICAL_CORES=$((AVAIL_CLIENT_THREAD_CORES / NUM_THREAD_PER_CLIENT_PROC))
    if [[  $(( NUM_CLIENT_PROC_PER_NODE > AVAIL_CLIENT_PHYSICAL_CORES )) -eq 1  ]]; then
        NUM_CLIENT_PROC_PER_NODE=$((AVAIL_CLIENT_PHYSICAL_CORES - 2))
    fi
    for (( j = 0; j <= 1; j+=1 )); do
        for (( q = 0; q < 4; q+=1 )); do
            for (( c = 0; c < 2; c+=1 )); do
                JOBNAME=${PROG_BASENAME}_${i}_${j}_${q}_${c}
                TARGET=./$i/$JOBNAME.sbatch
                cp template.sh $TARGET
                sed -i "s/JOBNAME/${JOBNAME}/g"           $TARGET
                sed -i "s/NODENUM/${i}/g"           $TARGET
                sed -i "s/MPHYSICALCORE/${MAX_PYSICAL_CORE}/g"           $TARGET
                sed -i "s/MHYPERTHREADING/${MAX_HYPERTHREADING}/g"           $TARGET
                sed -i "s/N_SERVER_PROC/${NUM_SERVER_PROC_PER_NODE}/g"           $TARGET
                sed -i "s/N_CLIENT_PROC/${NUM_CLIENT_PROC_PER_NODE}/g"           $TARGET
                sed -i "s/NTHREAD_PER_SPROC/${NUM_THREAD_PER_SERVER_PROC}/g"           $TARGET
                sed -i "s/NTHREAD_PER_CPROC/${NUM_THREAD_PER_CLIENT_PROC}/g"           $TARGET
                sed -i "s/PROJNAME/${PROJECT_NAME}/g"           $TARGET
                sed -i "s/USING_DART/${j}/g"           $TARGET
                sed -i "s/QUERY_TYPE/${q}/g"           $TARGET
                sed -i "s/COMMUNICATION_TYPE/${c}/g"           $TARGET
                if [[ "$i" -gt "4" ]]; then
                    sed -i "s/REG//g"           $TARGET
                else
                    sed -i "s/DBG//g"           $TARGET
                fi
            done
        done
    done
done
