#!/bin/bash
N_THREAD=NO
MAX_NODE=512
MAX_ATTR=1024
MAX_ATTRLEN=1000

PROJECT_NAME=$1

for (( i = 1; i <= $MAX_NODE; i*=2 )); do
    mkdir -p $i
    JOBNAME=kvtag_scale_${i}
    TARGET=./$i/$JOBNAME.sbatch
    cp template.sh $TARGET
    sed -i "s/JOBNAME/${JOBNAME}/g"           $TARGET
    sed -i "s/NODENUM/${i}/g"           $TARGET
    sed -i "s/PROJNAME/${PROJECT_NAME}/g"           $TARGET
    if [[ "$i" -gt "16" ]]; then
        sed -i "s/REG//g"           $TARGET
    else
        sed -i "s/DBG//g"           $TARGET
    fi
done
