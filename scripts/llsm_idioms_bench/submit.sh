#!/bin/bash

MIN_PROC=16
MAX_PROC=128

PROG_BASENAME=idioms

curdir=$(pwd)

first_submit=1

if [[ "$#" -ne 3 ]]; then
    echo "Usage: $0 <i_type> <q_type> <c_type>"
    exit 1
fi

# test if $1 is a single digit number between 0 and 1 (inclusive)
re='^[0-1]$'
if ! [[ "$1" =~ $re ]]; then
    echo "Error: i_type should be a single digit number between 0 and 1 (inclusive), 0 means not using index, 1 means using index"
    exit 1
fi

# test if $2 is a single digit number between 0 and 3 (inclusive)
re='^[0-3]$'
if ! [[ "$2" =~ $re ]]; then
    echo "Error: q_type should be a single digit number between 0 and 3 (inclusive), 0: exact query, 1: prefix query, 2: suffix query, 3: infix query"
    exit 1
fi

# test if $3 is a single digit number between 0 and 1 (inclusive)
re='^[0-1]$'
if ! [[ "$3" =~ $re ]]; then
    echo "Error: c_type should be a single digit number between 0 and 1 (inclusive), 0 means using non-collective mode, 1 means using collective mode"
    exit 1
fi


i_type=$1
q_type=$2
c_type=$3

for (( i = $MIN_PROC; i <= $MAX_PROC; i*=2 )); do
    
    cd $curdir/$i

    JOBNAME=${PROG_BASENAME}_${i}_${i_type}_${q_type}_${c_type}
    TARGET=$JOBNAME.sbatch

    njob=`squeue -u $USER | grep ${PROG_BASENAME} | wc -l`
    echo $njob
    while [ $njob -ge 16 ]
    do
        sleeptime=$[ ( $RANDOM % 5 )  ]
        sleep $sleeptime
        njob=`squeue -u $USER | grep ${PROG_BASENAME} | wc -l`
        echo $njob
    done

    if [[ $first_submit == 1 ]]; then
        # Submit first job w/o dependency
        echo "Submitting $TARGET"
        job=`sbatch $TARGET`
        first_submit=0
    else
        echo "Submitting $TARGET after ${job: -8}"
        job=`sbatch -d afterany:${job: -8} $TARGET`
    fi

    sleeptime=$[ ( $RANDOM % 5 )  ]
    sleep $sleeptime
    
done
