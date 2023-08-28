#!/bin/bash

MIN_PROC=16
MAX_PROC=128

PROG_BASENAME=kvtag_query_scale_col

curdir=$(pwd)

first_submit=1

for (( i = $MIN_PROC; i <= $MAX_PROC; i*=2 )); do
    JOBNAME=${PROG_BASENAME}_${i}
    cd $curdir/$i

    for (( j = 0; j < 2; j+=1)); do
        TARGET=$JOBNAME.sbatch.${j}

        njob=`squeue -u $USER | grep ${PROG_BASENAME} | wc -l`
        echo $njob
        while [ $njob -ge 4 ]
        do
            sleeptime=$[ ( $RANDOM % 1000 )  ]
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
done


# for (( j = $MIN_PROC; j <= $MAX_PROC ; j*=2 )); do

#     njob=`squeue -u $USER | grep vpic | wc -l`
#     echo $njob
#     while [ $njob -ge 4 ]
#     do
#         sleeptime=$[ ( $RANDOM % 1000 )  ]
#         sleep $sleeptime
#         njob=`squeue -u $USER | grep vpic | wc -l`
#         echo $njob
#     done


#     cd $curdir/$j
#     for filename in ./*.sh ; do

#         if [[ $first_submit == 1 ]]; then
#             # Submit first job w/o dependency
#             echo "Submitting $filename"
#             job=`sbatch $filename`
#             first_submit=0
#         else
#             echo "Submitting $filename after ${job: -8}"
#             job=`sbatch -d afterany:${job: -8} $filename`
#         fi

#         sleeptime=$[ ( $RANDOM % 5 )  ]
#         sleep $sleeptime

#     done
# done
