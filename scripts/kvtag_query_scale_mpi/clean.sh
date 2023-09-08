#!/bin/bash

if [[ "$#" -ne 1 ]]; then
    echo "Usage: $0 <clean_dir>"
    exit 1
fi

CLEAN_DIR=$1
MAX_NODE=512

# if CLEAN_DIR is set to '1', then clean all the directories named with numbers, otherwise, clean the sbatch script only
find ./ -name   "*.sbatch*" -delete

if [[ "$CLEAN_DIR" -eq "1" ]]; then
    for (( i = 1; i <= $MAX_NODE; i*=2 )); do

        rm -rf $i/*

    done
fi