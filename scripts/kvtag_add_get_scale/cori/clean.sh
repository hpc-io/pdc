#!/bin/bash
MAX_NODE=512

for (( i = 1; i <= $MAX_NODE; i*=2 )); do

    rm -rf $i/*

done