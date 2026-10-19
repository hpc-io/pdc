#!/bin/bash

PARSE_SCRIPT="./parse.sh"

echo "VPIC Times"

files=$(find . -type f -name 'client_output_vpicio_raw_*.log' | sort -t'_' -k4 -n)
for file in $files; do
    $PARSE_SCRIPT "$file"
done

echo "BDCATS Times"

files=$(find . -type f -name 'client_output_bdcats_raw_*.log' | sort -t'_' -k4 -n)
for file in $files; do
    $PARSE_SCRIPT "$file"
done

