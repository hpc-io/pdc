#!/bin/bash

PARSE_SCRIPT="./parse.sh"

echo "Processing all subdirectories..."

# Process VPIC logs
echo "VPIC Times"
find . -type f -name 'client_output_vpicio_*_*.log' | sort -t'_' -k4 -n | while read -r file; do
    echo "$file"
    $PARSE_SCRIPT "$file"
done

# Process BDCATS logs
echo "BDCATS Times"
find . -type f -name 'client_output_bdcats_*_*.log' | sort -t'_' -k4 -n | while read -r file; do
    echo "$file"
    $PARSE_SCRIPT "$file"
done

