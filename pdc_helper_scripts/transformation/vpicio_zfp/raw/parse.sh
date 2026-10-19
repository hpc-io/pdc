#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <logfile>"
    exit 1
fi

file="$1"

types=("Obj create time" "Obj open time" "Transfer create time" "Transfer start time" \
       "Sleep start" "Sleep end" "Transfer wait time" "Transfer close time" \
       "Obj close time")

for type in "${types[@]}"; do
    # Extract matching lines
    vals=$(grep "\[WARNING\].*PDC_CLIENT\[[0-9]*\]: $type:" "$file" | awk -F': ' '{printf "%s,", $NF}' | sed 's/,$//')
    
    # Only print if there is any data
    if [ -n "$vals" ]; then
        echo "$type,$vals"
    fi
done

