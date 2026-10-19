#!/bin/bash

# Usage: ./sort_profile_by_nsec.sh input.csv > sorted.csv

# Extract header
head -n 1 "$1"

# Sort the remaining lines by the 4th column (nSec), numerically, in descending order
tail -n +2 "$1" | sort -t, -k4,4nr

