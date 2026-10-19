#!/bin/bash

prev_jid=""

for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        # Submit the first job with no dependency
        jid=$(TRANSFORM=zfp sbatch --nodes=$nodes vpicio_scale.sh | awk '{print $4}')
    else
        # Submit subsequent jobs dependent on previous
        jid=$(TRANSFORM=zfp sbatch --nodes=$nodes --dependency=afterok:$prev_jid vpicio_scale.sh | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
