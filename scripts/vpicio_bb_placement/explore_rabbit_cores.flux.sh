#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 0:15:00
#FLUX: -J pdc_rabbit_explore
#FLUX: -o pdc_rabbit_explore.%J.out
#FLUX: -e pdc_rabbit_explore.%J.out

# Diagnostic-only script: gathers exactly the information needed to
# confirm or correct the rabbit-targeting block in
# job2_compute_plus_rabbit.flux.sh. Runs no PDC code at all -- this just
# allocates rabbit-attached cores experimentally and dumps everything
# relevant about what that actually got us: the flag's own help text,
# the resource set Flux assigned, which hostname(s) flux getrabbit
# reports, and a hardware fingerprint (hostname + CPU model) of where a
# process landed when targeted at that hostname vs. the normal compute
# node -- the Rabbit's storage-processor CPU is a different part (AMD
# EPYC) from the compute node's MI300A, so a differing CPU model string
# between the two sections below is the actual proof the server would
# land on separate hardware, not just a differently-labeled piece of the
# same node.
#
# Submit with: flux batch explore_rabbit_cores.flux.sh
# Then read the single output file it's configured above to produce.

set -uo pipefail # not -e: we want every probe below to run even if an earlier one fails

section() { echo; echo "=================================================="; echo "$1"; echo "=================================================="; }

section "flux version"
flux version

section "flux alloc --help=coral2-rabbit-cores (authoritative flag semantics)"
flux alloc --help=coral2-rabbit-cores 2>&1

section "This job's own jobid / queue info"
JOBID=$(flux getattr jobid)
echo "JOBID=$JOBID"
flux jobs -a "$JOBID" 2>&1

section "flux resource list (what this allocation sees, before any rabbit-cores reservation)"
flux resource list 2>&1

section "Reserving 1 rabbit core via --coral2-chassis=1 --coral2-rabbit-cores=1"
RABBIT_CORE_JOBID=$(flux submit --coral2-chassis=1 --coral2-rabbit-cores=1 -N1 -n1 sleep 600)
echo "RABBIT_CORE_JOBID=$RABBIT_CORE_JOBID"
sleep 3
flux jobs -a "$RABBIT_CORE_JOBID" 2>&1

section "flux job info \$RABBIT_CORE_JOBID jobspec (what we actually asked for)"
flux job info "$RABBIT_CORE_JOBID" jobspec 2>&1

section "flux job info \$RABBIT_CORE_JOBID R (the resource SET Flux actually assigned -- look for a rabbit-labeled slot/node entry here)"
flux job info "$RABBIT_CORE_JOBID" R 2>&1

section "flux getrabbit -j \$RABBIT_CORE_JOBID (confirmed, documented way to list rabbits attached to a job)"
GETRABBIT_OUT=$(flux getrabbit -j "$RABBIT_CORE_JOBID" 2>&1)
echo "$GETRABBIT_OUT"
RABBIT_HOST=$(echo "$GETRABBIT_OUT" | tr -d '[]' | cut -d',' -f1)
echo "Parsed RABBIT_HOST=$RABBIT_HOST"

section "flux resource list (after the rabbit-cores reservation -- diff against the 'before' section above)"
flux resource list 2>&1

section "Hardware fingerprint: THIS job's own allocation (the compute node)"
flux run -N1 -n1 bash -c 'echo "host: $(hostname)"; grep -m1 "model name" /proc/cpuinfo; nproc'

if [ -n "$RABBIT_HOST" ]; then
    section "Hardware fingerprint: targeted at RABBIT_HOST=$RABBIT_HOST via --requires=host:"
    flux run --requires=host:"$RABBIT_HOST" -n1 bash -c 'echo "host: $(hostname)"; grep -m1 "model name" /proc/cpuinfo; nproc' 2>&1
    echo
    echo "If the host/CPU model above MATCHES the compute-node fingerprint from the previous"
    echo "section, --requires=host: did NOT actually land on separate rabbit hardware --"
    echo "job2_compute_plus_rabbit.flux.sh's targeting mechanism needs rethinking. If it"
    echo "DIFFERS (especially the CPU model line), that's the confirmation the rabbit-cores"
    echo "reservation + host targeting genuinely works as job2 assumes."
else
    echo "Could not parse a rabbit hostname from flux getrabbit output above -- see that"
    echo "section directly; the reservation itself may not have produced an attached rabbit."
fi

section "Cleanup"
flux cancel "$RABBIT_CORE_JOBID" 2>&1 || true

section "Done"
