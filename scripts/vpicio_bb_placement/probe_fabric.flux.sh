#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 5m
#FLUX: --job-name=probe_fabric
#FLUX: --output=probe_fabric.{{id}}.out
#FLUX: --error=probe_fabric.{{id}}.out
#FLUX: --coral2-chassis=1
#FLUX: --coral2-rabbit-cores=1
#FLUX: -S dw="#DW jobdw type=xfs capacity=1GiB name=pdcprobe"

# One-off diagnostic: which libfabric providers (fi_info) and NICs
# (lspci/lsmod) actually exist on a compute node vs. a Rabbit host, to
# settle whether job2's "server on the rabbit, clients on compute"
# placement can ever share a single Mercury NA transport across both.
#
# Submit with: flux batch probe_fabric.flux.sh
# Then: cat probe_fabric.<jobid>.out

set -x

JOBID=$(flux getattr jobid)
GETRABBIT_OUT=$(flux --parent getrabbit -j "$JOBID" 2>&1)
RABBIT_HOST=$(echo "$GETRABBIT_OUT" | tr -d '[] ' | cut -d',' -f1)
echo "RABBIT_HOST=$RABBIT_HOST"

echo "=== fi_info on rabbit host ($RABBIT_HOST) ==="
flux run --requires=host:"$RABBIT_HOST" -n1 fi_info

echo "=== NIC check on rabbit host ($RABBIT_HOST) ==="
flux run --requires=host:"$RABBIT_HOST" -n1 bash -c \
    'lspci | grep -i -E "slingshot|cassini|cxi" ; echo "---lsmod---" ; lsmod | grep -i cxi ; echo "---ip link---" ; ip link'

echo "=== fi_info on compute node ==="
flux run --requires=-host:"$RABBIT_HOST" -n1 fi_info

echo "=== NIC check on compute node ==="
flux run --requires=-host:"$RABBIT_HOST" -n1 bash -c \
    'lspci | grep -i -E "slingshot|cassini|cxi" ; echo "---lsmod---" ; lsmod | grep -i cxi ; echo "---ip link---" ; ip link'
