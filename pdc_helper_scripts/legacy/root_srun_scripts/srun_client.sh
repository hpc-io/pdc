set -x
set -u

NUM_NODES=2
TASKS_PER_NODE=128
TOTAL_TASKS=$((NUM_NODES * TASKS_PER_NODE))

pushd ./build/bin
srun -N "$NUM_NODES" -n "$TOTAL_TASKS" --ntasks-per-node="$TASKS_PER_NODE" --output=client_output.log --error=client_output.err ./vpicio_mts 8388608 5 20
popd

